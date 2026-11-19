package host

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os/exec"
	"path"
	"regexp"
	"strings"

	"github.com/graphene-ci/library/host/internal/contract"
)

// MountInfo is the output of a Mount resource.
type MountInfo struct {
	// Device is the kernel device the path resolves to, e.g. /dev/nvme1n1.
	Device     string `json:"device"`
	Mountpoint string `json:"mountpoint"`
	FSType     string `json:"fsType"`
	// Unit is the systemd mount unit holding the mount; empty in Check mode.
	Unit string `json:"unit,omitempty"`
	// Formatted says this record created the filesystem.
	Formatted bool `json:"formatted,omitempty"`
}

type mountSpec = contract.MountSpec

var fsTypePattern = regexp.MustCompile(`^(ext4|xfs|btrfs)$`)

func validateMount(spec mountSpec) error {
	if !strings.HasPrefix(spec.Device, "/dev/") || path.Clean(spec.Device) != spec.Device {
		return fmt.Errorf("mount device %q: want a clean /dev path, best one under /dev/disk/by-id", spec.Device)
	}
	if !path.IsAbs(spec.Mountpoint) || path.Clean(spec.Mountpoint) != spec.Mountpoint || spec.Mountpoint == "/" {
		return fmt.Errorf("mountpoint %q: want a clean absolute path below /", spec.Mountpoint)
	}
	if !fsTypePattern.MatchString(spec.FSType) {
		return fmt.Errorf("mount %s: fsType %q, want ext4, xfs or btrfs", spec.Mountpoint, spec.FSType)
	}
	for _, o := range spec.Options {
		if o == "" || strings.ContainsAny(o, ", \n") {
			return fmt.Errorf("mount %s: option %q must be one word", spec.Mountpoint, o)
		}
	}
	if spec.Check && spec.Format {
		return fmt.Errorf("mount %s: Check never formats", spec.Mountpoint)
	}
	return nil
}

// mountUnitName is systemd's name for the mount unit of a path — what
// `systemd-escape --path --suffix=mount` prints: slashes become dashes,
// anything outside [A-Za-z0-9:_.] (and a leading dot) becomes \xNN.
func mountUnitName(mountpoint string) string {
	p := strings.Trim(path.Clean(mountpoint), "/")
	if p == "" {
		return "-.mount"
	}
	var b strings.Builder
	for i := range len(p) {
		c := p[i]
		switch {
		case c == '/':
			b.WriteByte('-')
		case c == '.' && i == 0:
			fmt.Fprintf(&b, `\x%02x`, c)
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9', c == ':', c == '_', c == '.':
			b.WriteByte(c)
		default:
			fmt.Fprintf(&b, `\x%02x`, c)
		}
	}
	return b.String() + ".mount"
}

// resolveDevice follows /dev/disk/by-id links to the kernel device, on
// the machine itself — where blkid and mount see it.
func (m Machine) resolveDevice(ctx context.Context, device string) (string, error) {
	out, err := m.runner.Run(ctx, "readlink", "-e", device)
	resolved := strings.TrimSpace(out)
	if err != nil || resolved == "" {
		return "", conflict(contract.MountKind, device, "no such device")
	}
	return resolved, nil
}

// signature reads what is on a device: "" for a blank device, else the
// filesystem type or "partition table:<type>".
func (m Machine) signature(ctx context.Context, device string) (string, error) {
	out, err := m.runner.Run(ctx, "blkid", "-p", "-o", "export", device)
	if err != nil {
		// blkid exits 2 for "no signature found": a blank device.
		var exit *exec.ExitError
		if errors.As(err, &exit) && exit.ExitCode() == 2 {
			return "", nil
		}
		return "", err
	}
	var fsType, ptType string
	for _, line := range strings.Split(out, "\n") {
		k, v, _ := strings.Cut(strings.TrimSpace(line), "=")
		switch k {
		case "TYPE":
			fsType = v
		case "PTTYPE":
			ptType = v
		}
	}
	switch {
	case fsType != "":
		return fsType, nil
	case ptType != "":
		return "partition table:" + ptType, nil
	default:
		return "", nil
	}
}

// mounted reports what is mounted at mountpoint: device and type; empty
// when nothing is.
func (m Machine) mounted(ctx context.Context, mountpoint string) (string, string) {
	out, _ := m.runner.Run(ctx, "findmnt", "-n", "-o", "SOURCE,FSTYPE", "--mountpoint", mountpoint)
	fields := strings.Fields(out)
	if len(fields) < 2 {
		return "", ""
	}
	return fields[0], fields[1]
}

func (m Machine) applyMount(ctx context.Context, req applyRequest[mountSpec]) (MountInfo, error) {
	spec := req.Spec
	if err := validateMount(spec); err != nil {
		return MountInfo{}, err
	}
	device, err := m.resolveDevice(ctx, spec.Device)
	if err != nil {
		return MountInfo{}, err
	}
	info := MountInfo{Device: device, Mountpoint: spec.Mountpoint, FSType: spec.FSType}
	if spec.Check {
		return info, m.checkMount(ctx, spec, device)
	}
	f, err := m.open()
	if err != nil {
		return MountInfo{}, err
	}
	defer f.Close()
	c, err := f.take(contract.MountKind, spec.Mountpoint, req.Record, func() (string, error) { return "", nil })
	if err != nil {
		return MountInfo{}, err
	}
	// The claim remembers a format the record did, so a retried apply
	// does not take the fresh filesystem for someone else's.
	formatted := c.Prior == "formatted"
	sig, err := m.signature(ctx, device)
	if err != nil {
		return MountInfo{}, err
	}
	switch {
	case sig == spec.FSType:
	case sig == "" && spec.Format:
		if _, err := m.runner.Run(ctx, "mkfs."+spec.FSType, device); err != nil {
			return MountInfo{}, err
		}
		formatted = true
		if err := f.mark(contract.MountKind, spec.Mountpoint, req.Record, "formatted"); err != nil {
			return MountInfo{}, err
		}
	case sig == "":
		return MountInfo{}, conflict(contract.MountKind, spec.Mountpoint, device+" is blank and Format is off")
	default:
		// Never format over data: a device carrying anything else is not
		// this record's to touch.
		return MountInfo{}, conflict(contract.MountKind, spec.Mountpoint, device+" carries "+sig)
	}
	info.Formatted = formatted
	if src, _ := m.mounted(ctx, spec.Mountpoint); src != "" && src != device {
		return MountInfo{}, conflict(contract.MountKind, spec.Mountpoint, "a mount of "+src)
	}
	unit := mountUnitName(spec.Mountpoint)
	content := fmt.Sprintf("[Unit]\nDescription=graphene record %s\n\n[Mount]\nWhat=%s\nWhere=%s\nType=%s\n",
		req.Record, spec.Device, spec.Mountpoint, spec.FSType)
	if len(spec.Options) > 0 {
		content += "Options=" + strings.Join(spec.Options, ",") + "\n"
	}
	content += "\n[Install]\nWantedBy=multi-user.target\n"
	if err := f.r.MkdirAll(rel(spec.Mountpoint), 0o750); err != nil {
		return MountInfo{}, err
	}
	if err := f.r.MkdirAll(rel(unitDir), 0o750); err != nil {
		return MountInfo{}, err
	}
	if err := f.writeFile(path.Join(unitDir, unit), []byte(content), 0o600); err != nil {
		return MountInfo{}, err
	}
	if err := m.systemctl(ctx, "daemon-reload"); err != nil {
		return MountInfo{}, err
	}
	if err := m.systemctl(ctx, "enable", "--now", unit); err != nil {
		return MountInfo{}, err
	}
	// The exit status of a start proves nothing: the mount is what counts.
	if src, _ := m.mounted(ctx, spec.Mountpoint); src != device {
		return MountInfo{}, fmt.Errorf("mount %s: %s did not come up (mounted: %q)", spec.Mountpoint, unit, src)
	}
	info.Unit = unit
	return info, nil
}

// checkMount verifies a mount someone else made: the device is mounted at
// the mountpoint with the type. Nothing changes, now or on removal.
func (m Machine) checkMount(ctx context.Context, spec mountSpec, device string) error {
	src, fsType := m.mounted(ctx, spec.Mountpoint)
	switch {
	case src == "":
		return conflict(contract.MountKind, spec.Mountpoint, "nothing: it is not mounted")
	case src != device:
		return conflict(contract.MountKind, spec.Mountpoint, "a mount of "+src+", not "+device)
	case fsType != spec.FSType:
		return conflict(contract.MountKind, spec.Mountpoint, "a "+fsType+" filesystem, not "+spec.FSType)
	}
	return nil
}

// removeMount unmounts and removes the record's unit. The filesystem and
// its data stay: a mount record never wipes a disk.
func (m Machine) removeMount(ctx context.Context, req applyRequest[mountSpec]) error {
	if req.Spec.Check {
		return nil
	}
	f, err := m.open()
	if err != nil {
		return err
	}
	defer f.Close()
	_, ours, err := f.held(contract.MountKind, req.Spec.Mountpoint, req.Record)
	if err != nil || !ours {
		return err
	}
	unit := mountUnitName(req.Spec.Mountpoint)
	file := rel(path.Join(unitDir, unit))
	if _, err := f.r.Lstat(file); err == nil {
		if err := m.systemctl(ctx, "disable", "--now", unit); err != nil {
			return err
		}
		if err := f.r.Remove(file); err != nil && !errors.Is(err, fs.ErrNotExist) {
			return err
		}
	} else if !errors.Is(err, fs.ErrNotExist) {
		return err
	}
	if err := m.systemctl(ctx, "daemon-reload"); err != nil {
		return err
	}
	return f.drop(contract.MountKind, req.Spec.Mountpoint, req.Record)
}
