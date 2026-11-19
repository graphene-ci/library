package host

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"path"
	"regexp"
	"slices"
	"strings"

	"github.com/graphene-ci/library/host/internal/contract"
)

// SysctlInfo is the output of a Sysctl resource: what each key was before.
type SysctlInfo struct {
	Prior map[string]string `json:"prior"`
}

type sysctlSpec = contract.SysctlSpec

var sysctlKeyPattern = regexp.MustCompile(`^[A-Za-z0-9_]+([./][A-Za-z0-9_:-]+)+$`)

// sysctlPath maps a key to its /proc/sys file. Dots separate levels; a key
// with slashes is already a path (an interface name may contain dots:
// net.ipv4.conf.eth0/1 is written net/ipv4/conf/eth0.1 by sysctl itself).
func sysctlPath(key string) (string, error) {
	if !sysctlKeyPattern.MatchString(key) {
		return "", fmt.Errorf("sysctl key %q: want dotted or slashed names", key)
	}
	rel := key
	if !strings.Contains(key, "/") {
		rel = strings.ReplaceAll(key, ".", "/")
	}
	return path.Join("/proc/sys", rel), nil
}

func validateSysctl(spec sysctlSpec) error {
	if len(spec.Values) == 0 {
		return errors.New("sysctl: no values")
	}
	for key, value := range spec.Values {
		if _, err := sysctlPath(key); err != nil {
			return err
		}
		if strings.ContainsAny(value, "\n\r") {
			return fmt.Errorf("sysctl %s: value must be one line", key)
		}
	}
	return nil
}

// dropInPath is the record's sysctl.d file: the values survive a reboot
// while the record lives.
func dropInPath(record string) string {
	return path.Join("/etc/sysctl.d", "90-graphene-"+fileSafe(record)+".conf")
}

func sortedKeys(values map[string]string) []string {
	keys := make([]string, 0, len(values))
	for k := range values {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	return keys
}

// applySysctl claims every key (recording its prior value on the machine),
// writes the drop-in, then sets the live values.
func (m Machine) applySysctl(_ context.Context, req applyRequest[sysctlSpec]) (SysctlInfo, error) {
	if err := validateSysctl(req.Spec); err != nil {
		return SysctlInfo{}, err
	}
	f, err := m.open()
	if err != nil {
		return SysctlInfo{}, err
	}
	defer f.Close()
	keys := sortedKeys(req.Spec.Values)
	info := SysctlInfo{Prior: map[string]string{}}
	for _, key := range keys {
		p, _ := sysctlPath(key)
		c, err := f.take(contract.SysctlKind, key, req.Record, func() (string, error) { return f.readTrimmed(p) })
		if err != nil {
			return SysctlInfo{}, err
		}
		info.Prior[key] = c.Prior
	}
	var conf strings.Builder
	for _, key := range keys {
		fmt.Fprintf(&conf, "%s = %s\n", key, req.Spec.Values[key])
	}
	dropIn := dropInPath(req.Record)
	if err := f.r.MkdirAll(rel(path.Dir(dropIn)), 0o750); err != nil {
		return SysctlInfo{}, err
	}
	// systemd-sysctl reads it as root: owner-only is enough.
	if err := f.r.WriteFile(rel(dropIn), []byte(conf.String()), 0o600); err != nil {
		return SysctlInfo{}, err
	}
	for _, key := range keys {
		p, _ := sysctlPath(key)
		if err := f.writeValue(p, req.Spec.Values[key]); err != nil {
			return SysctlInfo{}, fmt.Errorf("sysctl %s: %w", key, err)
		}
	}
	return info, nil
}

// removeSysctl deletes the drop-in and writes every held key's prior value
// back. `sysctl --system` would not do it: a key no file names keeps its
// current value.
func (m Machine) removeSysctl(_ context.Context, req applyRequest[sysctlSpec]) error {
	f, err := m.open()
	if err != nil {
		return err
	}
	defer f.Close()
	errs := []error{}
	if err := f.r.Remove(rel(dropInPath(req.Record))); err != nil && !errors.Is(err, fs.ErrNotExist) {
		errs = append(errs, err)
	}
	for _, key := range sortedKeys(req.Spec.Values) {
		c, ours, err := f.held(contract.SysctlKind, key, req.Record)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		if !ours {
			continue
		}
		p, _ := sysctlPath(key)
		if err := f.writeValue(p, c.Prior); err != nil {
			errs = append(errs, fmt.Errorf("restore sysctl %s: %w", key, err))
			continue
		}
		errs = append(errs, f.drop(contract.SysctlKind, key, req.Record))
	}
	return errors.Join(errs...)
}
