//go:build integration

package host

import (
	"context"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// The real machine: root, a live /proc and systemd as PID 1. Run it in a
// disposable privileged container (see README) — it changes the machine.
// Only network-namespaced sysctls are touched, so a container run never
// changes its host's kernel.
func realMachine(t *testing.T) Machine {
	t.Helper()
	if os.Getenv("GRAPHENE_HOST_IT") != "1" || os.Geteuid() != 0 {
		t.Skip("needs GRAPHENE_HOST_IT=1 and root on a disposable machine")
	}
	return NewMachine("", execRunner{})
}

func systemctlWord(t *testing.T, m Machine, verb, unit string) string {
	t.Helper()
	return m.query(context.Background(), verb, unit)
}

func TestRealSysctl(t *testing.T) {
	m := realMachine(t)
	ctx := context.Background()
	before, err := os.ReadFile("/proc/sys/net/ipv4/ip_local_port_range")
	require.NoError(t, err)
	req := applyRequest[sysctlSpec]{Record: "host-sysctl/it", Spec: sysctlSpec{Values: map[string]string{
		"net.ipv4.ip_local_port_range": "20000\t40000",
		"net.core.somaxconn":           "1234",
	}}}
	_, err = m.applySysctl(ctx, req)
	require.NoError(t, err)
	got, err := os.ReadFile("/proc/sys/net/core/somaxconn")
	require.NoError(t, err)
	require.Equal(t, "1234", strings.TrimSpace(string(got)))
	require.NoError(t, m.removeSysctl(ctx, req))
	after, err := os.ReadFile("/proc/sys/net/ipv4/ip_local_port_range")
	require.NoError(t, err)
	require.Equal(t, strings.TrimSpace(string(before)), strings.TrimSpace(string(after)))
}

func TestRealDir(t *testing.T) {
	m := realMachine(t)
	ctx := context.Background()
	req := applyRequest[dirSpec]{Record: "host-dir/it", Spec: dirSpec{Path: "/srv/it-data/pg", Mode: 0o700, UID: 999, GID: 999}}
	_, err := m.applyDir(ctx, req)
	require.NoError(t, err)
	st, err := os.Stat("/srv/it-data/pg")
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0o700), st.Mode().Perm())
	require.NoError(t, os.WriteFile("/srv/it-data/pg/data", make([]byte, 8<<20), 0o600))
	require.NoError(t, m.removeDir(ctx, req))
	_, err = os.Stat("/srv/it-data/pg")
	require.True(t, os.IsNotExist(err))
}

func TestRealUnits(t *testing.T) {
	m := realMachine(t)
	ctx := context.Background()
	unit := applyRequest[unitSpec]{Record: "host-unit/it", Spec: unitSpec{
		Name:    "graphene-it.service",
		Content: []byte("[Service]\nExecStart=/bin/sleep infinity\n[Install]\nWantedBy=multi-user.target\n"),
	}}
	_, err := m.applyUnit(ctx, unit)
	require.NoError(t, err)
	require.Equal(t, "active", systemctlWord(t, m, "is-active", "graphene-it.service"))
	require.Equal(t, "enabled", systemctlWord(t, m, "is-enabled", "graphene-it.service"))
	require.NoError(t, m.removeUnit(ctx, unit))
	_, err = os.Stat("/etc/systemd/system/graphene-it.service")
	require.True(t, os.IsNotExist(err))
	require.NotEqual(t, "active", systemctlWord(t, m, "is-active", "graphene-it.service"))
}

// A vendor unit (shipped under /usr/lib, like apt-daily.timer) held
// masked comes back enabled and running.
func TestRealUnitState(t *testing.T) {
	m := realMachine(t)
	ctx := context.Background()
	const vendor = "/usr/lib/systemd/system/graphene-vendor-it.service"
	require.NoError(t, os.WriteFile(vendor, []byte("[Service]\nExecStart=/bin/sleep infinity\n[Install]\nWantedBy=multi-user.target\n"), 0o600))
	t.Cleanup(func() {
		_, _ = execRunner{}.Run(ctx, "systemctl", "disable", "--now", "graphene-vendor-it.service")
		_ = os.Remove(vendor)
		_, _ = execRunner{}.Run(ctx, "systemctl", "daemon-reload")
	})
	_, err := execRunner{}.Run(ctx, "systemctl", "daemon-reload")
	require.NoError(t, err)
	_, err = execRunner{}.Run(ctx, "systemctl", "enable", "--now", "graphene-vendor-it.service")
	require.NoError(t, err)

	state := applyRequest[unitStateSpec]{Record: "host-unit-state/it", Spec: unitStateSpec{Name: "graphene-vendor-it.service", Want: string(UnitMasked)}}
	info, err := m.applyUnitState(ctx, state)
	require.NoError(t, err)
	require.Equal(t, UnitStateInfo{Name: "graphene-vendor-it.service", PriorEnabled: "enabled", PriorActive: "active"}, info)
	require.Equal(t, "masked", systemctlWord(t, m, "is-enabled", "graphene-vendor-it.service"))
	require.NotEqual(t, "active", systemctlWord(t, m, "is-active", "graphene-vendor-it.service"))

	require.NoError(t, m.removeUnitState(ctx, state))
	require.Equal(t, "enabled", systemctlWord(t, m, "is-enabled", "graphene-vendor-it.service"))
	require.Equal(t, "active", systemctlWord(t, m, "is-active", "graphene-vendor-it.service"))
}

// A loop device stands in for a data disk: formatted only while blank,
// mounted through a unit, unmounted on removal with the data kept, then
// mounted again without a format, and checked as someone else's mount.
func TestRealMount(t *testing.T) {
	m := realMachine(t)
	ctx := context.Background()
	img := "/srv/it-disk.img"
	require.NoError(t, os.WriteFile(img, nil, 0o600))
	require.NoError(t, os.Truncate(img, 64<<20))
	out, err := execRunner{}.Run(ctx, "losetup", "-f", "--show", img)
	require.NoError(t, err)
	dev := strings.TrimSpace(out)
	// A mount unit waits for udev's device unit, as on a real machine.
	_, err = execRunner{}.Run(ctx, "udevadm", "trigger", "--action=add", "--subsystem-match=block")
	require.NoError(t, err)
	_, err = execRunner{}.Run(ctx, "udevadm", "settle")
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = execRunner{}.Run(ctx, "losetup", "-d", dev)
		_ = os.Remove(img)
	})

	req := applyRequest[mountSpec]{Record: "host-mount/it", Spec: mountSpec{Device: dev, Mountpoint: "/srv/it-mnt", FSType: "ext4", Format: true}}
	info, err := m.applyMount(ctx, req)
	require.NoError(t, err)
	require.True(t, info.Formatted)
	src, fsType := m.mounted(ctx, "/srv/it-mnt")
	require.Equal(t, dev, src)
	require.Equal(t, "ext4", fsType)
	require.NoError(t, os.WriteFile("/srv/it-mnt/keep", []byte("data"), 0o600))

	require.NoError(t, m.removeMount(ctx, req))
	src, _ = m.mounted(ctx, "/srv/it-mnt")
	require.Empty(t, src, "removal unmounts")
	sig, err := m.signature(ctx, dev)
	require.NoError(t, err)
	require.Equal(t, "ext4", sig, "removal never wipes")

	// Again, without Format: the filesystem is reused, the data is there.
	again := applyRequest[mountSpec]{Record: "host-mount/it-2", Spec: mountSpec{Device: dev, Mountpoint: "/srv/it-mnt", FSType: "ext4"}}
	info, err = m.applyMount(ctx, again)
	require.NoError(t, err)
	require.False(t, info.Formatted)
	got, err := os.ReadFile("/srv/it-mnt/keep")
	require.NoError(t, err)
	require.Equal(t, "data", string(got))

	// Another record checks the mount it did not make.
	check := applyRequest[mountSpec]{Record: "host-mount/it-check", Spec: mountSpec{Device: dev, Mountpoint: "/srv/it-mnt", FSType: "ext4", Check: true}}
	_, err = m.applyMount(ctx, check)
	require.NoError(t, err)
	wrong := check
	wrong.Spec.FSType = "xfs"
	_, err = m.applyMount(ctx, wrong)
	require.True(t, isConflict(err), "got %v", err)
	require.NoError(t, m.removeMount(ctx, check))
	src, _ = m.mounted(ctx, "/srv/it-mnt")
	require.Equal(t, dev, src, "a check's removal leaves the mount alone")

	require.NoError(t, m.removeMount(ctx, again))
}
