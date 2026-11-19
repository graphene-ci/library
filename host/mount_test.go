package host

import (
	"context"
	"errors"
	"os/exec"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMountUnitName(t *testing.T) {
	t.Parallel()
	cases := []struct{ path, want string }{
		{path: "/data", want: "data.mount"},
		{path: "/srv/ydb/pdisk-1", want: `srv-ydb-pdisk\x2d1.mount`},
		{path: "/mnt/.hidden", want: `mnt-.hidden.mount`},
		{path: "/.data", want: `\x2edata.mount`},
		{path: "/var/lib/a b", want: `var-lib-a\x20b.mount`},
	}
	for _, c := range cases {
		t.Run(c.path, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, c.want, mountUnitName(c.path))
		})
	}
}

func TestValidateMount(t *testing.T) {
	t.Parallel()
	ok := mountSpec{Device: "/dev/disk/by-id/nvme-X", Mountpoint: "/data", FSType: "xfs", Options: []string{"noatime"}}
	cases := []struct {
		name string
		edit func(*mountSpec)
		ok   bool
	}{
		{name: "ok", edit: func(*mountSpec) {}, ok: true},
		{name: "check", edit: func(s *mountSpec) { s.Check = true }, ok: true},
		{name: "not a device", edit: func(s *mountSpec) { s.Device = "/data.img" }},
		{name: "root", edit: func(s *mountSpec) { s.Mountpoint = "/" }},
		{name: "fs", edit: func(s *mountSpec) { s.FSType = "ntfs" }},
		{name: "option injection", edit: func(s *mountSpec) { s.Options = []string{"rw,exec"} }},
		{name: "check formats", edit: func(s *mountSpec) { s.Check, s.Format = true, true }},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			s := ok
			c.edit(&s)
			require.Equal(t, c.ok, validateMount(s) == nil)
		})
	}
}

// exitErr is what blkid returns for a blank device: exit status 2.
func exitErr(t *testing.T) error {
	t.Helper()
	err := exec.Command("sh", "-c", "exit 2").Run()
	var exit *exec.ExitError
	require.True(t, errors.As(err, &exit))
	return err
}

func TestMountApply(t *testing.T) {
	t.Parallel()
	const id = "/dev/disk/by-id/nvme-DATA"
	const dev = "/dev/nvme1n1"
	blank := func(t *testing.T) answer { return answer{err: exitErr(t)} }
	cases := []struct {
		name       string
		format     bool
		blkid      func(*testing.T) answer
		mountedNow string
		conflict   bool
		mkfs       bool
	}{
		{name: "blank, format", format: true, blkid: blank, mkfs: true},
		{name: "same filesystem", blkid: func(*testing.T) answer { return answer{out: "DEVNAME=/dev/nvme1n1\nTYPE=xfs\n"} }},
		{name: "blank, no format", blkid: blank, conflict: true},
		{name: "another filesystem", format: true, blkid: func(*testing.T) answer { return answer{out: "TYPE=ext4\n"} }, conflict: true},
		{name: "partition table", format: true, blkid: func(*testing.T) answer { return answer{out: "PTTYPE=gpt\n"} }, conflict: true},
		{name: "something else mounted there", blkid: func(*testing.T) answer { return answer{out: "TYPE=xfs\n"} }, mountedNow: "/dev/sdz1 xfs", conflict: true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			r := &scripted{answers: map[string]answer{
				"readlink -e " + id:         {out: dev + "\n"},
				"blkid -p -o export " + dev: c.blkid(t),
			}, seq: map[string][]answer{
				// Before the start, then after it.
				"findmnt -n -o SOURCE,FSTYPE --mountpoint /data": {{out: c.mountedNow}, {out: dev + " xfs"}},
			}}
			m := NewMachine(t.TempDir(), r)
			req := applyRequest[mountSpec]{Record: "host-mount/m-data", Spec: mountSpec{Device: id, Mountpoint: "/data", FSType: "xfs", Format: c.format}}
			info, err := m.applyMount(context.Background(), req)
			if c.conflict {
				require.True(t, isConflict(err), "got %v", err)
				require.NotContains(t, strings.Join(r.Calls(), "\n"), "mkfs", "nothing is formatted over a refusal")
				require.NoError(t, m.removeMount(context.Background(), req))
				return
			}
			require.NoError(t, err)
			require.Equal(t, MountInfo{Device: dev, Mountpoint: "/data", FSType: "xfs", Unit: "data.mount", Formatted: c.mkfs}, info)
			require.Equal(t, c.mkfs, strings.Contains(strings.Join(r.Calls(), "\n"), "mkfs.xfs "+dev))
			unit := readFile(t, m.root, "/etc/systemd/system/data.mount")
			require.Contains(t, unit, "What="+id+"\nWhere=/data\nType=xfs\n")
			require.Contains(t, r.Calls(), "systemctl enable --now data.mount")

			// A retry right after the format: the fresh filesystem is the
			// record's own, not someone else's to refuse.
			if c.mkfs {
				r.answers["blkid -p -o export "+dev] = answer{out: "TYPE=xfs\n"}
				again, err := m.applyMount(context.Background(), req)
				require.NoError(t, err)
				require.True(t, again.Formatted)
			}

			require.NoError(t, m.removeMount(context.Background(), req))
			require.Contains(t, r.Calls(), "systemctl disable --now data.mount")
			require.NotContains(t, strings.Join(r.Calls(), "\n"), "wipefs", "removal never wipes")
		})
	}
}

// A mount unit whose start failed (a device that never appeared) exits 0;
// the record checks the mount itself.
func TestMountThatDoesNotComeUp(t *testing.T) {
	t.Parallel()
	const dev = "/dev/nvme1n1"
	r := &scripted{answers: map[string]answer{
		"readlink -e " + dev:                             {out: dev + "\n"},
		"blkid -p -o export " + dev:                      {out: "TYPE=xfs\n"},
		"findmnt -n -o SOURCE,FSTYPE --mountpoint /data": {out: ""},
	}}
	m := NewMachine(t.TempDir(), r)
	_, err := m.applyMount(context.Background(), applyRequest[mountSpec]{Record: "host-mount/m-data", Spec: mountSpec{Device: dev, Mountpoint: "/data", FSType: "xfs"}})
	require.ErrorContains(t, err, "did not come up")
}

func TestMountCheck(t *testing.T) {
	t.Parallel()
	const id = "/dev/disk/by-id/ata-OWNERS"
	cases := []struct {
		name    string
		mounted string
		ok      bool
	}{
		{name: "prepared", mounted: "/dev/sdb xfs", ok: true},
		{name: "not mounted", mounted: ""},
		{name: "other device", mounted: "/dev/sdc xfs"},
		{name: "other type", mounted: "/dev/sdb ext4"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			r := &scripted{answers: map[string]answer{
				"readlink -e " + id: {out: "/dev/sdb\n"},
				"findmnt -n -o SOURCE,FSTYPE --mountpoint /data": {out: c.mounted},
			}}
			m := NewMachine(t.TempDir(), r)
			req := applyRequest[mountSpec]{Record: "host-mount/m-data", Spec: mountSpec{Device: id, Mountpoint: "/data", FSType: "xfs", Check: true}}
			_, err := m.applyMount(context.Background(), req)
			require.Equal(t, c.ok, err == nil, "err %v", err)
			before := len(r.Calls())
			require.NoError(t, m.removeMount(context.Background(), req))
			require.Len(t, r.Calls(), before, "a check never changes the machine")
			for _, call := range r.Calls() {
				require.NotRegexp(t, `^(mkfs|systemctl|mount)`, call)
			}
		})
	}
}

func BenchmarkMountUnitName(b *testing.B) {
	for b.Loop() {
		mountUnitName("/srv/ydb/pdisk-1")
	}
}
