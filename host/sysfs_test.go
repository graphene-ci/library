package host

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSelected(t *testing.T) {
	t.Parallel()
	cases := []struct{ raw, want string }{
		{raw: "always [madvise] never", want: "madvise"},
		{raw: "[none] mq-deadline kyber", want: "none"},
		{raw: "performance", want: "performance"},
		{raw: "broken [bracket", want: "broken [bracket"},
		{raw: "", want: ""},
	}
	for _, c := range cases {
		t.Run(c.raw, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, c.want, selected(c.raw))
		})
	}
}

func TestValidateSysfs(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		spec sysfsSpec
		ok   bool
	}{
		{name: "thp", spec: sysfsSpec{Path: "/sys/kernel/mm/transparent_hugepage/enabled", Value: "never"}, ok: true},
		{name: "outside sys", spec: sysfsSpec{Path: "/etc/passwd", Value: "x"}},
		{name: "escape", spec: sysfsSpec{Path: "/sys/../etc/passwd", Value: "x"}},
		{name: "empty value", spec: sysfsSpec{Path: "/sys/kernel/x", Value: ""}},
		{name: "two lines", spec: sysfsSpec{Path: "/sys/kernel/x", Value: "a\nb"}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, c.ok, validateSysfs(c.spec) == nil)
		})
	}
}

func TestSysfsApplyAndRestore(t *testing.T) {
	t.Parallel()
	const thp = "/sys/kernel/mm/transparent_hugepage/enabled"
	m, root := fixture(t, map[string]string{thp: "always [madvise] never\n"})
	req := applyRequest[sysfsSpec]{Record: "host-sysfs/m-thp", Spec: sysfsSpec{Path: thp, Value: "never"}}
	info, err := m.applySysfs(context.Background(), req)
	require.NoError(t, err)
	require.Equal(t, SysfsInfo{Path: thp, Prior: "madvise"}, info)
	require.Equal(t, "never", readFile(t, root, thp))

	_, err = m.applySysfs(context.Background(), applyRequest[sysfsSpec]{Record: "host-sysfs/other", Spec: sysfsSpec{Path: thp, Value: "always"}})
	require.True(t, isConflict(err), "got %v", err)

	require.NoError(t, m.removeSysfs(context.Background(), req))
	require.Equal(t, "madvise", readFile(t, root, thp))
	require.NoError(t, m.removeSysfs(context.Background(), req))
}

func BenchmarkSelected(b *testing.B) {
	for b.Loop() {
		_ = selected("always [madvise] never")
	}
}
