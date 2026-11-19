package host

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/graphene-ci/library/host/internal/contract"
)

func TestSysctlPath(t *testing.T) {
	t.Parallel()
	cases := []struct {
		key  string
		want string
		bad  bool
	}{
		{key: "vm.swappiness", want: "/proc/sys/vm/swappiness"},
		{key: "net.ipv4.ip_local_port_range", want: "/proc/sys/net/ipv4/ip_local_port_range"},
		{key: "net/ipv4/conf/eth0.1/rp_filter", want: "/proc/sys/net/ipv4/conf/eth0.1/rp_filter"},
		{key: "swappiness", bad: true},
		{key: "vm..swappiness", bad: true},
		{key: "../../etc/passwd", bad: true},
		{key: "vm.swap piness", bad: true},
		{key: "", bad: true},
	}
	for _, c := range cases {
		t.Run(c.key, func(t *testing.T) {
			t.Parallel()
			got, err := sysctlPath(c.key)
			if c.bad {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, c.want, got)
		})
	}
}

func TestValidateSysctl(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name   string
		values map[string]string
		ok     bool
	}{
		{name: "one key", values: map[string]string{"vm.swappiness": "10"}, ok: true},
		{name: "empty", values: nil},
		{name: "two lines", values: map[string]string{"vm.swappiness": "10\nvm.x=1"}},
		{name: "bad key", values: map[string]string{"x": "1"}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			err := validateSysctl(sysctlSpec{Values: c.values})
			require.Equal(t, c.ok, err == nil, "err = %v", err)
		})
	}
}

func readFile(t *testing.T, root, p string) string {
	t.Helper()
	r, err := os.OpenRoot(root)
	require.NoError(t, err)
	defer func() { _ = r.Close() }()
	b, err := r.ReadFile(rel(p))
	require.NoError(t, err)
	return string(b)
}

func TestSysctlApplyAndRestore(t *testing.T) {
	t.Parallel()
	m, root := fixture(t, map[string]string{
		"/proc/sys/vm/swappiness":                "60\n",
		"/proc/sys/net/ipv4/ip_local_port_range": "32768\t60999\n",
		"/etc/sysctl.d/99-unrelated.conf":        "kernel.x = 1\n",
	})
	req := applyRequest[sysctlSpec]{Record: "host-sysctl/m-db", Spec: sysctlSpec{Values: map[string]string{
		"vm.swappiness":                "10",
		"net.ipv4.ip_local_port_range": "1024\t65000",
	}}}
	info, err := m.applySysctl(context.Background(), req)
	require.NoError(t, err)
	require.Equal(t, map[string]string{"vm.swappiness": "60", "net.ipv4.ip_local_port_range": "32768\t60999"}, info.Prior)
	require.Equal(t, "10", readFile(t, root, "/proc/sys/vm/swappiness"))
	require.Equal(t, "net.ipv4.ip_local_port_range = 1024\t65000\nvm.swappiness = 10\n",
		readFile(t, root, "/etc/sysctl.d/90-graphene-host-sysctl-m-db.conf"))

	// Applying again (a retried activity) keeps the ORIGINAL prior.
	info, err = m.applySysctl(context.Background(), req)
	require.NoError(t, err)
	require.Equal(t, "60", info.Prior["vm.swappiness"])

	// Another record cannot take a held key.
	_, err = m.applySysctl(context.Background(), applyRequest[sysctlSpec]{Record: "host-sysctl/other",
		Spec: sysctlSpec{Values: map[string]string{"vm.swappiness": "1"}}})
	require.True(t, isConflict(err), "got %v", err)

	require.NoError(t, m.removeSysctl(context.Background(), req))
	require.Equal(t, "60", readFile(t, root, "/proc/sys/vm/swappiness"))
	require.Equal(t, "32768\t60999", readFile(t, root, "/proc/sys/net/ipv4/ip_local_port_range"))
	_, err = os.Stat(filepath.Join(root, "etc/sysctl.d/90-graphene-host-sysctl-m-db.conf"))
	require.True(t, os.IsNotExist(err))
	require.Equal(t, "kernel.x = 1\n", readFile(t, root, "/etc/sysctl.d/99-unrelated.conf"))
	_, ours, err := opened(t, m).held(contract.SysctlKind, "vm.swappiness", req.Record)
	require.NoError(t, err)
	require.False(t, ours)

	// Removal is retry-safe.
	require.NoError(t, m.removeSysctl(context.Background(), req))
}

// A record whose apply failed part-way restores only the keys it took.
func TestSysctlRemoveLeavesForeignKeys(t *testing.T) {
	t.Parallel()
	m, root := fixture(t, map[string]string{"/proc/sys/vm/swappiness": "60"})
	held := applyRequest[sysctlSpec]{Record: "host-sysctl/holder", Spec: sysctlSpec{Values: map[string]string{"vm.swappiness": "5"}}}
	_, err := m.applySysctl(context.Background(), held)
	require.NoError(t, err)
	loser := applyRequest[sysctlSpec]{Record: "host-sysctl/loser", Spec: sysctlSpec{Values: map[string]string{"vm.swappiness": "1"}}}
	_, err = m.applySysctl(context.Background(), loser)
	require.Error(t, err)
	require.NoError(t, m.removeSysctl(context.Background(), loser))
	require.Equal(t, "5", readFile(t, root, "/proc/sys/vm/swappiness"))
}

func BenchmarkSysctlPath(b *testing.B) {
	for b.Loop() {
		if _, err := sysctlPath("net.ipv4.ip_local_port_range"); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSysctlApplyRemove(b *testing.B) {
	m, _ := fixture(b, map[string]string{"/proc/sys/vm/swappiness": "60"})
	req := applyRequest[sysctlSpec]{Record: "host-sysctl/bench", Spec: sysctlSpec{Values: map[string]string{"vm.swappiness": "10"}}}
	for b.Loop() {
		if _, err := m.applySysctl(context.Background(), req); err != nil {
			b.Fatal(err)
		}
		if err := m.removeSysctl(context.Background(), req); err != nil {
			b.Fatal(err)
		}
	}
}
