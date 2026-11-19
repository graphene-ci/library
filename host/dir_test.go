package host

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestValidateDir(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		spec dirSpec
		ok   bool
	}{
		{name: "data dir", spec: dirSpec{Path: "/data/run-1/pg"}, ok: true},
		{name: "root", spec: dirSpec{Path: "/"}},
		{name: "relative", spec: dirSpec{Path: "data/x"}},
		{name: "unclean", spec: dirSpec{Path: "/data/../etc"}},
		{name: "trash", spec: dirSpec{Path: "/data/.graphene-trash"}},
		{name: "mode", spec: dirSpec{Path: "/data/x", Mode: 0o17777}},
		{name: "uid", spec: dirSpec{Path: "/data/x", UID: -1}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, c.ok, validateDir(c.spec) == nil)
		})
	}
}

func TestDirLifecycle(t *testing.T) {
	t.Parallel()
	m, root := fixture(t, nil)
	req := applyRequest[dirSpec]{Record: "host-dir/m-data-run-1-pg", Spec: dirSpec{Path: "/data/run-1/pg", Mode: 0o700, UID: os.Getuid(), GID: os.Getgid()}}
	info, err := m.applyDir(context.Background(), req)
	require.NoError(t, err)
	require.Equal(t, "/data/run-1/pg", info.Path)
	st, err := os.Stat(filepath.Join(root, "/data/run-1/pg"))
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0o700), st.Mode().Perm())

	// A retried apply is fine; the dir is the record's own.
	_, err = m.applyDir(context.Background(), req)
	require.NoError(t, err)

	// Contents the workload wrote go with the record.
	big := filepath.Join(root, "/data/run-1/pg/base/1")
	require.NoError(t, os.MkdirAll(big, 0o750))
	require.NoError(t, os.WriteFile(filepath.Join(big, "16384"), make([]byte, 1<<20), 0o600))

	require.NoError(t, m.removeDir(context.Background(), req))
	_, err = os.Stat(filepath.Join(root, "data/run-1/pg"))
	require.True(t, os.IsNotExist(err))
	_, err = os.Stat(filepath.Join(root, "data/run-1", trashDir))
	require.True(t, os.IsNotExist(err), "an empty trash is removed")
	require.NoError(t, m.removeDir(context.Background(), req))
}

// A directory the record did not create is never adopted: removing the
// record would delete someone else's data.
func TestDirRefusesExisting(t *testing.T) {
	t.Parallel()
	m, root := fixture(t, map[string]string{"/data/theirs/file": "keep"})
	req := applyRequest[dirSpec]{Record: "host-dir/m-data-theirs", Spec: dirSpec{Path: "/data/theirs", UID: os.Getuid(), GID: os.Getgid()}}
	_, err := m.applyDir(context.Background(), req)
	require.True(t, isConflict(err), "got %v", err)
	require.NoError(t, m.removeDir(context.Background(), req))
	require.Equal(t, "keep", readFile(t, root, "/data/theirs/file"))
}

// The trash survives while another record's leftovers sit in it.
func TestDirTrashKeepsLeftovers(t *testing.T) {
	t.Parallel()
	m, root := fixture(t, map[string]string{"/data/" + trashDir + "/old.host-dir-x/f": "left"})
	req := applyRequest[dirSpec]{Record: "host-dir/m-data-new", Spec: dirSpec{Path: "/data/new", UID: os.Getuid(), GID: os.Getgid()}}
	_, err := m.applyDir(context.Background(), req)
	require.NoError(t, err)
	require.NoError(t, m.removeDir(context.Background(), req))
	require.Equal(t, "left", readFile(t, root, "/data/"+trashDir+"/old.host-dir-x/f"))
}

// A symlink a workload planted cannot take a record outside the machine:
// /data pointing elsewhere must not create (or later delete) a tree there.
func TestDirStaysOnTheMachine(t *testing.T) {
	t.Parallel()
	m, root := fixture(t, nil)
	outside := t.TempDir()
	require.NoError(t, os.Symlink(outside, filepath.Join(root, "data")))
	req := applyRequest[dirSpec]{Record: "host-dir/m-data-x", Spec: dirSpec{Path: "/data/x", UID: os.Getuid(), GID: os.Getgid()}}
	_, err := m.applyDir(context.Background(), req)
	require.Error(t, err)
	_, err = os.Stat(filepath.Join(outside, "x"))
	require.True(t, os.IsNotExist(err), "nothing may appear outside the machine root")
}

func BenchmarkPurge(b *testing.B) {
	m, root := fixture(b, nil)
	f, err := m.open()
	if err != nil {
		b.Fatal(err)
	}
	defer f.Close()
	for b.Loop() {
		b.StopTimer()
		dir, err := os.MkdirTemp(root, "tree")
		if err != nil {
			b.Fatal(err)
		}
		for i := range 100 {
			p := filepath.Join(dir, fmt.Sprint(i%10), fmt.Sprint(i))
			if err := os.MkdirAll(filepath.Dir(p), 0o750); err != nil {
				b.Fatal(err)
			}
			if err := os.WriteFile(p, []byte("x"), 0o600); err != nil {
				b.Fatal(err)
			}
		}
		b.StartTimer()
		if err := purge(context.Background(), f, filepath.Base(dir)); err != nil {
			b.Fatal(err)
		}
	}
}
