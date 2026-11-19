package host

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/temporal"
)

// answer is a scripted program result.
type answer struct {
	out string
	err error
}

// scripted is a Runner that answers from a table and records every call.
// A call with a queue in seq takes its answers in order (the last one
// repeats); otherwise answers holds its one answer.
type scripted struct {
	mu      sync.Mutex
	calls   []string
	answers map[string]answer
	seq     map[string][]answer
}

func (s *scripted) Run(_ context.Context, name string, args ...string) (string, error) {
	call := strings.Join(append([]string{name}, args...), " ")
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls = append(s.calls, call)
	if q := s.seq[call]; len(q) > 0 {
		a := q[0]
		if len(q) > 1 {
			s.seq[call] = q[1:]
		}
		return a.out, a.err
	}
	a := s.answers[call]
	return a.out, a.err
}

func (s *scripted) Calls() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.calls...)
}

// fixture builds a machine rooted in a temporary directory, with files.
func fixture(t testing.TB, files map[string]string) (Machine, string) {
	t.Helper()
	root := t.TempDir()
	r, err := os.OpenRoot(root)
	require.NoError(t, err)
	defer func() { _ = r.Close() }()
	for p, content := range files {
		require.NoError(t, r.MkdirAll(rel(filepath.Dir(p)), 0o750))
		require.NoError(t, r.WriteFile(rel(p), []byte(content), 0o600))
	}
	return NewMachine(root, &scripted{}), root
}

// opened opens the machine's filesystem for a test.
func opened(t testing.TB, m Machine) fsys {
	t.Helper()
	f, err := m.open()
	require.NoError(t, err)
	t.Cleanup(f.Close)
	return f
}

func isConflict(err error) bool {
	var app *temporal.ApplicationError
	return errors.As(err, &app) && app.Type() == ConflictCode && app.NonRetryable()
}

func TestClaims(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name      string
		first     string
		second    string
		conflict  bool
		wantPrior string
	}{
		{name: "same record keeps its first prior", first: "rec/a", second: "rec/a", wantPrior: "first"},
		{name: "other record is refused", first: "rec/a", second: "rec/b", conflict: true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			m, _ := fixture(t, nil)
			f := opened(t, m)
			reads := []string{"first", "second"}
			read := func() (string, error) {
				v := reads[0]
				reads = reads[1:]
				return v, nil
			}
			_, err := f.take("kind", "k/1", c.first, read)
			require.NoError(t, err)
			got, err := f.take("kind", "k/1", c.second, read)
			if c.conflict {
				require.True(t, isConflict(err), "want a non-retryable %s, got %v", ConflictCode, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, c.wantPrior, got.Prior)
		})
	}
}

func TestClaimRelease(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name    string
		holder  string
		dropper string
		gone    bool
	}{
		{name: "holder drops", holder: "rec/a", dropper: "rec/a", gone: true},
		{name: "stranger leaves it", holder: "rec/a", dropper: "rec/b", gone: false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			m, _ := fixture(t, nil)
			f := opened(t, m)
			_, err := f.take("kind", "key", c.holder, func() (string, error) { return "v", nil })
			require.NoError(t, err)
			require.NoError(t, f.drop("kind", "key", c.dropper))
			_, ours, err := f.held("kind", "key", c.holder)
			require.NoError(t, err)
			require.Equal(t, !c.gone, ours)
			// Dropping twice is retry-safe.
			require.NoError(t, f.drop("kind", "key", c.dropper))
		})
	}
}

func TestClaimRaceHasOneWinner(t *testing.T) {
	t.Parallel()
	m, _ := fixture(t, nil)
	f := opened(t, m)
	const contenders = 16
	var wg sync.WaitGroup
	results := make([]error, contenders)
	for i := range contenders {
		wg.Go(func() {
			_, results[i] = f.take("kind", "key", fmt.Sprintf("rec/%d", i), func() (string, error) { return "", nil })
		})
	}
	wg.Wait()
	wins := 0
	for _, err := range results {
		if err == nil {
			wins++
			continue
		}
		require.True(t, isConflict(err), "loser must be refused, got %v", err)
	}
	require.Equal(t, 1, wins)
}

// A symlink planted where a record writes is replaced, never followed: its
// target keeps its bytes.
func TestWriteFileReplacesSymlink(t *testing.T) {
	t.Parallel()
	m, root := fixture(t, map[string]string{"/etc/shadow": "secret"})
	require.NoError(t, os.MkdirAll(filepath.Join(root, "etc/systemd/system"), 0o750))
	require.NoError(t, os.Symlink("../../shadow", filepath.Join(root, "etc/systemd/system/x.service")))
	f := opened(t, m)
	require.NoError(t, f.writeFile("/etc/systemd/system/x.service", []byte("[Service]\n"), 0o600))
	require.Equal(t, "secret", readFile(t, root, "/etc/shadow"))
	require.Equal(t, "[Service]\n", readFile(t, root, "/etc/systemd/system/x.service"))
	// A retry after a crash that left the temporary file behind succeeds.
	require.NoError(t, f.r.WriteFile(rel("/etc/systemd/system/x.service.graphene-tmp"), []byte("stale"), 0o600))
	require.NoError(t, f.writeFile("/etc/systemd/system/x.service", []byte("v2"), 0o600))
	require.Equal(t, "v2", readFile(t, root, "/etc/systemd/system/x.service"))
}

func TestExecRunner(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name    string
		program string
		args    []string
		out     string
		fails   bool
	}{
		{name: "stdout", program: "sh", args: []string{"-c", "echo enabled"}, out: "enabled\n"},
		{name: "stdout survives a failure", program: "sh", args: []string{"-c", "echo disabled; exit 1"}, out: "disabled\n", fails: true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			out, err := execRunner{}.Run(context.Background(), c.program, c.args...)
			require.Equal(t, c.out, out)
			require.Equal(t, c.fails, err != nil)
		})
	}
}

func BenchmarkTake(b *testing.B) {
	m, _ := fixture(b, nil)
	f := opened(b, m)
	for b.Loop() {
		if _, err := f.take("kind", "key", "rec/a", func() (string, error) { return "v", nil }); err != nil {
			b.Fatal(err)
		}
	}
}
