package dockerlib

import (
	"context"
	"os"
	"path/filepath"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/mount"
	dockerclient "github.com/docker/docker/client"
	"github.com/stretchr/testify/require"
)

// The real daemon, opt-in: GRAPHENE_DOCKER_IT=1 go test -run TestJobAgainstDaemon.
// GRAPHENE_DOCKER_IT_IMAGE overrides the image (any with /bin/sh).
func TestJobAgainstDaemon(t *testing.T) {
	if os.Getenv("GRAPHENE_DOCKER_IT") == "" {
		t.Skip("set GRAPHENE_DOCKER_IT=1 to run against the local docker daemon")
	}
	img := os.Getenv("GRAPHENE_DOCKER_IT_IMAGE")
	if img == "" {
		img = "busybox"
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	work := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(work, "in.txt"), []byte("brought"), 0o600))

	report, err := jobActivity(ctx, JobSpec{
		Name: "graphene-job-it",
		Config: &container.Config{
			Image: img,
			Cmd:   []string{"sh", "-c", `umask 022; echo "answer=$(cat /work/in.txt)"; echo "progress" >&2; echo made > /work/out.txt; exit 3`},
		},
		Host: &container.HostConfig{
			AutoRemove: true, // must not cost us the exit status
			Mounts:     []mount.Mount{{Type: mount.TypeBind, Source: work, Target: "/work"}},
		},
	})
	require.NoError(t, err)
	require.Equal(t, 3, report.ExitCode)
	require.Equal(t, "answer=brought\n", report.Stdout, "stderr must not leak into Stdout")
	require.Contains(t, report.Tail, "progress")

	full, err := os.ReadFile(report.LogPath)
	require.NoError(t, err)
	require.Contains(t, string(full), "answer=brought")
	require.Contains(t, string(full), "progress")

	made, err := os.ReadFile(filepath.Join(work, "out.txt")) //nolint:gosec // the test's own temp dir
	require.NoError(t, err, "what the job wrote into its bind mount stays on the machine")
	require.Equal(t, "made\n", string(made))

	cli, err := dockerclient.NewClientWithOpts(dockerclient.FromEnv, dockerclient.WithAPIVersionNegotiation())
	require.NoError(t, err)
	defer func() { _ = cli.Close() }()
	_, err = cli.ContainerInspect(ctx, "graphene-job-it")
	require.Error(t, err, "the job removes its container")
}

// A job's output is unbounded by nature; the job's memory must not be.
// Two shapes: millions of lines, and one endless line (a progress bar, a
// binary dump, a one-line JSON document) — no newline to cut at.
func TestJobHugeOutputStaysBounded(t *testing.T) {
	if os.Getenv("GRAPHENE_DOCKER_IT") == "" {
		t.Skip("set GRAPHENE_DOCKER_IT=1 to run against the local docker daemon")
	}
	img := os.Getenv("GRAPHENE_DOCKER_IT_IMAGE")
	if img == "" {
		img = "busybox"
	}
	for name, tc := range map[string]struct {
		cmd   string
		bytes int64
	}{
		"two million lines":   {`yes 'the quick brown fox jumps over the lazy dog 0123456789' | head -n 2000000`, 2000000 * 55},
		"100 MiB in one line": {`head -c 104857600 /dev/zero | tr '\0' 'x'`, 104857600},
	} {
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
			defer cancel()
			peak := trackHeapPeak(t)
			report, err := jobActivity(ctx, JobSpec{
				Name:   "graphene-job-it-huge",
				Config: &container.Config{Image: img, Cmd: []string{"sh", "-c", tc.cmd}},
			})
			require.NoError(t, err)
			require.Zero(t, report.ExitCode)
			require.LessOrEqual(t, len(report.Stdout), jobStdoutLimit)
			require.LessOrEqual(t, len(report.Tail), jobTailLimit)
			st, err := os.Stat(report.LogPath)
			require.NoError(t, err)
			require.Equal(t, tc.bytes, st.Size(), "the log file holds ALL of it")
			t.Logf("output %d MiB, heap peak %d MiB", tc.bytes>>20, peak()>>20)
			require.Less(t, peak(), uint64(64<<20), "heap must not scale with the output")
		})
	}
}

// trackHeapPeak samples the heap while the test runs; the returned func
// reports the highest HeapInuse seen so far.
func trackHeapPeak(t *testing.T) func() uint64 {
	t.Helper()
	var peak atomic.Uint64
	done := make(chan struct{})
	t.Cleanup(func() { close(done) })
	go func() {
		tick := time.NewTicker(20 * time.Millisecond)
		defer tick.Stop()
		var m runtime.MemStats
		for {
			select {
			case <-done:
				return
			case <-tick.C:
				runtime.ReadMemStats(&m)
				if m.HeapInuse > peak.Load() {
					peak.Store(m.HeapInuse)
				}
			}
		}
	}()
	return peak.Load
}
