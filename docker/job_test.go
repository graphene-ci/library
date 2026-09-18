package dockerlib

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/pkg/stdcopy"
	pa "github.com/graphene-ci/pipeline/pkg/activity"
	"github.com/graphene-ci/pipeline/pkg/pipeline"
	"github.com/stretchr/testify/require"
)

func TestJobSpecValidate(t *testing.T) {
	t.Parallel()
	require.ErrorContains(t, JobSpec{}.validate(), "name is required")
	require.ErrorContains(t, JobSpec{Name: "probe"}.validate(), "image is required")
	require.ErrorContains(t, JobSpec{Name: "probe", Config: &container.Config{}}.validate(), "image is required")
	require.NoError(t, JobSpec{Name: "probe", Config: &container.Config{Image: "fixture"}}.validate())
}

// The job reads the exit status AFTER the container stops; docker's own
// auto-removal would race that read, so it is cleared — on a copy.
func TestJobHostConfigClearsAutoRemove(t *testing.T) {
	t.Parallel()
	require.NotNil(t, JobSpec{}.hostConfig())
	given := &container.HostConfig{AutoRemove: true, NetworkMode: "host"}
	got := JobSpec{Host: given}.hostConfig()
	require.False(t, got.AutoRemove)
	require.Equal(t, container.NetworkMode("host"), got.NetworkMode)
	require.True(t, given.AutoRemove, "the caller's spec must not be mutated")
}

// Docker multiplexes stdout and stderr into one framed stream: the report's
// Stdout must carry stdout alone, the log file and the tail — both.
func TestDrainJobOutputSplitsStreams(t *testing.T) {
	t.Parallel()
	var wire bytes.Buffer
	stdout, stderr := stdcopy.NewStdWriter(&wire, stdcopy.Stdout), stdcopy.NewStdWriter(&wire, stdcopy.Stderr)
	_, _ = stdout.Write([]byte("{\"ok\":"))
	_, _ = stderr.Write([]byte("warming up\n"))
	_, _ = stdout.Write([]byte("true}\n"))
	_, _ = stderr.Write([]byte("done")) // no trailing newline: still a line

	var logFile bytes.Buffer
	out, tail, err := drainJobOutput(context.Background(), &wire, false, &logFile, "probe")
	require.NoError(t, err)
	require.Equal(t, "{\"ok\":true}\n", out)
	require.Equal(t, "{\"ok\":warming up\ntrue}\ndone", logFile.String())
	require.Equal(t, logFile.String(), tail)
}

// A container with a TTY writes one raw stream: no frames to strip.
func TestDrainJobOutputTTY(t *testing.T) {
	t.Parallel()
	var logFile bytes.Buffer
	out, tail, err := drainJobOutput(context.Background(), strings.NewReader("line one\r\nline two\n"), true, &logFile, "probe")
	require.NoError(t, err)
	require.Equal(t, "line one\r\nline two\n", out)
	require.Equal(t, out, logFile.String())
	require.Equal(t, out, tail)
}

func TestJobBuffersAreBounded(t *testing.T) {
	t.Parallel()
	head := &headBuffer{limit: 4}
	_, _ = head.Write([]byte("ab"))
	_, _ = head.Write([]byte("cdef"))
	_, _ = head.Write([]byte("gh"))
	require.Equal(t, "abcd", head.String())

	tail := &tailBuffer{limit: 4}
	_, _ = tail.Write([]byte("ab"))
	_, _ = tail.Write([]byte("cdef"))
	require.Equal(t, "cdef", tail.String())
}

func TestJobLineWriterEmitsWholeLines(t *testing.T) {
	t.Parallel()
	var sink bytes.Buffer
	w := &jobLineWriter{ctx: context.Background(), name: "probe", sinks: &sink}
	n, err := w.Write([]byte("first\nsec"))
	require.NoError(t, err)
	require.Equal(t, 9, n)
	require.Equal(t, "sec", string(w.partial), "an unfinished line waits for its newline")
	_, _ = w.Write([]byte("ond\n"))
	require.Empty(t, w.partial)
	require.Equal(t, "first\nsecond\n", sink.String())
}

// No newline, no record — unless the line is endless: then it is cut, so the
// buffer never grows with the output.
func TestJobLineWriterCutsEndlessLine(t *testing.T) {
	t.Parallel()
	var sink bytes.Buffer
	w := &jobLineWriter{ctx: context.Background(), name: "probe", sinks: &sink}
	chunk := bytes.Repeat([]byte("x"), 32<<10)
	for range 100 {
		_, err := w.Write(chunk)
		require.NoError(t, err)
		require.Less(t, len(w.partial), jobLineLimit)
		require.LessOrEqual(t, cap(w.partial), 4*jobLineLimit)
	}
	require.Equal(t, 100*len(chunk), sink.Len(), "the sinks still get every byte")
}

// A Job is a Call: using it is what registers its body — no list to keep.
func TestJobRegistersItsActivity(t *testing.T) {
	t.Parallel()
	d, err := pipeline.Prepare("jobs", func(ctx pipeline.Context, _ struct{}) (JobReport, error) {
		a := pipeline.NewAgent(ctx, "agent")
		return pa.Activity(ctx, a, Job(JobSpec{Name: "probe", Config: &container.Config{Image: "fixture"}}))
	})
	require.NoError(t, err)
	require.Contains(t, d.Activities(), jobActivityName)
}
