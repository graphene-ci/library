package dockerlib

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/docker/docker/api/types/container"
	"github.com/stretchr/testify/require"
)

// frame is one docker multiplexed log frame.
func frame(stream byte, payload string) []byte {
	h := make([]byte, 8)
	h[0] = stream
	binary.BigEndian.PutUint32(h[4:], uint32(len(payload))) //nolint:gosec // test payloads are small
	return append(h, payload...)
}

func stamp(i int) string {
	return time.Unix(1_790_000_000, int64(i)*int64(time.Millisecond)).UTC().Format(time.RFC3339Nano)
}

type shipped struct {
	mu    sync.Mutex
	lines []string
}

func (s *shipped) capture(t *testing.T) {
	prev := emit
	emit = func(_ context.Context, body, stream string) {
		s.mu.Lock()
		s.lines = append(s.lines, stream+":"+body)
		s.mu.Unlock()
	}
	t.Cleanup(func() { emit = prev })
}

func (s *shipped) all() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.lines...)
}

// Frames are read in docker's order: a stderr line between two stdout
// lines is not skipped as "older" than the stream read beside it, a line
// split across frames is one line, and a line at or before the cursor is
// not shipped twice.
func TestReadLinesKeepsOrderAndCursor(t *testing.T) {
	var got shipped
	got.capture(t)
	var src bytes.Buffer
	src.Write(frame(1, stamp(1)+" out-1\n"))
	src.Write(frame(2, stamp(2)+" err-2\n"))
	src.Write(frame(1, stamp(3)+" out-"))
	src.Write(frame(1, "3\n"+stamp(4)+" out-4\n"))
	// stderr is stamped BEFORE the stdout line docker wrote ahead of it:
	// the streams interleave out of time order, and it is still new.
	src.Write(frame(1, stamp(6)+" out-6\n"))
	src.Write(frame(2, stamp(5)+" err-5\n"))
	tl := &tail{cursor: time.Unix(1_790_000_000, int64(time.Millisecond)).UnixNano()} // line 1 already shipped
	require.NoError(t, readLines(context.Background(), &src, false, tl))
	require.Equal(t, []string{"stderr:err-2", "stdout:out-3", "stdout:out-4", "stdout:out-6", "stderr:err-5"}, got.all())
	pos, _ := tl.position()
	require.Equal(t, time.Unix(1_790_000_000, 5*int64(time.Millisecond)).UnixNano(), pos, "resume from the older stream's edge")
	// Resuming there replays nothing the other stream already shipped.
	var again bytes.Buffer
	again.Write(frame(1, stamp(6)+" out-6\n"))
	again.Write(frame(2, stamp(5)+" err-5\n"))
	again.Write(frame(2, stamp(7)+" err-7\n"))
	require.NoError(t, readLines(context.Background(), &again, false, tl))
	require.Equal(t, "stderr:err-7", got.all()[len(got.all())-1])
	require.Len(t, got.all(), 6)
}

// fakeDaemon serves a log that grows; the first stream breaks after
// `cut` lines, the way a daemon restart or a dropped connection does.
type fakeDaemon struct {
	mu      sync.Mutex
	lines   int
	cut     int
	streams int
	running bool
}

func (d *fakeDaemon) ContainerInspect(context.Context, string) (container.InspectResponse, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	status := "running"
	if !d.running {
		status = "exited"
	}
	return container.InspectResponse{
		ContainerJSONBase: &container.ContainerJSONBase{State: &container.State{Status: status, Running: d.running, ExitCode: 3}},
		Config:            &container.Config{},
	}, nil
}

func (d *fakeDaemon) ContainerLogs(_ context.Context, _ string, opts container.LogsOptions) (io.ReadCloser, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.streams++
	var since time.Time
	if opts.Since != "" {
		var sec, nsec int64
		if _, err := fmt.Sscanf(opts.Since, "%d.%d", &sec, &nsec); err != nil {
			return nil, err
		}
		since = time.Unix(sec, nsec)
	}
	var buf bytes.Buffer
	for i := 1; i <= d.lines; i++ {
		at := time.Unix(1_790_000_000, int64(i)*int64(time.Millisecond))
		if at.Before(since) {
			continue
		}
		if d.streams == 1 && i > d.cut {
			return io.NopCloser(io.MultiReader(&buf, errReader{})), nil // the connection drops mid-stream
		}
		buf.Write(frame(1, fmt.Sprintf("%s line-%d\n", stamp(i), i)))
	}
	return io.NopCloser(&buf), nil
}

type errReader struct{}

func (errReader) Read([]byte) (int, error) { return 0, errors.New("connection reset by peer") }

// A stream that breaks mid-way is picked up from the cursor: every line
// once, in order — no duplicate at the seam, no gap; the stop of the
// container is reported when its stream ends.
func TestFollowerResumesWithoutDuplicatesOrGaps(t *testing.T) {
	var got shipped
	got.capture(t)
	d := &fakeDaemon{lines: 20, cut: 7, running: false}
	f := newFollower(func() (daemon, error) { return d, nil })
	f.retry = 10 * time.Millisecond
	go f.run()
	f.watch("c1", "docker/db", 0, "running")
	require.Eventually(t, func() bool { return len(got.all()) == 20 }, 2*time.Second, 5*time.Millisecond)
	tl := f.release(context.Background(), "c1")
	require.NotNil(t, tl)
	cursor, status := tl.position()
	require.Equal(t, "exited", status, "the stop is known the moment the stream ends")
	require.Equal(t, time.Unix(1_790_000_000, 20*int64(time.Millisecond)).UnixNano(), cursor)
	want := make([]string, 20)
	for i := range want {
		want[i] = fmt.Sprintf("stdout:line-%d", i+1)
	}
	require.Equal(t, want, got.all())
	require.GreaterOrEqual(t, d.streams, 2, "the broken stream was reopened")
	require.Nil(t, f.release(context.Background(), "c1"), "released once")
}

// The cursor names the exact nanosecond for docker's since.
func TestSinceArg(t *testing.T) {
	require.Empty(t, sinceArg(0))
	require.Equal(t, "1790000000.000000042", sinceArg(1_790_000_000*int64(time.Second)+42))
}
