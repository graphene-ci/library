package dockerlib

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"strconv"
	"sync"
	"time"

	cerrdefs "github.com/containerd/errdefs"
	"github.com/docker/docker/api/types/container"
	dockerclient "github.com/docker/docker/client"

	"github.com/graphene-ci/pipeline/pkg/obs"
)

// A container's log is a STREAM, and it is read as one: the executor
// follows every container it runs and ships each line the moment docker
// hands it over — the OpenTelemetry log pipeline batches them about once
// a second. A beat that polled the log every 30s delivered lines that
// late, and lost all of them written after its last pass when the
// container was removed or died: the lines that explain a crash are the
// last ones a container writes.
//
// The follower belongs to the executor PROCESS, started once when its
// worker is built. Activities only tell it which container to follow,
// from where, and when to let go.

// daemon is the part of the docker API the follower uses.
type daemon interface {
	ContainerLogs(ctx context.Context, id string, opts container.LogsOptions) (io.ReadCloser, error)
	ContainerInspect(ctx context.Context, id string) (container.InspectResponse, error)
}

// tail follows one container.
type tail struct {
	id, entity string
	mu         sync.Mutex
	// cursor is where the tail started: nothing at or before it is
	// shipped. streams is the docker timestamp (unix nanos) of the last
	// line shipped PER STREAM: docker stamps stdout and stderr lines as
	// they arrive and writes them to its log as they are copied, so the
	// two streams interleave out of time order — a stderr line stamped
	// before the stdout line read just ahead of it is still new.
	cursor  int64
	streams map[string]int64
	status  string // the last status this tail reported
	stop    chan struct{}
	done    chan struct{}
}

// position is the cursor a reader can resume from without a gap: the
// oldest of the streams' last lines (lines of the other stream between it
// and their own last are dropped by shipLine), the start when nothing has
// been shipped.
func (t *tail) position() (int64, string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	pos := int64(0)
	for _, ts := range t.streams {
		if pos == 0 || ts < pos {
			pos = ts
		}
	}
	return max(pos, t.cursor), t.status
}

// seen is the stream's own edge: the start, or its last line shipped.
func (t *tail) seen(stream string) int64 {
	return max(t.cursor, t.streams[stream])
}

// follower is every tail of this process.
type follower struct {
	dial    func() (daemon, error)
	retry   time.Duration
	mu      sync.Mutex
	tails   map[string]*tail
	started chan *tail
}

func newFollower(dial func() (daemon, error)) *follower {
	return &follower{dial: dial, retry: 2 * time.Second, tails: map[string]*tail{}, started: make(chan *tail)}
}

// logs is the process's follower; run starts it.
var logs = newFollower(func() (daemon, error) {
	return dockerclient.NewClientWithOpts(dockerclient.FromEnv, dockerclient.WithAPIVersionNegotiation())
})

var startLogs sync.Once

// run owns every tail goroutine: started once from the worker's
// construction, it lives as long as the process.
func (f *follower) run() {
	for t := range f.started {
		go f.follow(t)
	}
}

// watch follows a container from a cursor; following one already followed
// changes nothing. It returns at once.
func (f *follower) watch(id, entity string, since int64, status string) {
	f.mu.Lock()
	if _, ok := f.tails[id]; ok {
		f.mu.Unlock()
		return
	}
	t := &tail{id: id, entity: entity, cursor: since, streams: map[string]int64{}, status: status, stop: make(chan struct{}), done: make(chan struct{})}
	f.tails[id] = t
	f.mu.Unlock()
	f.started <- t
}

// tailOf is the container's tail, nil when it is not followed.
func (f *follower) tailOf(id string) *tail {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.tails[id]
}

// release stops following a container and hands back its tail — the
// cursor of every stream, so whoever reads on does not ship a line twice;
// nil when it was not followed. The wait for the tail to let go is
// bounded by ctx.
func (f *follower) release(ctx context.Context, id string) *tail {
	f.mu.Lock()
	t, ok := f.tails[id]
	delete(f.tails, id)
	f.mu.Unlock()
	if !ok {
		return nil
	}
	close(t.stop)
	select {
	case <-t.done:
	case <-ctx.Done():
	}
	return t
}

// follow streams one container's log until released. A broken stream —
// the daemon restarted, the container restarted — is picked up again
// from the cursor; a container that is not running is watched until it
// runs again or is released, and its stop is reported the moment the
// stream ends rather than at the next beat.
func (f *follower) follow(t *tail) {
	defer close(t.done)
	ctx, cancel := context.WithCancel(obs.WithEntity(context.Background(), t.entity))
	defer cancel()
	go func() {
		select {
		case <-t.stop:
			cancel()
		case <-ctx.Done():
		}
	}()
	for ctx.Err() == nil {
		cli, err := f.dial()
		if err == nil {
			err = f.stream(ctx, cli, t)
			f.settle(ctx, cli, t)
		}
		if cerrdefs.IsNotFound(err) {
			// Removed under us: nothing more will come.
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(f.retry):
		}
	}
}

// stream reads the log from the cursor until it ends.
func (f *follower) stream(ctx context.Context, cli daemon, t *tail) error {
	insp, err := cli.ContainerInspect(ctx, t.id)
	if err != nil {
		return err
	}
	since, _ := t.position()
	rc, err := cli.ContainerLogs(ctx, t.id, container.LogsOptions{
		ShowStdout: true, ShowStderr: true, Follow: true, Timestamps: true, Since: sinceArg(since),
	})
	if err != nil {
		return err
	}
	defer func() { _ = rc.Close() }()
	return readLines(ctx, rc, insp.Config != nil && insp.Config.Tty, t)
}

// settle reports a container that stopped: its stream has ended, and a
// transition is the record's news now, not 30 seconds later.
func (f *follower) settle(ctx context.Context, cli daemon, t *tail) {
	insp, err := cli.ContainerInspect(ctx, t.id)
	if err != nil || insp.State == nil {
		return
	}
	t.mu.Lock()
	prev := t.status
	t.status = insp.State.Status
	t.mu.Unlock()
	if prev != "" && prev != insp.State.Status {
		reportTransition(ctx, t.id, prev, insp.State.Status, insp.State.ExitCode)
	}
}

// drain reads what a container wrote after the cursor up to now, without
// following — the last words before it goes away.
func drain(ctx context.Context, cli daemon, t *tail) error {
	insp, err := cli.ContainerInspect(ctx, t.id)
	if err != nil {
		return err
	}
	since, _ := t.position()
	rc, err := cli.ContainerLogs(ctx, t.id, container.LogsOptions{
		ShowStdout: true, ShowStderr: true, Timestamps: true, Since: sinceArg(since),
	})
	if err != nil {
		return err
	}
	defer func() { _ = rc.Close() }()
	return readLines(ctx, rc, insp.Config != nil && insp.Config.Tty, t)
}

// sinceArg is docker's "since" for a cursor: the exact nanosecond, so
// nothing between the last shipped line and now is skipped. Lines AT the
// cursor come back again and are dropped by readLines.
func sinceArg(cursor int64) string {
	if cursor <= 0 {
		return ""
	}
	return strconv.FormatInt(cursor/1e9, 10) + "." + leftPad(strconv.FormatInt(cursor%1e9, 10), 9)
}

func leftPad(s string, n int) string {
	for len(s) < n {
		s = "0" + s
	}
	return s
}

// readLines ships every line of a docker log stream newer than the
// cursor, advancing the cursor line by line — a stream cut at any point
// leaves it exactly at the last line shipped. Docker writes frames in
// time order; they are read in that order (not demultiplexed into two
// pipes read side by side), so the cursor only moves forward and no line
// of one stream is skipped as "older" than the other stream's last.
func readLines(ctx context.Context, src io.Reader, tty bool, t *tail) error {
	if tty {
		return scanLines(ctx, src, t, "container")
	}
	r := bufio.NewReaderSize(src, 64*1024)
	partial := map[string][]byte{}
	header := make([]byte, 8)
	for {
		if _, err := io.ReadFull(r, header); err != nil {
			for stream, rest := range partial {
				if len(rest) > 0 {
					shipLine(ctx, t, string(rest), stream)
				}
			}
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				return nil
			}
			return err
		}
		stream := "stdout"
		if header[0] == 2 {
			stream = "stderr"
		}
		size := binary.BigEndian.Uint32(header[4:8])
		payload := make([]byte, size)
		if _, err := io.ReadFull(r, payload); err != nil {
			return err
		}
		partial[stream] = append(partial[stream], payload...)
		buf := partial[stream]
		for {
			i := bytes.IndexByte(buf, '\n')
			if i < 0 {
				break
			}
			shipLine(ctx, t, string(buf[:i]), stream)
			buf = buf[i+1:]
		}
		if len(buf) > maxLine {
			shipLine(ctx, t, string(buf), stream)
			buf = nil
		}
		partial[stream] = buf
	}
}

// maxLine bounds a line without a newline before it is shipped as is.
const maxLine = 1 << 20

func scanLines(ctx context.Context, src io.Reader, t *tail, stream string) error {
	scan := bufio.NewScanner(src)
	scan.Buffer(make([]byte, 0, 64*1024), maxLine)
	for scan.Scan() {
		shipLine(ctx, t, scan.Text(), stream)
	}
	return scan.Err()
}

// shipLine ships one "<docker stamp> <body>" line unless the cursor has
// already passed it.
func shipLine(ctx context.Context, t *tail, line, stream string) {
	ts, body := splitDockerStamp(line)
	t.mu.Lock()
	if t.streams == nil {
		t.streams = map[string]int64{}
	}
	if ts > 0 && ts <= t.seen(stream) {
		t.mu.Unlock()
		return // already shipped: the window's edge overlaps
	}
	if ts > t.streams[stream] {
		t.streams[stream] = ts
	}
	t.mu.Unlock()
	emit(ctx, body, stream)
}

// emit ships one log line of a container; a variable so tests can see
// what was shipped.
var emit = func(ctx context.Context, body, stream string) {
	obs.Info(ctx, body, obs.Str("stream", stream))
}
