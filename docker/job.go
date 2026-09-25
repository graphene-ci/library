package dockerlib

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	cerrdefs "github.com/containerd/errdefs"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/image"
	dockerclient "github.com/docker/docker/client"
	"github.com/docker/docker/pkg/stdcopy"

	"github.com/graphene-ci/pipeline/pkg/activity"
	"github.com/graphene-ci/pipeline/pkg/machine"
	"github.com/graphene-ci/pipeline/pkg/obs"
)

const (
	jobActivityName = "docker.job"
	// jobStdoutLimit bounds the stdout copy that travels back in the
	// report: a report is a workflow payload, not a log transport. The
	// full output is always in the log file.
	jobStdoutLimit = 256 << 10
	// jobTailLimit bounds the merged-output tail of the report. Wider than
	// an error-message tail: a CLI that prints its usage after the error
	// (cobra does) would otherwise push the actual cause out of it.
	jobTailLimit = 16 << 10
	// jobLineLimit is the longest log record a job emits; a longer line
	// is cut into records of this size — obs bounds a record's body at the
	// same 16 KiB, and cutting here keeps every byte where truncation
	// there would drop the rest. The log file is not affected.
	jobLineLimit = 16 << 10
	// jobRemoveTimeout bounds the removal of the finished container; it
	// runs on a context detached from a cancelled activity.
	jobRemoveTimeout = 30 * time.Second
)

// JobSpec describes one THROWAWAY container with docker's OWN types — the
// same pair Container takes. Everything docker already has a word for
// stays in that word: the command is Config.Cmd, the environment is
// Config.Env, files reach the container through Host.Mounts (bind the
// directory a file resource wrote to), the network is Host.NetworkMode.
type JobSpec struct {
	// Name is the container's name on the machine. A leftover container
	// of the same name is replaced — the name is the job's identity.
	Name   string                `json:"name"`
	Config *container.Config     `json:"config"`
	Host   *container.HostConfig `json:"host,omitempty"`
}

// JobReport is what came back from the container. A non-zero ExitCode is
// an OUTCOME, not an activity failure: the job ran, the caller decides
// what its exit status means. The activity fails only when the container
// could not be run at all.
type JobReport struct {
	ExitCode int `json:"exitCode"`
	// Stdout is the container's stdout, bounded to the first 256 KiB —
	// enough for a machine-readable answer (a JSON line, a version).
	Stdout string `json:"stdout,omitempty"`
	// Tail is the end of the merged stdout+stderr, last 16 KiB: where a
	// tool prints its summary or its error.
	Tail string `json:"tail,omitempty"`
	// LogPath is the full merged output on the machine — hand it to
	// artifact.FromAgentFile to keep it.
	LogPath string `json:"logPath,omitempty"`
}

// Job runs a container to completion on the machine: pull, create, start,
// stream its output to the run's logs line by line, wait, remove. It is
// the one-shot counterpart of Container — a verb, not a resource: nothing
// is owned, nothing stays behind but the log file.
//
// Whether a second execution is acceptable is the CALLER'S knowledge: a
// job that loads data or sends a message wants
// activity.WithGuarantee(activity.AtMostOnce).
func Job(spec JobSpec) activity.Call[JobReport] {
	return activity.Fn(jobActivityName, jobActivity, spec)
}

func jobActivity(ctx context.Context, spec JobSpec) (JobReport, error) {
	if err := spec.validate(); err != nil {
		return JobReport{}, err
	}
	stop := heartbeat(ctx, "running job "+spec.Name)
	defer stop()
	cli, err := dockerclient.NewClientWithOpts(dockerclient.FromEnv, dockerclient.WithAPIVersionNegotiation())
	if err != nil {
		return JobReport{}, err
	}
	defer func() { _ = cli.Close() }()

	// The image first — create does not pull. A failed pull is not fatal
	// (a locally built image is fine), but it is REMEMBERED: when create
	// then finds no image, the pull's error is the real cause.
	pullErr := pullImage(ctx, cli, spec.Config.Image)
	// A leftover of the same name is a previous attempt that died before
	// reporting: its state is unknowable, the name is ours.
	if err := cli.ContainerRemove(ctx, spec.Name, container.RemoveOptions{Force: true}); err != nil && !cerrdefs.IsNotFound(err) {
		return JobReport{}, fmt.Errorf("job %s: remove stale container: %w", spec.Name, err)
	}
	host := spec.hostConfig()
	created, err := cli.ContainerCreate(ctx, withTelemetry(spec.Config, host), host, nil, nil, spec.Name)
	if err != nil {
		if pullErr != nil {
			return JobReport{}, fmt.Errorf("job %s: create: %w (pull %s: %w)", spec.Name, err, spec.Config.Image, pullErr)
		}
		return JobReport{}, fmt.Errorf("job %s: create: %w", spec.Name, err)
	}
	// Removed on every exit path, a cancelled activity included.
	defer func() {
		rmCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), jobRemoveTimeout)
		defer cancel()
		_ = cli.ContainerRemove(rmCtx, created.ID, container.RemoveOptions{Force: true})
	}()
	if err := cli.ContainerStart(ctx, created.ID, container.StartOptions{}); err != nil {
		return JobReport{}, fmt.Errorf("job %s: start: %w", spec.Name, err)
	}

	logPath, logFile, err := createJobLog(spec.Name)
	if err != nil {
		return JobReport{}, err
	}
	defer func() { _ = logFile.Close() }()
	logs, err := cli.ContainerLogs(ctx, created.ID, container.LogsOptions{ShowStdout: true, ShowStderr: true, Follow: true})
	if err != nil {
		return JobReport{}, fmt.Errorf("job %s: logs: %w", spec.Name, err)
	}
	defer func() { _ = logs.Close() }()
	stdout, tail, err := drainJobOutput(ctx, logs, spec.Config.Tty, logFile, spec.Name)
	if err != nil {
		return JobReport{}, fmt.Errorf("job %s: read output: %w", spec.Name, err)
	}
	report := JobReport{Stdout: stdout, Tail: tail, LogPath: logPath}

	waitCh, errCh := cli.ContainerWait(ctx, created.ID, container.WaitConditionNotRunning)
	select {
	case res := <-waitCh:
		if res.Error != nil {
			return report, fmt.Errorf("job %s: %s", spec.Name, res.Error.Message)
		}
		report.ExitCode = int(res.StatusCode)
		if report.ExitCode != 0 {
			// An outcome, not an activity failure — but in the run's logs
			// it must not look like every other line: the tool's own
			// output carries no severity, this record does.
			obs.Warn(ctx, fmt.Sprintf("job %s exited with status %d", spec.Name, report.ExitCode),
				obs.Str("job", spec.Name), obs.Int("exitCode", report.ExitCode))
		}
		return report, nil
	case err := <-errCh:
		return report, fmt.Errorf("job %s: wait: %w", spec.Name, err)
	case <-ctx.Done():
		return report, ctx.Err()
	}
}

// pullImage pulls to completion; the daemon reports a failed pull in the
// stream's error, so draining it is part of the pull.
func pullImage(ctx context.Context, cli *dockerclient.Client, ref string) error {
	pull, err := cli.ImagePull(ctx, ref, image.PullOptions{})
	if err != nil {
		return err
	}
	defer func() { _ = pull.Close() }()
	_, err = io.Copy(io.Discard, pull)
	return err
}

func (s JobSpec) validate() error {
	switch {
	case s.Name == "":
		return errors.New("job: name is required")
	case s.Config == nil || s.Config.Image == "":
		return fmt.Errorf("job %s: config with an image is required", s.Name)
	}
	return nil
}

// hostConfig is the caller's HostConfig with AutoRemove cleared: the job
// removes its container itself, AFTER reading the exit status — docker's
// own auto-removal races that read.
func (s JobSpec) hostConfig() *container.HostConfig {
	if s.Host == nil {
		return &container.HostConfig{}
	}
	host := *s.Host
	host.AutoRemove = false
	return &host
}

// createJobLog opens <workspace>/jobs/<name>.log: under the run's
// workspace the path is the same here and on the machine, so the report
// can name it for an artifact. Outside a hosted container (the exec
// runtime, tests) the default temp dir is the machine's own.
func createJobLog(name string) (string, *os.File, error) {
	base := machine.Workspace()
	if base == "" {
		base = os.TempDir()
	}
	dir := filepath.Join(base, "jobs")
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return "", nil, err
	}
	path := filepath.Join(dir, name+".log")
	f, err := os.Create(path) //nolint:gosec // the path is built from the run's own workspace
	if err != nil {
		return "", nil, err
	}
	return path, f, nil
}

// drainJobOutput reads the container's output to its end: every line goes
// to the log file and to obs; stdout is also kept (bounded) for the
// report, the end of the merged stream as the tail. Docker multiplexes
// stdout and stderr into one stream unless the container has a TTY.
//
// Each stream assembles its OWN lines: docker's frames interleave at any
// byte, and one shared buffer would splice the unfinished line of one
// stream into the other's. The sinks stay shared — the file and the tail
// are the merged output as docker delivered it.
func drainJobOutput(ctx context.Context, src io.Reader, tty bool, logFile io.Writer, name string) (stdout, tail string, err error) {
	head := &headBuffer{limit: jobStdoutLimit}
	end := &tailBuffer{limit: jobTailLimit}
	sinks := io.MultiWriter(logFile, end)
	outLines := &jobLineWriter{ctx: ctx, name: name, stream: streamStdout, sinks: sinks}
	errLines := &jobLineWriter{ctx: ctx, name: name, stream: streamStderr, sinks: sinks}
	out := io.MultiWriter(outLines, head)
	if tty {
		_, err = io.Copy(out, src)
	} else {
		_, err = stdcopy.StdCopy(out, errLines, src)
	}
	outLines.flush()
	errLines.flush()
	return head.String(), end.String(), err
}

// The stream a line came from rides on its log record. It is NOT a
// severity: half the tools in the world log their ordinary progress to
// stderr, and pytest prints its failures to stdout.
const (
	streamStdout = "stdout"
	streamStderr = "stderr"
)

// jobLineWriter passes bytes through to its sinks and emits every
// complete line of ONE stream as a log record of the run.
type jobLineWriter struct {
	ctx     context.Context //nolint:containedctx // a writer has no call to carry it
	name    string
	stream  string
	sinks   io.Writer
	partial []byte
	// record replaces the obs emission in tests.
	record func(stream, line string)
}

func (w *jobLineWriter) Write(p []byte) (int, error) {
	if _, err := w.sinks.Write(p); err != nil {
		return 0, err
	}
	n := len(p)
	for len(p) > 0 {
		i := bytes.IndexByte(p, '\n')
		if i < 0 {
			w.partial = append(w.partial, p...)
			// Output need not have newlines at all (a progress bar, a
			// binary dump, a one-line document): an endless line is cut,
			// or this buffer grows with the output.
			if len(w.partial) >= jobLineLimit {
				w.flush()
			}
			break
		}
		w.partial = append(w.partial, p[:i]...)
		w.emit(w.partial)
		w.partial = w.partial[:0]
		p = p[i+1:]
	}
	return n, nil
}

func (w *jobLineWriter) flush() {
	if len(w.partial) > 0 {
		w.emit(w.partial)
		w.partial = w.partial[:0]
	}
}

func (w *jobLineWriter) emit(line []byte) {
	text := string(bytes.TrimRight(line, "\r"))
	if w.record != nil {
		w.record(w.stream, text)
		return
	}
	obs.Info(w.ctx, text, obs.Str("job", w.name), obs.Str("stream", w.stream))
}

// headBuffer keeps the first limit bytes and drops the rest.
type headBuffer struct {
	buf   bytes.Buffer
	limit int
}

func (h *headBuffer) Write(p []byte) (int, error) {
	if room := h.limit - h.buf.Len(); room > 0 {
		h.buf.Write(p[:min(len(p), room)])
	}
	return len(p), nil
}

func (h *headBuffer) String() string { return h.buf.String() }

// tailBuffer keeps the last limit bytes.
type tailBuffer struct {
	buf   []byte
	limit int
}

func (t *tailBuffer) Write(p []byte) (int, error) {
	t.buf = append(t.buf, p...)
	if over := len(t.buf) - t.limit; over > 0 {
		t.buf = t.buf[over:]
	}
	return len(p), nil
}

func (t *tailBuffer) String() string { return string(t.buf) }
