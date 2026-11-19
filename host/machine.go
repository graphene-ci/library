package host

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"net/url"
	"os"
	"path"
	"strings"

	temporalactivity "go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"

	"github.com/graphene-ci/pipeline/pkg/machine"
)

// Runner executes a program on the machine and returns its stdout. A
// non-zero exit is an error, but the stdout read so far still comes back:
// `systemctl is-enabled` reports a disabled unit through both.
type Runner interface {
	Run(ctx context.Context, name string, args ...string) (string, error)
}

// Machine is the machine an activity changes: its filesystem under root
// and its programs through the runner. Every host effect goes through it,
// so tests point it at a temporary directory and a scripted runner.
type Machine struct {
	root   string
	runner Runner
}

// NewMachine addresses the machine whose filesystem is at root ("" — the
// process runs on the machine itself).
func NewMachine(root string, runner Runner) Machine {
	return Machine{root: root, runner: runner}
}

// fsys is the machine's filesystem for one activity. Every path stays
// beneath the machine root, symlinks included: a path a workload planted
// cannot make a record write or delete outside the machine.
type fsys struct {
	r *os.Root
}

func (m Machine) open() (fsys, error) {
	dir := m.root
	if dir == "" {
		dir = "/"
	}
	r, err := os.OpenRoot(dir)
	if err != nil {
		return fsys{}, err
	}
	return fsys{r: r}, nil
}

func (f fsys) Close() { _ = f.r.Close() }

// rel turns a machine path into a name relative to the root.
func rel(p string) string {
	p = strings.TrimPrefix(path.Clean("/"+p), "/")
	if p == "" {
		return "."
	}
	return p
}

func (f fsys) readTrimmed(p string) (string, error) {
	b, err := f.r.ReadFile(rel(p))
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(b)), nil
}

// writeValue writes a kernel attribute (/proc/sys, /sys): the file exists,
// the write replaces its value.
func (f fsys) writeValue(p, value string) error {
	file, err := f.r.OpenFile(rel(p), os.O_WRONLY|os.O_TRUNC, 0)
	if err != nil {
		return err
	}
	if _, err := file.WriteString(value); err != nil {
		_ = file.Close()
		return fmt.Errorf("write %s: %w", p, err)
	}
	return file.Close()
}

// execRunner runs programs on the machine through pipeline/pkg/machine.
type execRunner struct{}

func (execRunner) Run(ctx context.Context, name string, args ...string) (string, error) {
	cmd := machine.Command(ctx, name, args...)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return stdout.String(), fmt.Errorf("%s %s: %w: %s", name, strings.Join(args, " "), err, strings.TrimSpace(stderr.String()))
	}
	return stdout.String(), nil
}

// A claim marks a host setting as held by one record. It lives on the
// machine, next to what it guards, so two records cannot fight over a key
// even across runs and pipelines, and the value to restore survives an
// interrupted Init that never reported back to its workflow.
const claimsDir = "/var/lib/graphene-host/claims"

// ConflictCode is the stable error type of a refusal: the setting is held
// by another record, or the target exists and is not the record's own.
const ConflictCode = "host_conflict"

type claim struct {
	Record string `json:"record"`
	// Prior is what the setting was before the record took it.
	Prior string `json:"prior,omitempty"`
}

func claimPath(kind, key string) string {
	return path.Join(claimsDir, kind, url.QueryEscape(key))
}

func conflict(kind, key, holder string) error {
	return temporal.NewNonRetryableApplicationError(
		fmt.Sprintf("%s %s is held by %s", kind, key, holder), ConflictCode, nil)
}

// take claims key for record. A claim the record already holds is
// returned unchanged — its prior was read before the record changed the
// setting, re-reading now would capture the record's own value.
func (f fsys) take(kind, key, record string, readPrior func() (string, error)) (claim, error) {
	p := claimPath(kind, key)
	if c, err := f.readClaim(p); err == nil {
		if c.Record != record {
			return claim{}, conflict(kind, key, c.Record)
		}
		return c, nil
	} else if !errors.Is(err, fs.ErrNotExist) {
		return claim{}, err
	}
	prior, err := readPrior()
	if err != nil {
		return claim{}, err
	}
	c := claim{Record: record, Prior: prior}
	if err := f.r.MkdirAll(rel(path.Dir(p)), 0o700); err != nil {
		return claim{}, err
	}
	b, err := json.Marshal(c)
	if err != nil {
		return claim{}, err
	}
	// O_EXCL: of two records racing for the key exactly one wins.
	file, err := f.r.OpenFile(rel(p), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if errors.Is(err, fs.ErrExist) {
		return f.take(kind, key, record, readPrior)
	}
	if err != nil {
		return claim{}, err
	}
	if _, err := file.Write(b); err != nil {
		_ = file.Close()
		return claim{}, err
	}
	return c, file.Close()
}

// held reports the record's claim on key, if it has one.
func (f fsys) held(kind, key, record string) (claim, bool, error) {
	c, err := f.readClaim(claimPath(kind, key))
	if errors.Is(err, fs.ErrNotExist) {
		return claim{}, false, nil
	}
	if err != nil {
		return claim{}, false, err
	}
	return c, c.Record == record, nil
}

// drop removes the record's claim on key; another record's claim stays.
func (f fsys) drop(kind, key, record string) error {
	_, ours, err := f.held(kind, key, record)
	if err != nil || !ours {
		return err
	}
	if err := f.r.Remove(rel(claimPath(kind, key))); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return err
	}
	return nil
}

func (f fsys) readClaim(p string) (claim, error) {
	b, err := f.r.ReadFile(rel(p))
	if err != nil {
		return claim{}, err
	}
	var c claim
	if err := json.Unmarshal(b, &c); err != nil {
		return claim{}, fmt.Errorf("claim %s: %w", p, err)
	}
	return c, nil
}

// heartbeat reports progress when running as an activity.
func heartbeat(ctx context.Context, details string) {
	if temporalactivity.IsActivity(ctx) {
		temporalactivity.RecordHeartbeat(ctx, details)
	}
}
