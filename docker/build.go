package dockerlib

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"time"

	temporalactivity "go.temporal.io/sdk/activity"

	"github.com/graphene-ci/pipeline/pkg/activity"
	"github.com/graphene-ci/pipeline/pkg/machine"
	"github.com/graphene-ci/pipeline/pkg/obs"
)

// BuildSpec describes one image build. Paths (Context, File) must be
// machine-valid — put the context under machine.Workspace() and this
// holds by construction (same absolute path in the container, on the
// machine, and for the daemon).
type BuildSpec struct {
	// Context is the build context directory.
	Context string `json:"context"`
	// File is the Dockerfile path; empty means <Context>/Dockerfile.
	File string `json:"file,omitempty"`
	// Tags to apply ("registry/name:tag").
	Tags []string `json:"tags,omitempty"`
	// Push pushes the tags after a successful build.
	Push bool `json:"push,omitempty"`
	// Target builds a specific stage.
	Target string `json:"target,omitempty"`
	// BuildArgs are --build-arg pairs.
	BuildArgs map[string]string `json:"buildArgs,omitempty"`
	// CacheFrom / CacheTo are buildx cache specs
	// ("type=registry,ref=...").
	CacheFrom []string `json:"cacheFrom,omitempty"`
	CacheTo   []string `json:"cacheTo,omitempty"`
}

// BuildReport is the build's outcome: the immutable identity of what
// was built. The local image stays on the machine as cache — it is not
// a resource and owns nothing.
type BuildReport struct {
	Digest string   `json:"digest,omitempty"`
	Tags   []string `json:"tags,omitempty"`
}

// Build assembles an image with the machine's own buildx — full
// BuildKit (cache export, multi-stage, secrets) with nothing wrapped:
// the CLI the machine already has (Install brings it) does the work,
// the digest comes back through --metadata-file, never log parsing.
func Build(spec BuildSpec) activity.Call[BuildReport] {
	return activity.Fn("docker.build", buildActivity, spec)
}

func buildActivity(ctx context.Context, spec BuildSpec) (BuildReport, error) {
	if spec.Context == "" {
		return BuildReport{}, errors.New("build: context directory is required")
	}
	bin, err := dockerBinary()
	if err != nil {
		return BuildReport{}, err
	}
	// The metadata file must be readable HERE and writable by the
	// chrooted CLI: the workspace is same-path on both sides; plain
	// TempDir is machine-valid in the exec runtime.
	metaDir, err := os.MkdirTemp(tempBase(), "graphene-build-")
	if err != nil {
		return BuildReport{}, err
	}
	defer func() { _ = os.RemoveAll(metaDir) }()
	metaFile := filepath.Join(metaDir, "metadata.json")

	args := []string{"buildx", "build", "--metadata-file", metaFile, "--progress", "plain"}
	if spec.File != "" {
		args = append(args, "-f", spec.File)
	}
	for _, tag := range spec.Tags {
		args = append(args, "-t", tag)
	}
	keys := make([]string, 0, len(spec.BuildArgs))
	for k := range spec.BuildArgs {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		args = append(args, "--build-arg", k+"="+spec.BuildArgs[k])
	}
	if spec.Target != "" {
		args = append(args, "--target", spec.Target)
	}
	for _, c := range spec.CacheFrom {
		args = append(args, "--cache-from", c)
	}
	for _, c := range spec.CacheTo {
		args = append(args, "--cache-to", c)
	}
	if spec.Push {
		args = append(args, "--push")
	}
	args = append(args, spec.Context)

	cmd := machine.Command(ctx, bin, args...)
	stop := heartbeat(ctx, "building "+spec.Context)
	out, err := obs.RunTail(ctx, cmd, 4096)
	stop()
	if err != nil {
		return BuildReport{}, fmt.Errorf("docker build: %w: %s", err, out)
	}
	report := BuildReport{Tags: spec.Tags}
	raw, err := os.ReadFile(metaFile) //nolint:gosec // our own temp file
	if err != nil {
		return report, fmt.Errorf("build succeeded but metadata is unreadable: %w", err)
	}
	var meta map[string]json.RawMessage
	if err := json.Unmarshal(raw, &meta); err != nil {
		return report, fmt.Errorf("build metadata: %w", err)
	}
	for _, key := range []string{"containerimage.digest", "containerimage.config.digest"} {
		var digest string
		if json.Unmarshal(meta[key], &digest) == nil && digest != "" {
			report.Digest = digest
			break
		}
	}
	return report, nil
}

// dockerBinary locates the machine's docker CLI. On the machine itself
// normal PATH lookup applies; through the chroot the path must be
// absolute and is probed against the machine's filesystem.
func dockerBinary() (string, error) {
	if machine.Root() == "" {
		return "docker", nil
	}
	for _, p := range []string{"/usr/bin/docker", "/usr/local/bin/docker", "/bin/docker"} {
		if _, err := os.Stat(machine.Path(p)); err == nil {
			return p, nil
		}
	}
	return "", errors.New("docker CLI not found on the machine (dockerlib.Install brings it)")
}

// tempBase is a directory valid on both sides of the container
// boundary: the workspace when hosted, the default temp dir otherwise.
func tempBase() string {
	return machine.Workspace()
}

// heartbeat keeps the activity alive while a long command runs.
func heartbeat(ctx context.Context, message string) func() {
	if !temporalactivity.IsActivity(ctx) {
		return func() {}
	}
	done := make(chan struct{})
	go func() {
		ticker := time.NewTicker(15 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-done:
				return
			case <-ticker.C:
				temporalactivity.RecordHeartbeat(ctx, message)
			}
		}
	}()
	return func() { close(done) }
}
