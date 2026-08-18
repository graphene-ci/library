// Package dockerlib is the activity-and-resource library over docker on
// a machine: install the engine, run containers. All bodies execute
// inside the per-(agent × run) container on the machine — with the
// machine's docker socket under their feet; the run workflow only
// orchestrates.
package dockerlib

import (
	"context"
	"fmt"
	"os/exec"
	"strings"
	"time"

	"github.com/docker/docker/api/types/container"
	dockerclient "github.com/docker/docker/client"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"github.com/graphene-ci/pipeline/pkg/activity"
	"github.com/graphene-ci/pipeline/pkg/pipeline"
	"github.com/graphene-ci/pipeline/pkg/ref"
)

// InstallReport is what Install returns.
type InstallReport struct {
	Version string `json:"version"`
	// Installed is false when the engine was already there.
	Installed bool `json:"installed"`
}

// Install brings the docker engine onto the machine — converging: an
// engine that is already there is reported, not reinstalled.
func Install() activity.Call[InstallReport] {
	return activity.Fn0("docker.install", func(ctx context.Context) (InstallReport, error) {
		if version, err := dockerVersion(ctx); err == nil {
			return InstallReport{Version: version}, nil
		}
		script := exec.CommandContext(ctx, "sh", "-c", "curl -fsSL https://get.docker.com | sh")
		if out, err := script.CombinedOutput(); err != nil {
			return InstallReport{}, fmt.Errorf("install docker: %w: %s", err, tail(string(out), 2048))
		}
		version, err := dockerVersion(ctx)
		if err != nil {
			return InstallReport{}, err
		}
		return InstallReport{Version: version, Installed: true}, nil
	})
}

// Spec describes one container with docker's OWN types — structures
// their authors made, not ours.
type Spec struct {
	Name   string                `json:"name"`
	Config *container.Config     `json:"config"`
	Host   *container.HostConfig `json:"host,omitempty"`
}

// Info is the output of a running container resource.
type Info struct {
	Id string `json:"id"`
}

// Activity names — the library's wire identities.
const (
	runActivityName    = "docker.container.run"
	removeActivityName = "docker.container.remove"
)

// Container declares a container on the agent's machine as an ORDINARY
// resource: parented to the agent (it dies with it), visible in CLI/UI —
// only this code has it hidden behind sugar.
//
// TODO(tree): the durable record lands with the server-side tree
// support; the parent link is carried, not yet enforced server-side.
func Container(ctx pipeline.Context, agent pipeline.AgentHandle, spec Spec, opts ...pipeline.ResourceOption) pipeline.Resource[Info] {
	self := ref.OwnerRef("docker/" + spec.Name)
	if ctx.Recording() {
		ctx.RecordActivity(runActivityName, runActivity)
		ctx.RecordActivity(removeActivityName, removeActivity)
		return pipeline.NewResource[Info](ctx, self, nil)
	}
	opts = append([]pipeline.ResourceOption{pipeline.Parent(agent)}, opts...)
	o := pipeline.BuildResourceOptions(ctx, opts)
	_ = o // carried into the record when the server tree lands
	fut := pipeline.DispatchOnAgent(ctx, agent.AgentId(), workflow.ActivityOptions{
		StartToCloseTimeout: 10 * time.Minute,
		HeartbeatTimeout:    time.Minute,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    time.Second,
			BackoffCoefficient: 2,
			MaximumInterval:    time.Minute,
		},
	}, runActivityName, spec)
	return pipeline.NewResource[Info](ctx, self, fut)
}

// runActivity converges one container: absent — created and started,
// stopped — started, running — reported. Idempotent by name.
func runActivity(ctx context.Context, spec Spec) (Info, error) {
	cli, err := dockerclient.NewClientWithOpts(dockerclient.FromEnv, dockerclient.WithAPIVersionNegotiation())
	if err != nil {
		return Info{}, err
	}
	defer func() { _ = cli.Close() }()

	if existing, err := cli.ContainerInspect(ctx, spec.Name); err == nil {
		if !existing.State.Running {
			if err := cli.ContainerStart(ctx, existing.ID, container.StartOptions{}); err != nil {
				return Info{}, err
			}
		}
		return Info{Id: existing.ID}, nil
	}
	created, err := cli.ContainerCreate(ctx, spec.Config, spec.Host, nil, nil, spec.Name)
	if err != nil {
		return Info{}, err
	}
	if err := cli.ContainerStart(ctx, created.ID, container.StartOptions{}); err != nil {
		return Info{}, err
	}
	return Info{Id: created.ID}, nil
}

// removeActivity tears one container down; absence is success.
func removeActivity(ctx context.Context, spec Spec) error {
	cli, err := dockerclient.NewClientWithOpts(dockerclient.FromEnv, dockerclient.WithAPIVersionNegotiation())
	if err != nil {
		return err
	}
	defer func() { _ = cli.Close() }()
	err = cli.ContainerRemove(ctx, spec.Name, container.RemoveOptions{Force: true})
	if err != nil && !dockerclient.IsErrNotFound(err) {
		return err
	}
	return nil
}

func dockerVersion(ctx context.Context) (string, error) {
	out, err := exec.CommandContext(ctx, "docker", "version", "--format", "{{.Server.Version}}").Output()
	if err != nil {
		return "", fmt.Errorf("docker not available: %w", err)
	}
	return strings.TrimSpace(string(out)), nil
}

func tail(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return "…" + s[len(s)-n:]
}
