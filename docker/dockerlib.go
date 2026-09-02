// Package dockerlib is the activity-and-resource library over docker on
// a machine: install the engine, run containers. All bodies execute
// inside the per-(agent × run) container on the machine — with the
// machine's docker socket under their feet; the run workflow only
// orchestrates.
package dockerlib

import (
	"context"
	"encoding/json"
	"fmt"
	"io"

	cerrdefs "github.com/containerd/errdefs"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/image"
	dockerclient "github.com/docker/docker/client"

	"github.com/graphene-ci/pipeline/pkg/activity"
	"github.com/graphene-ci/pipeline/pkg/capabilityapi"
	"github.com/graphene-ci/pipeline/pkg/machine"
	"github.com/graphene-ci/pipeline/pkg/obs"
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
// engine that is already there is reported, not reinstalled. A library
// verb is a Call: run it with pipelineactivity.Activity on one agent or
// pipelineactivity.ActivityAll on a selection.
func Install() activity.Call[InstallReport] {
	return activity.Fn0("docker.install", func(ctx context.Context) (InstallReport, error) {
		if version, err := dockerVersion(ctx); err == nil {
			// Already there — still publish: the record must know.
			if err := publish(ctx, version); err != nil {
				return InstallReport{}, err
			}
			return InstallReport{Version: version}, nil
		}
		// The DISTRIBUTION decides the package manager — read from the
		// machine's own /etc/os-release, never guessed. get.docker.com
		// goes first (it speaks most deb/rpm families and geo-blocks
		// are the reason for the fallback); the case below covers what
		// it does not: ALT, Arch, Alpine, SUSE. Package hooks must not
		// start services inside the chroot (policy-rc.d 101) — the
		// daemon is brought up through the host's systemd afterwards.
		script := machine.Shell(ctx,
			"printf '#!/bin/sh\nexit 101\n' > /usr/sbin/policy-rc.d && chmod +x /usr/sbin/policy-rc.d; "+
				"trap 'rm -f /usr/sbin/policy-rc.d' EXIT; "+
				"export DEBIAN_FRONTEND=noninteractive; "+
				". /etc/os-release 2>/dev/null || ID=unknown; "+
				"family=\"$ID $ID_LIKE\"; "+
				"if curl -fsSL -m 30 https://get.docker.com -o /tmp/get-docker.sh 2>/dev/null && sh /tmp/get-docker.sh; then :; else "+
				"case \"$family\" in "+
				"*altlinux*) apt-get update -qq && apt-get install -y -qq docker-engine ;; "+
				"*debian*|*ubuntu*) apt-get update -qq && apt-get install -y -qq docker.io ;; "+
				"*fedora*) dnf install -y -q moby-engine ;; "+
				"*rhel*|*centos*) (command -v dnf >/dev/null && dnf install -y -q docker) || yum install -y -q docker ;; "+
				"*suse*) zypper --non-interactive install docker ;; "+
				"*arch*) pacman -Sy --noconfirm docker ;; "+
				"*alpine*) apk add --no-cache docker ;; "+
				"*) echo \"no docker recipe for distribution: $family\" >&2; exit 1 ;; "+
				"esac; fi")
		if out, err := obs.RunTail(ctx, script, errTailBytes); err != nil {
			return InstallReport{}, fmt.Errorf("install docker: %w: %s", err, out)
		}
		// The chrooted installer cannot start the daemon itself; the
		// host's systemd is reachable through the machine root.
		start := machine.Shell(ctx, "systemctl enable --now docker 2>/dev/null || rc-update add docker boot 2>/dev/null && rc-service docker start 2>/dev/null || service docker start 2>/dev/null || true")
		if out, err := obs.RunTail(ctx, start, errTailBytes); err != nil {
			return InstallReport{}, fmt.Errorf("start docker: %w: %s", err, out)
		}
		version, err := dockerVersion(ctx)
		if err != nil {
			return InstallReport{}, err
		}
		if err := publish(ctx, version); err != nil {
			return InstallReport{}, err
		}
		return InstallReport{Version: version, Installed: true}, nil
	})
}

// publish records what this body just made true, right where it
// happened: capability "docker" on the machine this container runs on.
func publish(ctx context.Context, version string) error {
	return capabilityapi.PublishSelf(ctx, pipeline.Capability{
		Name:      "docker",
		Version:   version,
		BroughtBy: "dockerlib.Install",
		Ready:     true,
	})
}

// Spec describes one container with docker's OWN types — structures
// their authors made, not ours.
type Spec struct {
	Name   string                `json:"name"`
	Config *container.Config     `json:"config"`
	Host   *container.HostConfig `json:"host,omitempty"`
	// Scrape is a prometheus metrics endpoint the container exposes
	// (e.g. "http://localhost:9187/metrics"); the observation beat pulls
	// it and ships the samples as this container's own metrics (Р-Н27).
	Scrape string `json:"scrape,omitempty"`
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
// resource: an entity record in the tree — owned, cascaded,
// transferable to a stand. Its lifecycle runs on this agent's own
// executor, which lives while the record does.
func Container(ctx pipeline.Context, agent pipeline.Agent, spec Spec, opts ...pipeline.ResourceOption) pipeline.Resource[Info] {
	self := ref.OwnerRef(string(ContainerKind) + "/" + spec.Name)
	if ctx.Recording() {
		recordEntities(ctx)
		ctx.RecordDeclare(self, pipeline.BuildResourceOptions(ctx, planOpts(agent, opts)))
		return pipeline.NewResource[Info](ctx, self, nil)
	}
	// Parent only when the agent is OURS: a handle in the tree. A
	// foreign agent has no ResourceRef — the container stays owned by
	// the run, what is not yours cannot be burdened.
	if h, ok := agent.(pipeline.Handle); ok {
		opts = append([]pipeline.ResourceOption{pipeline.Parent(h)}, opts...)
	}
	o := pipeline.BuildResourceOptions(ctx, opts)
	raw, _ := json.Marshal(containerSpec{Name: spec.Name, Config: spec.Config, Host: spec.Host, Owner: o.Parent, Flows: o.Flows, Scrape: spec.Scrape})
	fut := pipeline.DispatchOnAgent(ctx, agent.AgentId(), dockerActivityOptions(), declareActivityName, declareRequest{
		Kind: ContainerKind, Name: spec.Name, Labels: o.Labels, RunId: string(ctx.RunId()), Spec: raw,
	})
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
	// The image first — create does not pull.
	if pull, err := cli.ImagePull(ctx, spec.Config.Image, image.PullOptions{}); err == nil {
		_, _ = io.Copy(io.Discard, pull)
		_ = pull.Close()
	} // a locally built image is fine — pull is best-effort
	created, err := cli.ContainerCreate(ctx, spec.Config, spec.Host, nil, nil, spec.Name)
	if err != nil {
		return Info{}, err
	}
	if err := cli.ContainerStart(ctx, created.ID, container.StartOptions{}); err != nil {
		return Info{}, err
	}
	return Info{Id: created.ID}, nil
}

// removeActivity tears one container down BY ID (the finalizer sends
// the created container's id, not the spec); absence is success.
func removeActivity(ctx context.Context, containerId string) error {
	cli, err := dockerclient.NewClientWithOpts(dockerclient.FromEnv, dockerclient.WithAPIVersionNegotiation())
	if err != nil {
		return err
	}
	defer func() { _ = cli.Close() }()
	err = cli.ContainerRemove(ctx, containerId, container.RemoveOptions{Force: true})
	if err != nil && !cerrdefs.IsNotFound(err) {
		return err
	}
	return nil
}

// dockerVersion asks the daemon over the API — no CLI involved, works
// in both runtimes (the agent points DOCKER_HOST at the machine's
// socket).
func dockerVersion(ctx context.Context) (string, error) {
	cli, err := dockerclient.NewClientWithOpts(dockerclient.FromEnv, dockerclient.WithAPIVersionNegotiation())
	if err != nil {
		return "", err
	}
	defer func() { _ = cli.Close() }()
	v, err := cli.ServerVersion(ctx)
	if err != nil {
		return "", fmt.Errorf("docker not available: %w", err)
	}
	return v.Version, nil
}
