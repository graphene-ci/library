package dockerlib

import (
	"context"
	"encoding/json"

	cerrdefs "github.com/containerd/errdefs"
	"github.com/docker/docker/api/types/network"
	dockerclient "github.com/docker/docker/client"

	"github.com/graphene-ci/pipeline/pkg/pipeline"
	"github.com/graphene-ci/pipeline/pkg/ref"
)

// NetworkInfo is the output of a network resource.
type NetworkInfo struct {
	Id   string `json:"id"`
	Name string `json:"name"`
}

// NetworkSpec names a network with docker's own creation options.
type NetworkSpec struct {
	Name    string                `json:"name"`
	Options network.CreateOptions `json:"options"`
}

// Network activity names.
const (
	networkEnsureActivityName = "docker.network.ensure"
	networkRemoveActivityName = "docker.network.remove"
)

// Network declares a docker network on the agent's machine as an
// ordinary resource — an owned entity record; it dies with its owner.
func Network(ctx pipeline.Context, agent pipeline.Agent, spec NetworkSpec, opts ...pipeline.ResourceOption) pipeline.Resource[NetworkInfo] {
	self := ref.OwnerRef(string(NetworkKind) + "/" + spec.Name)
	if ctx.Recording() {
		recordEntities(ctx)
		return pipeline.NewResource[NetworkInfo](ctx, self, nil)
	}
	if h, ok := agent.(pipeline.Handle); ok {
		opts = append([]pipeline.ResourceOption{pipeline.Parent(h)}, opts...)
	}
	o := pipeline.BuildResourceOptions(ctx, opts)
	raw, _ := json.Marshal(networkSpec{Spec: spec, Owner: o.Parent})
	fut := pipeline.DispatchOnAgent(ctx, agent.AgentId(), dockerActivityOptions(), declareActivityName, declareRequest{
		Kind: NetworkKind, Name: spec.Name, Labels: o.Labels, RunId: string(ctx.RunId()), Spec: raw,
	})
	return pipeline.NewResource[NetworkInfo](ctx, self, fut)
}

// networkEnsureActivity converges one network: present — reported,
// absent — created. Idempotent by name.
func networkEnsureActivity(ctx context.Context, spec NetworkSpec) (NetworkInfo, error) {
	cli, err := dockerclient.NewClientWithOpts(dockerclient.FromEnv, dockerclient.WithAPIVersionNegotiation())
	if err != nil {
		return NetworkInfo{}, err
	}
	defer func() { _ = cli.Close() }()
	if existing, err := cli.NetworkInspect(ctx, spec.Name, network.InspectOptions{}); err == nil {
		return NetworkInfo{Id: existing.ID, Name: existing.Name}, nil
	}
	created, err := cli.NetworkCreate(ctx, spec.Name, spec.Options)
	if err != nil {
		return NetworkInfo{}, err
	}
	return NetworkInfo{Id: created.ID, Name: spec.Name}, nil
}

func networkRemoveActivity(ctx context.Context, name string) error {
	cli, err := dockerclient.NewClientWithOpts(dockerclient.FromEnv, dockerclient.WithAPIVersionNegotiation())
	if err != nil {
		return err
	}
	defer func() { _ = cli.Close() }()
	if err := cli.NetworkRemove(ctx, name); err != nil && !cerrdefs.IsNotFound(err) {
		return err
	}
	return nil
}
