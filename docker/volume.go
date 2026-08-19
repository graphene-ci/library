package dockerlib

import (
	"context"
	"time"

	cerrdefs "github.com/containerd/errdefs"
	"github.com/docker/docker/api/types/volume"
	dockerclient "github.com/docker/docker/client"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"github.com/graphene-ci/pipeline/pkg/pipeline"
	"github.com/graphene-ci/pipeline/pkg/ref"
)

// VolumeInfo is the output of a volume resource.
type VolumeInfo struct {
	Name       string `json:"name"`
	Mountpoint string `json:"mountpoint,omitempty"`
}

// Volume activity names.
const (
	volumeEnsureActivityName = "docker.volume.ensure"
	volumeRemoveActivityName = "docker.volume.remove"
)

// Volume declares a docker volume on the agent's machine as an
// ordinary resource — it dies with its owner (data of a stand without
// the stand is garbage). The spec is docker's OWN type.
func Volume(ctx pipeline.Context, agent pipeline.Agent, spec volume.CreateOptions, opts ...pipeline.ResourceOption) pipeline.Resource[VolumeInfo] {
	self := ref.OwnerRef("docker-volume/" + spec.Name)
	if ctx.Recording() {
		ctx.RecordActivity(volumeEnsureActivityName, volumeEnsureActivity)
		ctx.RecordActivity(volumeRemoveActivityName, volumeRemoveActivity)
		return pipeline.NewResource[VolumeInfo](ctx, self, nil)
	}
	if h, ok := agent.(pipeline.Handle); ok {
		opts = append([]pipeline.ResourceOption{pipeline.Parent(h)}, opts...)
	}
	o := pipeline.BuildResourceOptions(ctx, opts)
	_ = o // carried into the record when the server tree lands
	fut := pipeline.DispatchOnAgent(ctx, agent.AgentId(), dockerActivityOptions(), volumeEnsureActivityName, spec)
	return pipeline.NewResource[VolumeInfo](ctx, self, fut)
}

// volumeEnsureActivity converges one volume: docker's create is
// idempotent by name.
func volumeEnsureActivity(ctx context.Context, spec volume.CreateOptions) (VolumeInfo, error) {
	cli, err := dockerclient.NewClientWithOpts(dockerclient.FromEnv, dockerclient.WithAPIVersionNegotiation())
	if err != nil {
		return VolumeInfo{}, err
	}
	defer func() { _ = cli.Close() }()
	v, err := cli.VolumeCreate(ctx, spec)
	if err != nil {
		return VolumeInfo{}, err
	}
	return VolumeInfo{Name: v.Name, Mountpoint: v.Mountpoint}, nil
}

func volumeRemoveActivity(ctx context.Context, name string) error {
	cli, err := dockerclient.NewClientWithOpts(dockerclient.FromEnv, dockerclient.WithAPIVersionNegotiation())
	if err != nil {
		return err
	}
	defer func() { _ = cli.Close() }()
	if err := cli.VolumeRemove(ctx, name, false); err != nil && !cerrdefs.IsNotFound(err) {
		return err
	}
	return nil
}

// dockerActivityOptions is the shared dispatch shape of the docker
// resource activities.
func dockerActivityOptions() workflow.ActivityOptions {
	return workflow.ActivityOptions{
		StartToCloseTimeout: 10 * time.Minute,
		HeartbeatTimeout:    time.Minute,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    time.Second,
			BackoffCoefficient: 2,
			MaximumInterval:    time.Minute,
		},
	}
}
