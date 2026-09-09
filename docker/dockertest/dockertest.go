// Package dockertest models Docker resource declarations and capability
// publication for pipeline tests. It never accesses a Docker daemon or shell.
package dockertest

import (
	"encoding/json"
	"fmt"

	dockerlib "github.com/graphene-ci/library/docker"
	"github.com/graphene-ci/library/docker/internal/contract"
	"github.com/graphene-ci/pipeline/pkg/pipeline"
	"github.com/graphene-ci/pipeline/pkg/pipelinetest"
	"github.com/graphene-ci/pipeline/pkg/ref"
	"go.temporal.io/sdk/workflow"
)

// Install installs deterministic container/volume/network outputs and the
// docker.install capability effect. version is explicit test data.
func Install(world *pipelinetest.World, version string) {
	world.Handle("docker.install", func(ctx workflow.Context, _ []any) (any, error) {
		agent := pipelinetest.Agent(ctx)
		installed, resolved := true, version
		for _, capability := range world.AgentState(agent).Capabilities {
			if capability.Name == "docker" && capability.Ready {
				installed, resolved = false, capability.Version
				break
			}
		}
		err := world.PublishCapability(agent, pipeline.Capability{Name: "docker", Version: resolved, Ready: true, BroughtBy: "dockerlib.Install"})
		return dockerlib.InstallReport{Version: resolved, Installed: installed}, err
	})
	pipelinetest.Handle1(world, contract.DeclareActivity, func(ctx workflow.Context, req contract.DeclareRequest) (any, error) {
		var spec contract.ContainerSpec
		if err := json.Unmarshal(req.Spec, &spec); err != nil {
			return nil, err
		}
		name := ref.OwnerRef(string(req.Kind) + "/" + req.Name)
		if req.Kind == dockerlib.ContainerKind && (spec.Config == nil || spec.Config.Image == "") {
			return nil, fmt.Errorf("container %s requires an image", req.Name)
		}
		if err := world.Declare(ctx, pipelinetest.Resource{Ref: name, Owner: spec.Owner, Agent: pipelinetest.Agent(ctx), Spec: req.Spec, Labels: req.Labels, Flows: spec.Flows}); err != nil {
			return nil, err
		}
		var output any
		switch req.Kind {
		case dockerlib.ContainerKind:
			output = dockerlib.Info{Id: "test-" + req.Name}
		case dockerlib.VolumeKind:
			output = dockerlib.VolumeInfo{Name: req.Name, Mountpoint: "/test/volumes/" + req.Name}
		case dockerlib.NetworkKind:
			output = dockerlib.NetworkInfo{Id: "test-" + req.Name, Name: req.Name}
		default:
			return nil, fmt.Errorf("unsupported docker kind %s", req.Kind)
		}
		if err := world.Ready(ctx, name, output); err != nil {
			return nil, err
		}
		return output, nil
	})
}
