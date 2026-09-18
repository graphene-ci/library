// Package filetest models File resource declarations for pipeline tests. It
// never touches a filesystem: a declared file lands in the simulated
// machine's files, where artifact.FromAgentFile and assertions find it.
package filetest

import (
	"encoding/json"
	"fmt"

	filelib "github.com/graphene-ci/library/file"
	"github.com/graphene-ci/library/file/internal/contract"
	"github.com/graphene-ci/pipeline/pkg/pipelinetest"
	"github.com/graphene-ci/pipeline/pkg/ref"
	"go.temporal.io/sdk/workflow"
)

// Install makes File declarations converge in the simulator: the record is
// declared under its owner and becomes ready with its path. Inline content
// (FromBytes / FromEmbed) is written into the agent's simulated files;
// secret and artifact sources declare the record without bytes — their
// values resolve on a real machine only.
func Install(world *pipelinetest.World) {
	pipelinetest.Handle1(world, contract.DeclareActivity, func(ctx workflow.Context, req contract.DeclareRequest) (any, error) {
		var spec contract.FileSpec
		if err := json.Unmarshal(req.Spec, &spec); err != nil {
			return nil, err
		}
		if spec.Path == "" {
			return nil, fmt.Errorf("file %s requires a path", req.Name)
		}
		agent := pipelinetest.Agent(ctx)
		name := ref.OwnerRef(string(filelib.FileKind) + "/" + req.Name)
		if err := world.Declare(ctx, pipelinetest.Resource{Ref: name, Owner: spec.Owner, Agent: agent, Spec: req.Spec, Labels: req.Labels, Flows: spec.Flows}); err != nil {
			return nil, err
		}
		if spec.Content != nil {
			world.File(agent, spec.Path, spec.Content)
		}
		info := filelib.Info{Path: spec.Path}
		if err := world.Ready(ctx, name, info); err != nil {
			return nil, err
		}
		return info, nil
	})
}
