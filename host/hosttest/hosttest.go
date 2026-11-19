// Package hosttest models host resource declarations for pipeline tests.
// It never touches a machine: a declared setting becomes a record under
// its owner and turns ready, so a pipeline's tree, cascade and leaks are
// asserted without root or systemd.
package hosttest

import (
	"encoding/json"
	"fmt"
	"path"

	"go.temporal.io/sdk/workflow"

	hostlib "github.com/graphene-ci/library/host"
	"github.com/graphene-ci/library/host/internal/contract"
	"github.com/graphene-ci/pipeline/pkg/pipelinetest"
	"github.com/graphene-ci/pipeline/pkg/ref"
)

// Install makes host declarations converge in the simulator.
func Install(world *pipelinetest.World) {
	// AttachDir finds a directory record the world already holds.
	pipelinetest.Handle1(world, "host.dir.attach", func(_ workflow.Context, name string) (hostlib.DirInfo, error) {
		var info hostlib.DirInfo
		r, ok := world.Resource(ref.OwnerRef(contract.DirKind + "/" + name))
		if !ok || r.Phase != "ready" {
			return info, fmt.Errorf("dir %s does not exist or is not ready", name)
		}
		return info, json.Unmarshal(r.State, &info)
	})
	pipelinetest.Handle1(world, contract.DeclareActivity, func(ctx workflow.Context, req contract.DeclareRequest) (any, error) {
		tree, info, err := simulate(req)
		if err != nil {
			return nil, err
		}
		name := ref.OwnerRef(req.Kind + "/" + req.Name)
		agent := pipelinetest.Agent(ctx)
		if err := world.Declare(ctx, pipelinetest.Resource{Ref: name, Owner: tree.Owner, Agent: agent, Spec: req.Spec, Labels: req.Labels, Flows: tree.Flows}); err != nil {
			return nil, err
		}
		if err := world.Ready(ctx, name, info); err != nil {
			return nil, err
		}
		return info, nil
	})
}

// simulate decodes the spec of req's kind into its tree and the info a
// real machine would report (no prior values: the simulated machine has
// none).
func simulate(req contract.DeclareRequest) (contract.Tree, any, error) {
	switch req.Kind {
	case contract.SysctlKind:
		var s contract.SysctlSpec
		err := json.Unmarshal(req.Spec, &s)
		return s.Tree, hostlib.SysctlInfo{Prior: map[string]string{}}, err
	case contract.SysfsKind:
		var s contract.SysfsSpec
		err := json.Unmarshal(req.Spec, &s)
		return s.Tree, hostlib.SysfsInfo{Path: s.Path}, err
	case contract.DirKind:
		var s contract.DirSpec
		err := json.Unmarshal(req.Spec, &s)
		return s.Tree, hostlib.DirInfo{Path: s.Path}, err
	case contract.UnitKind:
		var s contract.UnitSpec
		err := json.Unmarshal(req.Spec, &s)
		return s.Tree, hostlib.UnitInfo{Name: s.Name, Path: path.Join("/etc/systemd/system", s.Name)}, err
	case contract.UnitStateKind:
		var s contract.UnitStateSpec
		err := json.Unmarshal(req.Spec, &s)
		return s.Tree, hostlib.UnitStateInfo{Name: s.Name}, err
	case contract.MountKind:
		var s contract.MountSpec
		err := json.Unmarshal(req.Spec, &s)
		return s.Tree, hostlib.MountInfo{Device: s.Device, Mountpoint: s.Mountpoint, FSType: s.FSType}, err
	default:
		return contract.Tree{}, nil, fmt.Errorf("unknown host kind %q", req.Kind)
	}
}
