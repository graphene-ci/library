// The durable half of dockerlib: containers, volumes, and networks are
// ENTITY RECORDS — owned in the tree, cascaded, transferable to a
// stand — whose lifecycle activities run on the machine's own
// (agent × run) executor. The executor lives while these records do.
package dockerlib

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/volume"
	"github.com/graphene-ci/temporal-entity/pkg/entclient"
	"github.com/graphene-ci/temporal-entity/pkg/entdefine"
	entity "github.com/graphene-ci/temporal-entity/pkg/entity"
	temporalactivity "go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"

	"github.com/graphene-ci/pipeline/pkg/flow/ownership"
	"github.com/graphene-ci/pipeline/pkg/pipeline"
	"github.com/graphene-ci/pipeline/pkg/ref"
	"github.com/graphene-ci/pipeline/pkg/wire"
)

// Entity kinds.
const (
	ContainerKind entity.KindName = "docker"
	VolumeKind    entity.KindName = "docker-volume"
	NetworkKind   entity.KindName = "docker-network"
)

// containerSpec is the container entity's desired state.
type containerSpec struct {
	Name   string                `json:"name"`
	Config *container.Config     `json:"config"`
	Host   *container.HostConfig `json:"host,omitempty"`
	Owner  ref.OwnerRef          `json:"owner,omitempty"`
}

type containerState struct {
	Info Info `json:"info"`
	ownership.State
}

type volumeSpec struct {
	Options volume.CreateOptions `json:"options"`
	Owner   ref.OwnerRef         `json:"owner,omitempty"`
}

type volumeState struct {
	Info VolumeInfo `json:"info"`
	ownership.State
}

type networkSpec struct {
	Spec  NetworkSpec  `json:"spec"`
	Owner ref.OwnerRef `json:"owner,omitempty"`
}

type networkState struct {
	Info NetworkInfo `json:"info"`
	ownership.State
}

func entityActivityCtx(ctx workflow.Context) workflow.Context {
	return workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 10 * time.Minute,
		HeartbeatTimeout:    time.Minute,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    time.Second,
			BackoffCoefficient: 2,
			MaximumInterval:    time.Minute,
		},
	})
}

func containerDef() *entdefine.Definition[containerSpec, containerState] {
	def := entdefine.New[containerSpec, containerState](ContainerKind,
		entdefine.WithSearchAttributes[containerSpec, containerState](true),
		entdefine.WithInit[containerSpec, containerState](func(ctx workflow.Context, spec containerSpec) (containerState, error) {
			var st containerState
			if spec.Owner != "" {
				ownership.Init(ctx, &st.State, spec.Owner)
			}
			err := workflow.ExecuteActivity(entityActivityCtx(ctx), runActivityName,
				Spec{Name: spec.Name, Config: spec.Config, Host: spec.Host}).Get(ctx, &st.Info)
			return st, err
		}),
		entdefine.WithFinalize[containerSpec, containerState](func(ctx workflow.Context, st *containerState) error {
			return workflow.ExecuteActivity(entityActivityCtx(ctx), removeActivityName, st.Info.Id).Get(ctx, nil)
		}),
	)
	ownership.Register(def, func(s *containerState) *ownership.State { return &s.State })
	return def
}

func volumeDef() *entdefine.Definition[volumeSpec, volumeState] {
	def := entdefine.New[volumeSpec, volumeState](VolumeKind,
		entdefine.WithSearchAttributes[volumeSpec, volumeState](true),
		entdefine.WithInit[volumeSpec, volumeState](func(ctx workflow.Context, spec volumeSpec) (volumeState, error) {
			var st volumeState
			if spec.Owner != "" {
				ownership.Init(ctx, &st.State, spec.Owner)
			}
			err := workflow.ExecuteActivity(entityActivityCtx(ctx), volumeEnsureActivityName, spec.Options).Get(ctx, &st.Info)
			return st, err
		}),
		entdefine.WithFinalize[volumeSpec, volumeState](func(ctx workflow.Context, st *volumeState) error {
			return workflow.ExecuteActivity(entityActivityCtx(ctx), volumeRemoveActivityName, st.Info.Name).Get(ctx, nil)
		}),
	)
	ownership.Register(def, func(s *volumeState) *ownership.State { return &s.State })
	return def
}

func networkDef() *entdefine.Definition[networkSpec, networkState] {
	def := entdefine.New[networkSpec, networkState](NetworkKind,
		entdefine.WithSearchAttributes[networkSpec, networkState](true),
		entdefine.WithInit[networkSpec, networkState](func(ctx workflow.Context, spec networkSpec) (networkState, error) {
			var st networkState
			if spec.Owner != "" {
				ownership.Init(ctx, &st.State, spec.Owner)
			}
			err := workflow.ExecuteActivity(entityActivityCtx(ctx), networkEnsureActivityName, spec.Spec).Get(ctx, &st.Info)
			return st, err
		}),
		entdefine.WithFinalize[networkSpec, networkState](func(ctx workflow.Context, st *networkState) error {
			return workflow.ExecuteActivity(entityActivityCtx(ctx), networkRemoveActivityName, st.Info.Name).Get(ctx, nil)
		}),
	)
	ownership.Register(def, func(s *networkState) *ownership.State { return &s.State })
	return def
}

// declareRequest asks the declare activity for one docker entity.
type declareRequest struct {
	Kind   entity.KindName   `json:"kind"`
	Name   string            `json:"name"`
	Labels map[string]string `json:"labels,omitempty"`
	RunId  string            `json:"runId,omitempty"`
	// Spec is the kind-shaped spec as JSON.
	Spec json.RawMessage `json:"spec"`
}

const declareActivityName = "docker.entity.declare"

var declared struct {
	container *entdefine.Definition[containerSpec, containerState]
	volume    *entdefine.Definition[volumeSpec, volumeState]
	network   *entdefine.Definition[networkSpec, networkState]
}

// recordEntities registers the definitions and the declare activity —
// once per process, during the recording pass.
func recordEntities(ctx pipeline.Context) {
	ctx.RecordKind(string(ContainerKind))
	ctx.RecordKind(string(VolumeKind))
	ctx.RecordKind(string(NetworkKind))
	ctx.RecordActivity(runActivityName, runActivity)
	ctx.RecordActivity(removeActivityName, removeActivity)
	ctx.RecordActivity(volumeEnsureActivityName, volumeEnsureActivity)
	ctx.RecordActivity(volumeRemoveActivityName, volumeRemoveActivity)
	ctx.RecordActivity(networkEnsureActivityName, networkEnsureActivity)
	ctx.RecordActivity(networkRemoveActivityName, networkRemoveActivity)
	ctx.RecordWorker(func(w worker.Worker, cl client.Client) error {
		if declared.container == nil {
			declared.container = containerDef()
			declared.volume = volumeDef()
			declared.network = networkDef()
		}
		if err := declared.container.Register(w); err != nil {
			return err
		}
		if err := declared.volume.Register(w); err != nil {
			return err
		}
		if err := declared.network.Register(w); err != nil {
			return err
		}
		w.RegisterActivityWithOptions(makeDeclare(cl), temporalactivity.RegisterOptions{Name: declareActivityName})
		return nil
	})
}

// makeDeclare builds the declare activity: create (or attach to) the
// entity ON THIS EXECUTOR'S OWN QUEUE and wait for readiness. The
// record then owns the executor's lifetime, not the other way around.
func makeDeclare(cl client.Client) func(context.Context, declareRequest) (json.RawMessage, error) {
	return func(ctx context.Context, req declareRequest) (json.RawMessage, error) {
		if err := wire.ValidateUserLabels(req.Labels); err != nil {
			return nil, err
		}
		queue := temporalactivity.GetInfo(ctx).TaskQueue
		labels := map[string]string{}
		for k, v := range req.Labels {
			labels[k] = v
		}
		if req.RunId != "" {
			labels["graphene.io/run"] = req.RunId
		}
		switch req.Kind {
		case ContainerKind:
			var spec containerSpec
			if err := json.Unmarshal(req.Spec, &spec); err != nil {
				return nil, err
			}
			return declareOne(ctx, entclient.Bind(declared.container, cl, queue), req.Name, spec, labels,
				func(st containerState) any { return st.Info })
		case VolumeKind:
			var spec volumeSpec
			if err := json.Unmarshal(req.Spec, &spec); err != nil {
				return nil, err
			}
			return declareOne(ctx, entclient.Bind(declared.volume, cl, queue), req.Name, spec, labels,
				func(st volumeState) any { return st.Info })
		case NetworkKind:
			var spec networkSpec
			if err := json.Unmarshal(req.Spec, &spec); err != nil {
				return nil, err
			}
			return declareOne(ctx, entclient.Bind(declared.network, cl, queue), req.Name, spec, labels,
				func(st networkState) any { return st.Info })
		default:
			return nil, fmt.Errorf("unknown docker entity kind %q", req.Kind)
		}
	}
}

// declareOne is the create-and-wait shared by the three kinds.
func declareOne[Spec, State any](ctx context.Context, cl *entclient.Client[Spec, State], name string, spec Spec, labels map[string]string, out func(State) any) (json.RawMessage, error) {
	rid := entity.ResourceID(name)
	if _, err := cl.CreateOrAttach(ctx, rid, spec, entclient.WithLabels(labels)); err != nil {
		return nil, err
	}
	for {
		desc, err := cl.Describe(ctx, rid)
		if err != nil {
			return nil, err
		}
		switch desc.Phase {
		case entity.PhaseReady:
			return json.Marshal(out(desc.State))
		case entity.PhaseCreateFailed:
			return nil, fmt.Errorf("%s failed to converge", name)
		case entity.PhaseDeleting, entity.PhaseDeleted, entity.PhaseDeleteFailed:
			return nil, fmt.Errorf("%s is going away (phase %s)", name, desc.Phase)
		case entity.PhaseCreating:
			// keep polling
		}
		temporalactivity.RecordHeartbeat(ctx, string(desc.Phase))
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(2 * time.Second):
		}
	}
}

// planOpts renders the constructor's options for the PLAN as they will
// resolve at run time: an owned agent handle becomes the parent.
func planOpts(agent pipeline.Agent, opts []pipeline.ResourceOption) []pipeline.ResourceOption {
	if h, ok := agent.(pipeline.Handle); ok {
		return append([]pipeline.ResourceOption{pipeline.Parent(h)}, opts...)
	}
	return opts
}
