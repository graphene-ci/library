// The durable half of dockerlib: containers, volumes, and networks are
// ENTITY RECORDS — owned in the tree, cascaded, transferable to a
// stand — whose lifecycle activities run on the machine's own
// (agent × run) executor. The executor lives while these records do.
package dockerlib

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"sync"
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
	// Flows are the declared outgoing edges of this container (Р-Н25) —
	// carried into the record's state for the topology view.
	Flows []ownership.Flow `json:"flows,omitempty"`
	// Scrape is a prometheus metrics endpoint the container exposes; the
	// observation beat pulls it and ships the samples as the container's
	// own metrics (Р-Н27). Empty disables scraping.
	Scrape string `json:"scrape,omitempty"`
}

type containerState struct {
	Info Info `json:"info"`
	// Name is the container's name on the machine, recorded at init
	// BEFORE the create runs — so finalize can remove even a container an
	// interrupted init created (by name) but never reported an Id for.
	// runActivity names the container by spec.Name; docker removes by name
	// or id alike, so removing by name covers both the ready record and
	// the mid-create leak. There is no reaper for library containers on a
	// user's machine (only for managed run containers on the server), so a
	// missed remove on a persistent machine leaks forever.
	Name string `json:"name,omitempty"`
	// Status is what the last observation beat saw; a transition is
	// logged the moment it is noticed.
	Status string `json:"status,omitempty"`
	// ObservedUnixNano is the log window's low edge.
	ObservedUnixNano int64 `json:"observedUnixNano,omitempty"`
	// Scrape mirrors the spec's metrics endpoint for the beat.
	Scrape string `json:"scrape,omitempty"`
	ownership.State
}

type volumeSpec struct {
	Options volume.CreateOptions `json:"options"`
	Owner   ref.OwnerRef         `json:"owner,omitempty"`
}

type volumeState struct {
	Info VolumeInfo `json:"info"`
	// Name is the volume name recorded at init before the ensure runs, so
	// finalize removes even a volume an interrupted init created but never
	// reported. See containerState.Name.
	Name string `json:"name,omitempty"`
	ownership.State
}

type networkSpec struct {
	Spec  NetworkSpec  `json:"spec"`
	Owner ref.OwnerRef `json:"owner,omitempty"`
}

type networkState struct {
	Info NetworkInfo `json:"info"`
	// Name is the network name recorded at init before the ensure runs, so
	// finalize removes even a network an interrupted init created but never
	// reported. See containerState.Name.
	Name string `json:"name,omitempty"`
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

// boundedRemoveCtx bounds a teardown remove: retries ride out a
// transient daemon blip, but ScheduleToCloseTimeout caps the total so a
// permanently-unreachable daemon (the machine going away) cannot wedge the
// cascade — the finalize then gives up and lets the record go. Shared by
// container, volume and network finalize.
func boundedRemoveCtx(ctx workflow.Context) workflow.Context {
	return workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout:    time.Minute,
		ScheduleToCloseTimeout: 2 * time.Minute,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    time.Second,
			BackoffCoefficient: 2,
			MaximumInterval:    15 * time.Second,
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
			st.State.Flows = spec.Flows
			st.Scrape = spec.Scrape
			st.Name = spec.Name // recorded BEFORE create, so a cancel mid-create still finalizes
			err := workflow.ExecuteActivity(entityActivityCtx(ctx), runActivityName,
				Spec{Name: spec.Name, Config: spec.Config, Host: spec.Host}).Get(ctx, &st.Info)
			return st, err
		}),
		entdefine.WithFinalize[containerSpec, containerState](func(ctx workflow.Context, st *containerState) error {
			// Remove by NAME, not id: a cancel that cut init mid-create left
			// no Id in state, yet runActivity may already have created the
			// container by name — removing by name reaps that leak too (there
			// is no reaper for library containers on a user's machine). An id
			// is a valid remove target as well, so name covers the ready case.
			target := st.Name
			if target == "" {
				target = st.Info.Id
			}
			if target == "" {
				return nil // no name and no id — nothing was ever created
			}
			// BOUNDED and BEST-EFFORT. A transient daemon blip is retried
			// (boundedRemoveCtx caps the retries so it cannot wedge the whole
			// run's teardown forever), but if the daemon stays unreachable —
			// the machine is being torn down under us, the container dies with
			// it — we log and let the record go. A permanent wedge on one
			// container would strand the entire cascade; a rare leak on a
			// surviving machine is the lesser evil.
			if err := workflow.ExecuteActivity(boundedRemoveCtx(ctx), removeActivityName, target).Get(ctx, nil); err != nil {
				workflow.GetLogger(ctx).Warn("container remove gave up; letting the record go",
					"container", target, "error", err)
			}
			return nil
		}),
		// The record's own telemetry: each beat ships the container's
		// log lines, a stats sample, and any status transition — the
		// executor's obs interceptor stamps it all with THIS record's
		// reference.
		entdefine.WithReconcileEvery[containerSpec, containerState](observeEvery, func(ctx workflow.Context, ec *entdefine.Ctx[containerSpec, containerState]) error {
			st := ec.State()
			if st.Info.Id == "" {
				return nil
			}
			var res observeResult
			if err := workflow.ExecuteActivity(entityActivityCtx(ctx), observeActivityName, observeRequest{
				Id: st.Info.Id, SinceUnixNano: st.ObservedUnixNano, PrevStatus: st.Status, Scrape: st.Scrape,
				Entity: workflow.GetInfo(ctx).WorkflowExecution.ID,
			}).Get(ctx, &res); err != nil {
				return nil //nolint:nilerr // observation must never kill the record
			}
			st.Status, st.ObservedUnixNano = res.Status, res.LastLogUnixNano
			return nil
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
			st.Name = spec.Options.Name // recorded BEFORE ensure, so a cancel mid-create still finalizes
			err := workflow.ExecuteActivity(entityActivityCtx(ctx), volumeEnsureActivityName, spec.Options).Get(ctx, &st.Info)
			return st, err
		}),
		entdefine.WithFinalize[volumeSpec, volumeState](func(ctx workflow.Context, st *volumeState) error {
			target := st.Name
			if target == "" {
				target = st.Info.Name
			}
			if target == "" {
				return nil
			}
			// BOUNDED and BEST-EFFORT — same reasoning as the container finalize.
			if err := workflow.ExecuteActivity(boundedRemoveCtx(ctx), volumeRemoveActivityName, target).Get(ctx, nil); err != nil {
				workflow.GetLogger(ctx).Warn("volume remove gave up; letting the record go",
					"volume", target, "error", err)
			}
			return nil
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
			st.Name = spec.Spec.Name // recorded BEFORE ensure, so a cancel mid-create still finalizes
			err := workflow.ExecuteActivity(entityActivityCtx(ctx), networkEnsureActivityName, spec.Spec).Get(ctx, &st.Info)
			return st, err
		}),
		entdefine.WithFinalize[networkSpec, networkState](func(ctx workflow.Context, st *networkState) error {
			target := st.Name
			if target == "" {
				target = st.Info.Name
			}
			if target == "" {
				return nil
			}
			// BOUNDED and BEST-EFFORT — same reasoning as the container finalize.
			if err := workflow.ExecuteActivity(boundedRemoveCtx(ctx), networkRemoveActivityName, target).Get(ctx, nil); err != nil {
				workflow.GetLogger(ctx).Warn("network remove gave up; letting the record go",
					"network", target, "error", err)
			}
			return nil
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

// recordOnce guards recordEntities: the constructor calls it on EVERY
// container/volume/network declared, but the definitions, activities and
// the worker hook must register exactly ONCE — a second RecordWorker
// hook re-registers the "docker" workflow and panics ("already
// registered"). The recording pass is one per process, so a process-wide
// Once is the right scope.
var recordOnce sync.Once

// recordEntities registers the definitions and the declare activity —
// once per process, during the recording pass.
func recordEntities(ctx pipeline.Context) {
	recordOnce.Do(func() { recordEntitiesOnce(ctx) })
}

func recordEntitiesOnce(ctx pipeline.Context) {
	// Full dictionary entries: the installation's kind records learn
	// what a declaration looks like and which dimensions each kind
	// serves — all five: the observation beat and the executor's obs
	// interceptor emit under the record's own reference.
	allDims := []string{"state", "events", "logs", "metrics", "traces"}
	ctx.RecordKindInfo(string(ContainerKind),
		"a docker container on the agent's machine", reflect.TypeOf(containerSpec{}), allDims)
	ctx.RecordKindInfo(string(VolumeKind),
		"a docker volume on the agent's machine", reflect.TypeOf(volumeSpec{}), allDims)
	ctx.RecordKindInfo(string(NetworkKind),
		"a docker network on the agent's machine", reflect.TypeOf(networkSpec{}), allDims)
	ctx.RecordActivity(runActivityName, runActivity)
	ctx.RecordActivity(observeActivityName, observeActivity)
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
