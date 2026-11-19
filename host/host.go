// Package host is the library of HOST SETTINGS as resources: kernel
// parameters, sysfs attributes, directories and systemd units on an
// agent's machine. Each is a record in the tree — owned, cascaded,
// observed — and its Finalize puts the machine back: a run that leaves
// its settings behind through records returns the host to its base state
// when its cascade runs.
//
// A setting is CLAIMED on the machine (a small file under
// /var/lib/graphene-host/claims) by the record that holds it: two records
// cannot hold the same key, and the value to restore is stored on the
// machine before anything changes, so an interrupted Init still
// finalizes correctly. A refusal carries the stable error type
// ConflictCode.
package host

import (
	"context"
	"encoding/json"
	"fmt"
	"io/fs"
	"reflect"
	"strings"
	"time"

	"github.com/graphene-ci/temporal-entity/pkg/entclient"
	"github.com/graphene-ci/temporal-entity/pkg/entdefine"
	entity "github.com/graphene-ci/temporal-entity/pkg/entity"
	temporalactivity "go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"

	"github.com/graphene-ci/library/host/internal/contract"
	"github.com/graphene-ci/pipeline/pkg/flow/ownership"
	"github.com/graphene-ci/pipeline/pkg/leak"
	"github.com/graphene-ci/pipeline/pkg/machine"
	"github.com/graphene-ci/pipeline/pkg/pipeline"
	"github.com/graphene-ci/pipeline/pkg/ref"
	"github.com/graphene-ci/pipeline/pkg/wire"
)

// Entity kinds.
const (
	SysctlKind    entity.KindName = contract.SysctlKind
	SysfsKind     entity.KindName = contract.SysfsKind
	DirKind       entity.KindName = contract.DirKind
	UnitKind      entity.KindName = contract.UnitKind
	UnitStateKind entity.KindName = contract.UnitStateKind
)

// applyRequest is the input of every apply and remove activity: the
// record that acts (its claims carry this name) and its spec.
type applyRequest[Spec any] struct {
	Record string `json:"record"`
	Spec   Spec   `json:"spec"`
}

// hostState is the state of every host record: the spec it applied, kept
// for Finalize, and what Init reported.
type hostState[Spec, Out any] struct {
	ownership.State
	Spec Spec `json:"spec"`
	Info Out  `json:"info"`
}

// kindDef describes one host kind for the shared entity shape.
type kindDef[Spec, Out any] struct {
	kind        entity.KindName
	description string
	tree        func(Spec) contract.Tree
	apply       string
	remove      string
	// removeOpts bound the teardown; a gone machine must not wedge the
	// cascade.
	removeOpts workflow.ActivityOptions
}

// applyOpts retry a transient failure a few times; a key this kernel does
// not have or a path the machine refuses is permanent, and the record must
// fail instead of creating forever.
func applyOpts() workflow.ActivityOptions {
	return workflow.ActivityOptions{
		StartToCloseTimeout: 5 * time.Minute,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    time.Second,
			BackoffCoefficient: 2,
			MaximumInterval:    15 * time.Second,
			MaximumAttempts:    5,
		},
	}
}

// boundedRemove rides out a transient blip; ScheduleToClose caps the
// total, then Finalize gives up and lets the record go.
func boundedRemove() workflow.ActivityOptions {
	return workflow.ActivityOptions{
		StartToCloseTimeout:    time.Minute,
		ScheduleToCloseTimeout: 2 * time.Minute,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    time.Second,
			BackoffCoefficient: 2,
			MaximumInterval:    15 * time.Second,
		},
	}
}

// purgeRemove allows a large tree its time, with a heartbeat to prove the
// purge is alive.
func purgeRemove() workflow.ActivityOptions {
	return workflow.ActivityOptions{
		StartToCloseTimeout:    time.Hour,
		ScheduleToCloseTimeout: 2 * time.Hour,
		HeartbeatTimeout:       time.Minute,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    5 * time.Second,
			BackoffCoefficient: 2,
			MaximumInterval:    time.Minute,
		},
	}
}

func (k kindDef[Spec, Out]) definition() *entdefine.Definition[Spec, hostState[Spec, Out]] {
	type state = hostState[Spec, Out]
	remove := func(ctx workflow.Context, st *state) {
		record := workflow.GetInfo(ctx).WorkflowExecution.ID
		err := workflow.ExecuteActivity(workflow.WithActivityOptions(ctx, k.removeOpts), k.remove,
			applyRequest[Spec]{Record: record, Spec: st.Spec}).Get(ctx, nil)
		if err != nil {
			leak.Report(ctx, err)
		}
	}
	def := entdefine.New[Spec, state](k.kind,
		entdefine.WithSearchAttributes[Spec, state](true),
		entdefine.WithInit[Spec, state](func(ctx workflow.Context, spec Spec) (state, error) {
			var st state
			tree := k.tree(spec)
			// Flows BEFORE Init: Init mirrors owner and edges into visibility.
			st.Flows = tree.Flows
			if tree.Owner != "" {
				ownership.Init(ctx, &st.State, tree.Owner)
			}
			// The spec is kept BEFORE apply runs: an apply cut short has
			// already changed the machine, and Finalize must know what.
			st.Spec = spec
			record := workflow.GetInfo(ctx).WorkflowExecution.ID
			err := workflow.ExecuteActivity(workflow.WithActivityOptions(ctx, applyOpts()), k.apply,
				applyRequest[Spec]{Record: record, Spec: spec}).Get(ctx, &st.Info)
			if err != nil {
				// A failed apply undoes what it got to: the claims it took
				// and the values it wrote.
				remove(ctx, &st)
				return st, err
			}
			return st, nil
		}),
		entdefine.WithFinalize[Spec, state](func(ctx workflow.Context, st *state) error {
			remove(ctx, st)
			return nil
		}),
	)
	ownership.Register(def, func(s *state) *ownership.State { return &s.State })
	return def
}

// declarer binds a kind for the declare activity.
type declarer func(ctx context.Context, cl client.Client, queue string, req contract.DeclareRequest, labels map[string]string) (json.RawMessage, error)

func (k kindDef[Spec, Out]) declarer(def *entdefine.Definition[Spec, hostState[Spec, Out]]) declarer {
	return func(ctx context.Context, cl client.Client, queue string, req contract.DeclareRequest, labels map[string]string) (json.RawMessage, error) {
		var spec Spec
		if err := json.Unmarshal(req.Spec, &spec); err != nil {
			return nil, err
		}
		bound := entclient.Bind(def, cl, queue)
		rid := entity.ResourceID(req.Name)
		if _, err := bound.CreateOrAttach(ctx, rid, spec, entclient.WithLabels(labels)); err != nil {
			return nil, err
		}
		for {
			desc, err := bound.Describe(ctx, rid)
			if err != nil {
				return nil, err
			}
			switch desc.Phase {
			case entity.PhaseReady:
				return json.Marshal(desc.State.Info)
			case entity.PhaseCreateFailed:
				return nil, fmt.Errorf("%s %s failed to apply", k.kind, req.Name)
			case entity.PhaseDeleting, entity.PhaseDeleted, entity.PhaseDeleteFailed:
				return nil, fmt.Errorf("%s %s is going away (phase %s)", k.kind, req.Name, desc.Phase)
			case entity.PhaseCreating:
			}
			temporalactivity.RecordHeartbeat(ctx, string(desc.Phase))
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(time.Second):
			}
		}
	}
}

func sysctlKind() kindDef[sysctlSpec, SysctlInfo] {
	return kindDef[sysctlSpec, SysctlInfo]{
		kind:        SysctlKind,
		description: "kernel parameters held while the record lives, prior values restored on removal",
		tree:        func(s sysctlSpec) contract.Tree { return s.Tree },
		apply:       "host.sysctl.apply", remove: "host.sysctl.remove",
		removeOpts: boundedRemove(),
	}
}

func sysfsKind() kindDef[sysfsSpec, SysfsInfo] {
	return kindDef[sysfsSpec, SysfsInfo]{
		kind:        SysfsKind,
		description: "a /sys attribute held while the record lives, prior value restored on removal",
		tree:        func(s sysfsSpec) contract.Tree { return s.Tree },
		apply:       "host.sysfs.apply", remove: "host.sysfs.remove",
		removeOpts: boundedRemove(),
	}
}

func dirKind() kindDef[dirSpec, DirInfo] {
	return kindDef[dirSpec, DirInfo]{
		kind:        DirKind,
		description: "a directory the record creates; removal deletes it with its contents",
		tree:        func(s dirSpec) contract.Tree { return s.Tree },
		apply:       "host.dir.apply", remove: "host.dir.remove",
		removeOpts: purgeRemove(),
	}
}

func unitKind() kindDef[unitSpec, UnitInfo] {
	return kindDef[unitSpec, UnitInfo]{
		kind:        UnitKind,
		description: "a systemd unit the record installs and runs; removal stops it and deletes its file",
		tree:        func(s unitSpec) contract.Tree { return s.Tree },
		apply:       "host.unit.apply", remove: "host.unit.remove",
		removeOpts: boundedRemove(),
	}
}

func unitStateKind() kindDef[unitStateSpec, UnitStateInfo] {
	return kindDef[unitStateSpec, UnitStateInfo]{
		kind:        UnitStateKind,
		description: "an existing systemd unit held stopped, disabled or masked; removal restores its state",
		tree:        func(s unitStateSpec) contract.Tree { return s.Tree },
		apply:       "host.unit-state.apply", remove: "host.unit-state.remove",
		removeOpts: boundedRemove(),
	}
}

// registration is everything one kind contributes to a worker.
type registration struct {
	kind          entity.KindName
	description   string
	spec          reflect.Type
	apply, remove string
	applyFn       any
	removeFn      any
	register      func(worker.WorkflowRegistry) error
	declare       declarer
}

func registerKind[Spec, Out any](register func(registration), k kindDef[Spec, Out],
	applyFn func(context.Context, applyRequest[Spec]) (Out, error), removeFn func(context.Context, applyRequest[Spec]) error,
) {
	def := k.definition()
	register(registration{
		kind: k.kind, description: k.description, spec: reflect.TypeFor[Spec](),
		apply: k.apply, remove: k.remove, applyFn: applyFn, removeFn: removeFn,
		register: def.Register, declare: k.declarer(def),
	})
}

// recordEntities scopes registration to a pipeline preparation.
func recordEntities(ctx pipeline.Context) {
	if ctx.RecordOnce("host.entities") {
		recordEntitiesOnce(ctx, NewMachine(machine.Root(), execRunner{}))
	}
}

func recordEntitiesOnce(ctx pipeline.Context, m Machine) {
	dims := []string{"state", "events", "logs", "metrics", "traces"}
	declarers := map[string]declarer{}
	defs := []func(worker.WorkflowRegistry) error{}
	register := func(r registration) {
		ctx.RecordKindInfo(string(r.kind), r.description, r.spec, dims)
		ctx.RecordActivity(r.apply, r.applyFn)
		ctx.RecordActivity(r.remove, r.removeFn)
		declarers[string(r.kind)] = r.declare
		defs = append(defs, r.register)
	}
	registerKind(register, sysctlKind(), m.applySysctl, m.removeSysctl)
	registerKind(register, sysfsKind(), m.applySysfs, m.removeSysfs)
	registerKind(register, dirKind(), m.applyDir, m.removeDir)
	registerKind(register, unitKind(), m.applyUnit, m.removeUnit)
	registerKind(register, unitStateKind(), m.applyUnitState, m.removeUnitState)

	ctx.RecordWorker(func(w worker.Worker, cl client.Client) error {
		for _, register := range defs {
			if err := register(w); err != nil {
				return err
			}
		}
		w.RegisterActivityWithOptions(makeDeclare(cl, declarers), temporalactivity.RegisterOptions{Name: contract.DeclareActivity})
		return nil
	})
}

func makeDeclare(cl client.Client, declarers map[string]declarer) func(context.Context, contract.DeclareRequest) (json.RawMessage, error) {
	return func(ctx context.Context, req contract.DeclareRequest) (json.RawMessage, error) {
		if err := wire.ValidateUserLabels(req.Labels); err != nil {
			return nil, err
		}
		d, ok := declarers[req.Kind]
		if !ok {
			return nil, fmt.Errorf("unknown host kind %q", req.Kind)
		}
		labels := map[string]string{}
		for k, v := range req.Labels {
			labels[k] = v
		}
		if req.RunId != "" {
			labels["graphene.io/run"] = req.RunId
		}
		return d(ctx, cl, temporalactivity.GetInfo(ctx).TaskQueue, req, labels)
	}
}

// declare is the shared constructor: plan, validate, own, dispatch.
func declare[Out any](ctx pipeline.Context, agent pipeline.Agent, kind entity.KindName, name string,
	opts []pipeline.ResourceOption, build func(contract.Tree) any, validate func() error,
) pipeline.Resource[Out] {
	self := ref.OwnerRef(string(kind) + "/" + name)
	if h, ok := agent.(pipeline.Handle); ok {
		// Parent only when the agent is OURS: a foreign agent cannot be
		// burdened, the setting stays with the run.
		opts = append([]pipeline.ResourceOption{pipeline.Parent(h)}, opts...)
	}
	if ctx.Recording() {
		recordEntities(ctx)
		ctx.RecordDeclare(self, pipeline.BuildResourceOptions(ctx, opts))
		return pipeline.NewResource[Out](ctx, self, nil)
	}
	if err := validate(); err != nil {
		return pipeline.FailedResource[Out](ctx, self, err)
	}
	o := pipeline.BuildResourceOptions(ctx, opts)
	raw, err := json.Marshal(build(contract.Tree{Owner: o.Parent, Flows: o.Flows}))
	if err != nil {
		return pipeline.FailedResource[Out](ctx, self, err)
	}
	fut := pipeline.DispatchOnAgent(ctx, agent.AgentId(), dispatchOpts(), contract.DeclareActivity, contract.DeclareRequest{
		Kind: string(kind), Name: name, Labels: o.Labels, RunId: string(ctx.RunId()), Spec: raw,
	})
	return pipeline.NewResource[Out](ctx, self, fut)
}

func dispatchOpts() workflow.ActivityOptions {
	return workflow.ActivityOptions{StartToCloseTimeout: 10 * time.Minute, HeartbeatTimeout: time.Minute}
}

// recordName names a record: the agent, then a slug of what it holds, so
// two machines never collide and one machine's settings stay apart.
func recordName(agent pipeline.Agent, what string) string {
	return string(agent.AgentId()) + "-" + strings.Trim(fileSafe(what), "-")
}

// fileSafe keeps letters, digits, dot and dash; everything else becomes a
// dash. Record names and the files named after records use it.
func fileSafe(s string) string {
	var b strings.Builder
	for _, r := range s {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '-', r == '.':
			b.WriteRune(r)
		default:
			b.WriteByte('-')
		}
	}
	return b.String()
}

// Sysctl holds kernel parameters on the agent's machine while the record
// lives: the values go live and into a sysctl.d drop-in (they survive a
// reboot); removal deletes the drop-in and writes each key's prior value
// back. name tells several Sysctl records on one machine apart.
func Sysctl(ctx pipeline.Context, agent pipeline.Agent, name string, values map[string]string, opts ...pipeline.ResourceOption) pipeline.Resource[SysctlInfo] {
	spec := sysctlSpec{Values: values}
	return declare[SysctlInfo](ctx, agent, SysctlKind, recordName(agent, name), opts,
		func(t contract.Tree) any { spec.Tree = t; return spec },
		func() error { return validateSysctl(spec) })
}

// Sysfs holds one /sys attribute (transparent hugepages, a CPU governor,
// a block device's scheduler) at value while the record lives; removal
// writes the prior value back. A reboot resets it: hold it from a record
// that is declared again after one.
func Sysfs(ctx pipeline.Context, agent pipeline.Agent, path, value string, opts ...pipeline.ResourceOption) pipeline.Resource[SysfsInfo] {
	spec := sysfsSpec{Path: path, Value: value}
	return declare[SysfsInfo](ctx, agent, SysfsKind, recordName(agent, path), opts,
		func(t contract.Tree) any { spec.Tree = t; return spec },
		func() error { return validateSysfs(spec) })
}

// DirAccess is a directory's permission bits and owner. A zero Mode is
// 0750; a zero UID/GID is root.
type DirAccess struct {
	Mode fs.FileMode
	UID  int
	GID  int
}

// Dir creates a directory on the agent's machine that the record owns:
// removal deletes it WITH ITS CONTENTS (moved aside at once, purged
// after). A path that already exists is refused — the record never owns
// data it did not create. Containers reach it through a bind mount.
func Dir(ctx pipeline.Context, agent pipeline.Agent, path string, access DirAccess, opts ...pipeline.ResourceOption) pipeline.Resource[DirInfo] {
	spec := dirSpec{Path: path, Mode: uint32(access.Mode.Perm()), UID: access.UID, GID: access.GID}
	return declare[DirInfo](ctx, agent, DirKind, recordName(agent, path), opts,
		func(t contract.Tree) any { spec.Tree = t; return spec },
		func() error { return validateDir(spec) })
}

// Unit installs a systemd unit the record owns: the file goes to
// /etc/systemd/system, the unit is enabled and started; removal stops and
// disables it and deletes the file. A unit file of that name that exists
// already is refused. An EnvironmentFile it names is a File record.
func Unit(ctx pipeline.Context, agent pipeline.Agent, name string, content []byte, opts ...pipeline.ResourceOption) pipeline.Resource[UnitInfo] {
	spec := unitSpec{Name: name, Content: content}
	return declare[UnitInfo](ctx, agent, UnitKind, recordName(agent, name), opts,
		func(t contract.Tree) any { spec.Tree = t; return spec },
		func() error { return validateUnit(spec) })
}

// UnitState holds a unit the record does not own — unattended-upgrades,
// apt-daily.timer — stopped, disabled or masked while the record lives;
// removal restores the state it found (masked, enabled, running).
func UnitState(ctx pipeline.Context, agent pipeline.Agent, name string, want UnitWant, opts ...pipeline.ResourceOption) pipeline.Resource[UnitStateInfo] {
	spec := unitStateSpec{Name: name, Want: string(want)}
	return declare[UnitStateInfo](ctx, agent, UnitStateKind, recordName(agent, name), opts,
		func(t contract.Tree) any { spec.Tree = t; return spec },
		func() error { return validateUnitState(spec) })
}
