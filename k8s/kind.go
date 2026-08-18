package k8slib

import (
	"fmt"
	"sync"
	"time"

	"github.com/graphene-ci/temporal-entity/pkg/entdefine"
	entity "github.com/graphene-ci/temporal-entity/pkg/entity"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	"github.com/graphene-ci/pipeline/pkg/pipeline"
)

// Every Kubernetes resource is a temporal-entity: Init = server-side
// apply + converge to ready, ReconcileEvery = drift detection and
// healing, Finalize = delete + wait gone. One entity definition per
// kind; the definition is untyped inside (manifests travel as maps),
// the user's typing lives at the Resource[T] boundary and in the hooks,
// which decode the live object before calling user knowledge.

// k8sSpec is the entity spec: the desired manifest and which cluster.
type k8sSpec struct {
	Manifest   map[string]any     `json:"manifest"`
	Kubeconfig pipeline.SecretRef `json:"kubeconfig"`
}

// k8sState is the entity state: the live object and the heal history.
// Kubeconfig is the teardown copy — the finalizer only sees State in the
// current chassis (its documented limitation), so what teardown needs
// lives here. TODO(chassis): let Finalize see Spec and drop it.
type k8sState struct {
	Live       map[string]any     `json:"live,omitempty"`
	Heals      int                `json:"heals,omitempty"`
	Drifted    bool               `json:"drifted,omitempty"`
	Kubeconfig pipeline.SecretRef `json:"kubeconfig"`
}

// kindConfig is the per-kind knowledge, wrapped untyped.
type kindConfig struct {
	ready          func(live map[string]any) (bool, error)
	drifted        func(desired, live map[string]any) (bool, error)
	validate       func(desired map[string]any) error
	reconcileEvery time.Duration
	pollInterval   time.Duration
	timeout        time.Duration
}

func (c *kindConfig) defaults() {
	if c.ready == nil {
		c.ready = func(live map[string]any) (bool, error) {
			return isReady(&unstructured.Unstructured{Object: live}), nil
		}
	}
	if c.reconcileEvery == 0 {
		c.reconcileEvery = 30 * time.Second
	}
	if c.pollInterval == 0 {
		c.pollInterval = 5 * time.Second
	}
	if c.timeout == 0 {
		c.timeout = 20 * time.Minute
	}
}

// kindEntry is one registered kind of this process.
type kindEntry struct {
	key string
	cfg kindConfig
	def *entdefine.Definition[k8sSpec, k8sState]
}

// kinds is the process-wide kind registry, filled during the recording
// pass (write-once before workers start) and read by the declare
// activity.
var kinds = struct {
	sync.Mutex
	m map[string]*kindEntry
}{m: map[string]*kindEntry{}}

// ensureKind builds (or finds) the entity definition of one kind. The
// first declaration fixes the kind's config; hooks are per kind, not per
// object — symmetric to a temporal-entity kind definition.
func ensureKind(key string, cfg kindConfig) *kindEntry {
	kinds.Lock()
	defer kinds.Unlock()
	if e, ok := kinds.m[key]; ok {
		return e
	}
	cfg.defaults()
	e := &kindEntry{key: key, cfg: cfg}
	e.def = entdefine.New[k8sSpec, k8sState](entity.KindName(key),
		entdefine.WithInit[k8sSpec, k8sState](e.initEntity),
		entdefine.WithFinalize[k8sSpec, k8sState](e.finalizeEntity),
		entdefine.WithReconcileEvery[k8sSpec, k8sState](cfg.reconcileEvery, e.reconcileEntity),
	)
	kinds.m[key] = e
	return e
}

func lookupKind(key string) (*kindEntry, error) {
	kinds.Lock()
	defer kinds.Unlock()
	e, ok := kinds.m[key]
	if !ok {
		return nil, fmt.Errorf("kind %q was not declared during the recording pass", key)
	}
	return e, nil
}

func (e *kindEntry) activityCtx(ctx workflow.Context) workflow.Context {
	return workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 5 * time.Minute,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    time.Second,
			BackoffCoefficient: 2,
			MaximumInterval:    time.Minute,
		},
	})
}

// initEntity applies the manifest and converges until the kind's ready
// knowledge says so.
func (e *kindEntry) initEntity(ctx workflow.Context, spec k8sSpec) (k8sState, error) {
	var st k8sState
	actx := e.activityCtx(ctx)
	req := opRequest{Kubeconfig: spec.Kubeconfig, Manifest: spec.Manifest}
	if err := workflow.ExecuteActivity(actx, applyActivityName, req).Get(ctx, nil); err != nil {
		return st, fmt.Errorf("apply: %w", err)
	}
	deadline := workflow.Now(ctx).Add(e.cfg.timeout)
	for {
		var obs observation
		if err := workflow.ExecuteActivity(actx, observeActivityName, req).Get(ctx, &obs); err != nil {
			return st, fmt.Errorf("observe: %w", err)
		}
		if obs.Exists {
			ready, err := e.cfg.ready(obs.Manifest)
			if err != nil {
				return st, fmt.Errorf("ready check: %w", err)
			}
			if ready {
				st.Live = obs.Manifest
				st.Kubeconfig = spec.Kubeconfig
				return st, nil
			}
		}
		if !workflow.Now(ctx).Before(deadline) {
			return st, fmt.Errorf("did not become ready within %s", e.cfg.timeout)
		}
		if err := workflow.Sleep(ctx, e.cfg.pollInterval); err != nil {
			return st, err
		}
	}
}

// reconcileEntity is the drift tick: gone or drifted — re-apply and
// count the heal.
func (e *kindEntry) reconcileEntity(ctx workflow.Context, ec *entdefine.Ctx[k8sSpec, k8sState]) error {
	if ec.Phase() != entity.PhaseReady {
		return nil
	}
	actx := e.activityCtx(ctx)
	spec := ec.Spec()
	req := opRequest{Kubeconfig: spec.Kubeconfig, Manifest: spec.Manifest}
	var obs observation
	if err := workflow.ExecuteActivity(actx, observeActivityName, req).Get(ctx, &obs); err != nil {
		return err
	}
	st := ec.State()
	drifted := !obs.Exists
	if obs.Exists && e.cfg.drifted != nil {
		var err error
		drifted, err = e.cfg.drifted(spec.Manifest, obs.Manifest)
		if err != nil {
			return fmt.Errorf("drift check: %w", err)
		}
	}
	st.Drifted = drifted
	if drifted {
		if err := workflow.ExecuteActivity(actx, applyActivityName, req).Get(ctx, nil); err != nil {
			return fmt.Errorf("heal: %w", err)
		}
		st.Heals++
		st.Drifted = false
	}
	if obs.Exists {
		st.Live = obs.Manifest
	}
	return nil
}

// finalizeEntity deletes the object and waits for it to be gone.
func (e *kindEntry) finalizeEntity(ctx workflow.Context, st *k8sState) error {
	if st.Live == nil {
		return nil
	}
	actx := e.activityCtx(ctx)
	req := opRequest{Kubeconfig: st.Kubeconfig, Manifest: st.Live}
	if err := workflow.ExecuteActivity(actx, deleteActivityName, req).Get(ctx, nil); err != nil {
		return err
	}
	for {
		var obs observation
		if err := workflow.ExecuteActivity(actx, observeActivityName, req).Get(ctx, &obs); err != nil {
			return err
		}
		if !obs.Exists {
			return nil
		}
		if err := workflow.Sleep(ctx, 2*time.Second); err != nil {
			return err
		}
	}
}

