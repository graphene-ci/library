package k8slib

import (
	"fmt"
	"time"

	"github.com/graphene-ci/temporal-entity/pkg/entdefine"
	entity "github.com/graphene-ci/temporal-entity/pkg/entity"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	"github.com/graphene-ci/library/k8s/internal/contract"
	"github.com/graphene-ci/pipeline/pkg/flow/ownership"
)

// Every Kubernetes resource is a temporal-entity: Init = server-side
// apply + converge to ready, ReconcileEvery = drift detection and
// healing, Finalize = delete + wait gone. One entity definition per
// kind; the definition is untyped inside (manifests travel as maps),
// the user's typing lives at the Resource[T] boundary and in the hooks,
// which decode the live object before calling user knowledge.

// k8sSpec is the entity spec: the desired manifest and which cluster.
type k8sSpec = contract.Spec
type k8sState = contract.State

// kindConfig is the per-kind knowledge, wrapped untyped.
type kindConfig struct {
	ready          func(live map[string]any) (bool, error)
	failed         func(live map[string]any) error
	drifted        func(desired, live map[string]any) (bool, error)
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

// newKind builds a definition scoped to one pipeline preparation.
func newKind(key string, cfg kindConfig) *kindEntry {
	cfg.defaults()
	e := &kindEntry{key: key, cfg: cfg}
	e.def = entdefine.New[k8sSpec, k8sState](entity.KindName(key),
		entdefine.WithInit[k8sSpec, k8sState](e.initEntity),
		entdefine.WithFinalize[k8sSpec, k8sState](e.finalizeEntity),
		entdefine.WithReconcileEvery[k8sSpec, k8sState](cfg.reconcileEvery, e.reconcileEntity),
		entdefine.WithSearchAttributes[k8sSpec, k8sState](true),
	)
	// The tree half: k8s resources are owned like every system record.
	ownership.Register(e.def, func(s *k8sState) *ownership.State { return &s.State })
	return e
}

// initAttempts bounds an ops activity during convergence: a cluster that
// refuses apply or observe ten times in a row (~8 minutes of backoff) is
// a failure of the declaration, not a reason to wait forever.
const initAttempts = 10

// initCtx is activityCtx with the convergence bound.
func (e *kindEntry) initCtx(ctx workflow.Context) workflow.Context {
	return workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 5 * time.Minute,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    time.Second,
			BackoffCoefficient: 2,
			MaximumInterval:    time.Minute,
			MaximumAttempts:    initAttempts,
		},
	})
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

// withdrawTimeout bounds the removal of an object whose declaration
// failed: a controller finishing the delete of what it half-created.
const withdrawTimeout = 10 * time.Minute

// initEntity applies the manifest and converges until the kind's ready
// knowledge says so. A declaration that FAILS — refused, broken by the
// kind's knowledge, not ready in time — takes its object out of the
// cluster before the record closes: a failed record is gone from the
// tree, and an object left behind would be one nobody deletes, which a
// controller keeps creating until the cloud lets it (a network refused
// for quota appears an hour later, and everything that resolves it by
// name follows). An init cut by deletion is not withdrawn here: the
// finalizer runs then.
func (e *kindEntry) initEntity(ctx workflow.Context, spec k8sSpec) (k8sState, error) {
	st, err := e.converge(ctx, spec)
	if err == nil || ctx.Err() != nil {
		return st, err
	}
	if werr := e.withdraw(ctx, st); werr != nil {
		workflow.GetLogger(ctx).Error("failed declaration left its object in the cluster",
			"object", st.Live["metadata"], "error", werr)
		_ = workflow.ExecuteActivity(e.initCtx(ctx), conditionsActivityName, conditionsNote{
			Conditions: st.Conditions, Note: "the failed declaration's object is still in the cluster: " + werr.Error(),
		}).Get(ctx, nil)
	}
	return st, err
}

// withdraw deletes a failed declaration's object and waits, bounded, for
// it to be gone.
func (e *kindEntry) withdraw(ctx workflow.Context, st k8sState) error {
	if st.Live == nil {
		return nil
	}
	actx := e.initCtx(ctx)
	req := opRequest{Kubeconfig: st.Kubeconfig, InCluster: st.InCluster, Manifest: st.Live}
	if err := workflow.ExecuteActivity(actx, deleteActivityName, req).Get(ctx, nil); err != nil {
		return fmt.Errorf("delete: %w", err)
	}
	deadline := workflow.Now(ctx).Add(withdrawTimeout)
	for {
		var obs observation
		if err := workflow.ExecuteActivity(actx, observeActivityName, req).Get(ctx, &obs); err != nil {
			return fmt.Errorf("observe: %w", err)
		}
		if !obs.Exists {
			return nil
		}
		if !workflow.Now(ctx).Before(deadline) {
			return fmt.Errorf("still present after %s", withdrawTimeout)
		}
		if err := workflow.Sleep(ctx, e.cfg.pollInterval); err != nil {
			return err
		}
	}
}

// converge is the declaration proper: apply, then observe until ready.
func (e *kindEntry) converge(ctx workflow.Context, spec k8sSpec) (k8sState, error) {
	var st k8sState
	owner := spec.Owner
	if owner != "" {
		ownership.Init(ctx, &st.State, owner)
	}
	// The DESIRED manifest is the teardown fallback from the first
	// moment: an init aborted by deletion (or failed after apply) must
	// still leave the finalizer something to delete. Observed replaces
	// it on readiness; delete tolerates not-found.
	st.Kubeconfig = spec.Kubeconfig
	st.InCluster = spec.InCluster
	st.Live = spec.Manifest
	actx := e.initCtx(ctx)
	req := opRequest{Kubeconfig: spec.Kubeconfig, InCluster: spec.InCluster, Manifest: spec.Manifest}
	if err := workflow.ExecuteActivity(actx, applyActivityName, req).Get(ctx, nil); err != nil {
		return st, fmt.Errorf("apply: %w", err)
	}
	deadline := workflow.Now(ctx).Add(e.cfg.timeout)
	reported := ""
	for {
		var obs observation
		if err := workflow.ExecuteActivity(actx, observeActivityName, req).Get(ctx, &obs); err != nil {
			return st, fmt.Errorf("observe: %s", contract.Explain(err.Error(), st.Conditions))
		}
		if obs.Exists {
			st.Conditions = contract.ConditionsOf(obs.Manifest)
			// What the controller says while the object converges is the
			// record's news NOW — "the cloud refuses: quota exceeded" —
			// not a line in the timeout twenty minutes later.
			if summary := contract.Summary(st.Conditions); summary != reported {
				reported = summary
				_ = workflow.ExecuteActivity(actx, conditionsActivityName, conditionsNote{Conditions: st.Conditions}).Get(ctx, nil)
			}
			if e.cfg.failed != nil {
				if err := e.cfg.failed(obs.Manifest); err != nil {
					return st, temporal.NewNonRetryableApplicationError(
						contract.Explain("failed: "+err.Error(), st.Conditions), "k8s.failed", nil)
				}
			}
			ready, err := e.cfg.ready(obs.Manifest)
			if err != nil {
				return st, fmt.Errorf("ready check: %w", err)
			}
			if ready {
				st.Live = obs.Manifest
				st.Kubeconfig = spec.Kubeconfig
				st.InCluster = spec.InCluster
				return st, nil
			}
		}
		if !workflow.Now(ctx).Before(deadline) {
			return st, temporal.NewNonRetryableApplicationError(
				contract.Explain(fmt.Sprintf("did not become ready within %s", e.cfg.timeout), st.Conditions), "k8s.timeout", nil)
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
	req := opRequest{Kubeconfig: spec.Kubeconfig, InCluster: spec.InCluster, Manifest: spec.Manifest}
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
	req := opRequest{Kubeconfig: st.Kubeconfig, InCluster: st.InCluster, Manifest: st.Live}
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
