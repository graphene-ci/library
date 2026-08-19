package k8slib

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/graphene-ci/temporal-entity/pkg/entclient"
	entity "github.com/graphene-ci/temporal-entity/pkg/entity"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"

	"github.com/graphene-ci/pipeline/pkg/pipeline"
	"github.com/graphene-ci/pipeline/pkg/ref"
	"github.com/graphene-ci/pipeline/pkg/wire"
)

// Option tunes one KIND of resources, typed by the user's own Go type —
// symmetric to a temporal-entity kind definition: readiness, drift, and
// validation are the user's knowledge, the chassis is the library's.
// The first declaration of a kind fixes its config.
type Option[T any] func(*conf[T])

type conf[T any] struct {
	ready          func(live *T) bool
	drifted        func(desired, live *T) bool
	validate       func(desired *T) error
	reconcileEvery time.Duration
	pollInterval   time.Duration
	timeout        time.Duration
	tree           []pipeline.ResourceOption
}

// WithReady replaces the default readiness (the Ready-condition /
// kstatus convention) with the user's own: a pure function of the live
// object.
func WithReady[T any](fn func(live *T) bool) Option[T] {
	return func(c *conf[T]) { c.ready = fn }
}

// WithDrifted adds drift knowledge: the reconcile tick re-applies when
// it reports true. Without it only disappearance counts as drift.
func WithDrifted[T any](fn func(desired, live *T) bool) Option[T] {
	return func(c *conf[T]) { c.drifted = fn }
}

// WithValidate rejects a bad desired object before anything runs.
func WithValidate[T any](fn func(desired *T) error) Option[T] {
	return func(c *conf[T]) { c.validate = fn }
}

// WithReconcileEvery sets the drift-tick period (default 30s).
func WithReconcileEvery[T any](d time.Duration) Option[T] {
	return func(c *conf[T]) { c.reconcileEvery = d }
}

// WithPollInterval sets the convergence polling period (default 5s).
func WithPollInterval[T any](d time.Duration) Option[T] {
	return func(c *conf[T]) { c.pollInterval = d }
}

// WithTimeout bounds waiting for readiness (default 20m).
func WithTimeout[T any](d time.Duration) Option[T] {
	return func(c *conf[T]) { c.timeout = d }
}

// WithResourceOption carries the pipeline resource options of the
// declaration (pipeline.Parent, pipeline.Children) into the typed call.
func WithResourceOption[T any](opts ...pipeline.ResourceOption) Option[T] {
	return func(c *conf[T]) { c.tree = append(c.tree, opts...) }
}

// Resource declares one Kubernetes object as a graphene Resource backed
// by a temporal-entity: Init = apply + converge, ReconcileEvery = drift
// heal, Finalize = delete. obj is the user's own typed object (a
// k8s.io/api type, a provider's CRD type); Ready(ctx) returns the LIVE
// typed object — real ids live in its Status, put there by the
// provider, not invented here. For cross-resource wiring prefer the
// provider's native *Ref fields: the cluster resolves them itself.
func Resource[T any](ctx pipeline.Context, c *Client, obj *T, opts ...Option[T]) pipeline.Resource[*T] {
	cfg := conf[T]{}
	for _, opt := range opts {
		opt(&cfg)
	}
	manifest, kindKey, name, err := c.manifestOf(obj)
	self := ref.OwnerRef(kindKey + "/" + name)

	if ctx.Recording() {
		if err == nil {
			ctx.RecordKind(kindKey)
			ensureKindRecorded(ctx, kindKey, cfg)
		}
		return pipeline.NewResource[*T](ctx, self, nil)
	}

	fut, set := workflow.NewFuture(ctx)
	res := pipeline.NewResource[*T](ctx, self, fut)
	if err != nil {
		set.SetError(fmt.Errorf("k8s resource: %w", err))
		return res
	}
	if cfg.validate != nil {
		if err := cfg.validate(obj); err != nil {
			set.SetError(fmt.Errorf("validate %s: %w", self, err))
			return res
		}
	}
	o := pipeline.BuildResourceOptions(ctx, cfg.tree)
	req := declareRequest{
		Kind:      kindKey,
		Name:      name,
		TaskQueue: wire.RunQueue(ctx.RunId()),
		Labels:    o.Labels,
		RunId:     string(ctx.RunId()),
		Spec:      k8sSpec{Manifest: manifest, Kubeconfig: c.kubeconfig, Owner: o.Parent},
	}
	workflow.Go(ctx, func(gctx workflow.Context) {
		actx := workflow.WithActivityOptions(gctx, workflow.ActivityOptions{
			TaskQueue:           req.TaskQueue,
			StartToCloseTimeout: cfg.timeoutOrDefault(),
			HeartbeatTimeout:    time.Minute,
			RetryPolicy: &temporal.RetryPolicy{
				InitialInterval:    time.Second,
				BackoffCoefficient: 2,
				MaximumInterval:    time.Minute,
			},
		})
		var st k8sState
		if err := workflow.ExecuteActivity(actx, declareActivityName, req).Get(gctx, &st); err != nil {
			set.SetError(fmt.Errorf("%s: %w", self, err))
			return
		}
		live, err := decodeInto[T](st.Live)
		if err != nil {
			set.SetError(fmt.Errorf("decode live %s: %w", self, err))
			return
		}
		set.Set(live, nil)
	})
	return res
}

func (c conf[T]) timeoutOrDefault() time.Duration {
	if c.timeout > 0 {
		return c.timeout + 5*time.Minute
	}
	return 25 * time.Minute
}

// ensureKindRecorded builds the kind's entity definition from the typed
// config and records everything the workers need: the ops activities,
// the definition registration, and (once) the declare activity.
func ensureKindRecorded[T any](ctx pipeline.Context, kindKey string, cfg conf[T]) {
	untyped := kindConfig{
		reconcileEvery: cfg.reconcileEvery,
		pollInterval:   cfg.pollInterval,
		timeout:        cfg.timeout,
	}
	if cfg.ready != nil {
		ready := cfg.ready
		untyped.ready = func(live map[string]any) (bool, error) {
			t, err := decodeInto[T](live)
			if err != nil {
				return false, err
			}
			return ready(t), nil
		}
	}
	if cfg.drifted != nil {
		drifted := cfg.drifted
		untyped.drifted = func(desired, live map[string]any) (bool, error) {
			d, err := decodeInto[T](desired)
			if err != nil {
				return false, err
			}
			l, err := decodeInto[T](live)
			if err != nil {
				return false, err
			}
			return drifted(d, l), nil
		}
	}
	e := ensureKind(kindKey, untyped)

	ctx.RecordActivity(applyActivityName, applyActivity)
	ctx.RecordActivity(observeActivityName, observeActivity)
	ctx.RecordActivity(deleteActivityName, deleteActivity)
	ctx.RecordWorker(func(w worker.Worker, _ client.Client) error {
		return e.def.Register(w)
	})
	recordDeclareOnce(ctx)
}

// declareActivityName is the builtin that starts (or attaches to) the
// entity and waits for readiness — the run-worker mirror of the server's
// declare activities for system resources.
const declareActivityName = "k8s.entity.declare"

// declareRequest asks the declare activity for one entity.
type declareRequest struct {
	Kind      string            `json:"kind"`
	Name      string            `json:"name"`
	TaskQueue string            `json:"taskQueue"`
	Labels    map[string]string `json:"labels,omitempty"`
	RunId     string            `json:"runId,omitempty"`
	Spec      k8sSpec           `json:"spec"`
}

var declareRecorded bool // recording pass is single-threaded, pre-worker

func recordDeclareOnce(ctx pipeline.Context) {
	if declareRecorded {
		return
	}
	declareRecorded = true
	ctx.RecordWorker(func(w worker.Worker, cl client.Client) error {
		w.RegisterActivityWithOptions(makeDeclare(cl), activity.RegisterOptions{Name: declareActivityName})
		return nil
	})
}

// makeDeclare builds the declare activity over the worker's own client.
func makeDeclare(cl client.Client) func(context.Context, declareRequest) (k8sState, error) {
	return func(ctx context.Context, req declareRequest) (k8sState, error) {
		e, err := lookupKind(req.Kind)
		if err != nil {
			return k8sState{}, err
		}
		if err := wire.ValidateUserLabels(req.Labels); err != nil {
			return k8sState{}, err
		}
		labels := make(map[string]string, len(req.Labels)+1)
		for k, v := range req.Labels {
			labels[k] = v
		}
		if req.RunId != "" {
			labels[wire.LabelRun] = req.RunId
		}
		entities := entclient.Bind(e.def, cl, req.TaskQueue)
		rid := entity.ResourceID(req.Name)
		if _, err := entities.CreateOrAttach(ctx, rid, req.Spec, entclient.WithLabels(labels)); err != nil {
			return k8sState{}, err
		}
		for {
			out, err := entities.Describe(ctx, rid)
			if err != nil {
				return k8sState{}, err
			}
			switch out.Phase {
			case entity.PhaseReady:
				return out.State, nil
			case entity.PhaseCreateFailed:
				return k8sState{}, fmt.Errorf("%s/%s failed to converge", req.Kind, req.Name)
			case entity.PhaseDeleting, entity.PhaseDeleted, entity.PhaseDeleteFailed:
				return k8sState{}, fmt.Errorf("%s/%s is going away (phase %s)", req.Kind, req.Name, out.Phase)
			case entity.PhaseCreating:
				// keep polling
			}
			activity.RecordHeartbeat(ctx, string(out.Phase))
			select {
			case <-ctx.Done():
				return k8sState{}, ctx.Err()
			case <-time.After(2 * time.Second):
			}
		}
	}
}

// manifestOf flattens the typed object into a manifest, deriving
// apiVersion/kind from the client's scheme when TypeMeta is not set by
// hand.
func (c *Client) manifestOf(obj any) (manifest map[string]any, kindKey, name string, err error) {
	var m map[string]any
	switch v := obj.(type) {
	case *unstructured.Unstructured:
		m = v.Object
	case map[string]any:
		m = v
	default:
		converted, cerr := runtime.DefaultUnstructuredConverter.ToUnstructured(obj)
		if cerr != nil {
			return nil, "", "", fmt.Errorf("convert %T: %w", obj, cerr)
		}
		m = converted
	}
	u := unstructured.Unstructured{Object: m}
	if u.GetKind() == "" {
		ro, ok := obj.(runtime.Object)
		if !ok {
			return nil, "", "", fmt.Errorf("object %T carries no kind and is not a runtime.Object", obj)
		}
		gvks, _, gerr := c.scheme.ObjectKinds(ro)
		if gerr != nil || len(gvks) == 0 {
			return nil, "", "", fmt.Errorf("no kind for %T: teach the client with WithScheme(provider.AddToScheme): %w", obj, gerr)
		}
		u.SetGroupVersionKind(gvks[0])
	}
	name = u.GetName()
	if name == "" {
		return nil, "", "", fmt.Errorf("object %T has no metadata.name", obj)
	}
	if ns := u.GetNamespace(); ns != "" {
		name = ns + "." + name
	}
	gvk := u.GroupVersionKind()
	kindKey = "k8s." + gvk.Group + "." + gvk.Version + "." + gvk.Kind
	return u.Object, kindKey, name, nil
}

// decodeInto round-trips a manifest into the user's type.
func decodeInto[T any](m map[string]any) (*T, error) {
	if m == nil {
		return nil, nil
	}
	raw, err := json.Marshal(m)
	if err != nil {
		return nil, err
	}
	out := new(T)
	if err := json.Unmarshal(raw, out); err != nil {
		return nil, err
	}
	return out, nil
}
