package k8slib

import (
	"fmt"
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"

	"github.com/graphene-ci/pipeline/pkg/pipeline"
	"github.com/graphene-ci/pipeline/pkg/ref"
)

// Resource declares one Kubernetes object as a graphene Resource: apply
// (server-side, idempotent) and converge until Ready. obj is ANY typed
// Kubernetes object — a k8s.io/api type, a third-party CRD type — or an
// *unstructured.Unstructured; the args structs stay exactly what their
// authors made them.
//
// TODO(tree): the durable record and its place in the ownership tree
// land with the server-side tree support; until then the resource lives
// as the run's converging future and the options are carried, not yet
// enforced server-side.
func (c *Client) Resource(ctx pipeline.Context, obj any, opts ...pipeline.ResourceOption) pipeline.Resource[Object] {
	manifest, name, err := toManifest(obj)
	self := ref.OwnerRef("k8s/" + name)
	if ctx.Recording() {
		ctx.RecordActivity(applyActivityName, applyActivity)
		ctx.RecordActivity(observeActivityName, observeActivity)
		ctx.RecordActivity(deleteActivityName, deleteActivity)
		return pipeline.NewResource[Object](ctx, self, nil)
	}
	o := pipeline.BuildResourceOptions(ctx, opts)
	_ = o // carried into the record when the server tree lands
	fut, set := workflow.NewFuture(ctx)
	if err != nil {
		set.SetError(fmt.Errorf("k8s resource: %w", err))
		return pipeline.NewResource[Object](ctx, self, fut)
	}
	req := request{Kubeconfig: c.kubeconfig, Manifest: manifest}
	workflow.Go(ctx, func(gctx workflow.Context) {
		actx := workflow.WithActivityOptions(gctx, workflow.ActivityOptions{
			// The run queue is the workflow's own queue: the kubeconfig
			// resolves inside these activities, on the run's worker.
			StartToCloseTimeout: 5 * time.Minute,
			RetryPolicy: &temporal.RetryPolicy{
				InitialInterval:    time.Second,
				BackoffCoefficient: 2,
				MaximumInterval:    time.Minute,
			},
		})
		if err := workflow.ExecuteActivity(actx, applyActivityName, req).Get(gctx, nil); err != nil {
			set.SetError(fmt.Errorf("apply %s: %w", name, err))
			return
		}
		for {
			var out observation
			if err := workflow.ExecuteActivity(actx, observeActivityName, req).Get(gctx, &out); err != nil {
				set.SetError(fmt.Errorf("observe %s: %w", name, err))
				return
			}
			if out.Ready {
				set.Set(Object{Id: out.Id, Manifest: out.Manifest}, nil)
				return
			}
			if err := workflow.Sleep(gctx, 5*time.Second); err != nil {
				set.SetError(err)
				return
			}
		}
	})
	return pipeline.NewResource[Object](ctx, self, fut)
}

// toManifest flattens any typed object into an unstructured manifest.
func toManifest(obj any) (map[string]any, string, error) {
	var m map[string]any
	switch v := obj.(type) {
	case *unstructured.Unstructured:
		m = v.Object
	case map[string]any:
		m = v
	default:
		converted, err := runtime.DefaultUnstructuredConverter.ToUnstructured(obj)
		if err != nil {
			return nil, "", fmt.Errorf("convert %T: %w", obj, err)
		}
		m = converted
	}
	u := unstructured.Unstructured{Object: m}
	name := u.GetName()
	if name == "" {
		return nil, "", fmt.Errorf("object %T has no metadata.name", obj)
	}
	return m, u.GetKind() + "/" + name, nil
}
