package k8stest_test

import (
	"testing"
	"time"

	k8slib "github.com/graphene-ci/library/k8s"
	"github.com/graphene-ci/library/k8s/k8stest"
	"github.com/graphene-ci/pipeline/pkg/pipeline"
	"github.com/graphene-ci/pipeline/pkg/pipelinetest"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/testsuite"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

func TestReadinessIsScopedToPreparation(t *testing.T) {
	for _, wanted := range []string{"first", "second"} {
		t.Run(wanted, func(t *testing.T) {
			t.Parallel()
			var suite testsuite.WorkflowTestSuite
			w := pipelinetest.Install(t, suite.NewTestWorkflowEnvironment())
			objects := k8stest.Install(w)
			require.NoError(t, objects.Set("k8s..v1.ConfigMap/config", map[string]any{"status": map[string]any{"phase": "pending"}}))
			require.NoError(t, objects.After(2*time.Second, "k8s..v1.ConfigMap/config", map[string]any{"status": map[string]any{"phase": wanted}}))
			wf := pipelinetest.Workflow(w, "readiness", func(ctx pipeline.Context, _ struct{}) (string, error) {
				client := k8slib.NewClientFromSecret(pipeline.UseSecret("cluster"))
				object := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "v1", "kind": "ConfigMap"}}
				r := k8slib.Resource(ctx, client, "config", object,
					k8slib.WithPollInterval[unstructured.Unstructured](time.Second),
					k8slib.WithTimeout[unstructured.Unstructured](5*time.Second),
					k8slib.WithReady(func(live *unstructured.Unstructured) bool {
						phase, _, _ := unstructured.NestedString(live.Object, "status", "phase")
						return phase == wanted
					}))
				live := r.Ready(ctx)
				if ctx.Recording() {
					return "", nil
				}
				phase, _, err := unstructured.NestedString(live.Object, "status", "phase")
				return phase, err
			})
			start := w.Env.Now()
			w.Env.ExecuteWorkflow(wf, struct{}{})
			require.NoError(t, w.Env.GetWorkflowError())
			var got string
			require.NoError(t, w.Env.GetWorkflowResult(&got))
			require.Equal(t, wanted, got)
			require.GreaterOrEqual(t, w.Env.Now().Sub(start), 2*time.Second)
			w.AssertNoLeaks(t)
		})
	}
}

func TestUnsatisfiedPredicateTimesOut(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	w := pipelinetest.Install(t, suite.NewTestWorkflowEnvironment())
	objects := k8stest.Install(w)
	require.NoError(t, objects.Set("k8s..v1.ConfigMap/config", map[string]any{"status": map[string]any{"phase": "pending"}}))
	wf := pipelinetest.Workflow(w, "notready", func(ctx pipeline.Context, _ struct{}) (string, error) {
		client := k8slib.NewClientFromSecret(pipeline.UseSecret("cluster"))
		object := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "v1", "kind": "ConfigMap"}}
		r := k8slib.Resource(ctx, client, "config", object,
			k8slib.WithPollInterval[unstructured.Unstructured](time.Second),
			k8slib.WithTimeout[unstructured.Unstructured](3*time.Second),
			k8slib.WithReady(func(_ *unstructured.Unstructured) bool { return false }))
		_ = r.Ready(ctx)
		return "", nil
	})
	w.Env.ExecuteWorkflow(wf, struct{}{})
	require.ErrorContains(t, w.Env.GetWorkflowError(), "did not become ready within 3s")
	w.AssertNoLeaks(t)
}
