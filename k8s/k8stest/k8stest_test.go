package k8stest_test

import (
	"errors"
	"fmt"
	"github.com/graphene-ci/pipeline/pkg/id"
	"strings"
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

// A test that does not know which objects a run will declare answers them
// all with one default; a fixture set by name still wins.
func TestSetDefaultAnswersUnnamedObjects(t *testing.T) {
	t.Parallel()
	var suite testsuite.WorkflowTestSuite
	w := pipelinetest.Install(t, suite.NewTestWorkflowEnvironment())
	objects := k8stest.Install(w)
	var seen []string
	objects.SetDefault(func(kind, name string, manifest map[string]any) any {
		seen = append(seen, kind+"/"+name)
		require.Equal(t, "ConfigMap", manifest["kind"], "the default sees the declared manifest")
		return map[string]any{"status": map[string]any{"phase": "default"}}
	})
	require.NoError(t, objects.Set("k8s..v1.ConfigMap/named", map[string]any{"status": map[string]any{"phase": "named"}}))
	wf := pipelinetest.Workflow(w, "defaults", func(ctx pipeline.Context, _ struct{}) ([]string, error) {
		client := k8slib.NewClientFromSecret(pipeline.UseSecret("cluster"))
		ready := func(live *unstructured.Unstructured) bool {
			phase, _, _ := unstructured.NestedString(live.Object, "status", "phase")
			return phase != ""
		}
		var phases []string
		for _, name := range []string{"named", "unnamed"} {
			object := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "v1", "kind": "ConfigMap"}}
			live := k8slib.Resource(ctx, client, name, object,
				k8slib.WithTimeout[unstructured.Unstructured](5*time.Second), k8slib.WithReady(ready)).Ready(ctx)
			if ctx.Recording() {
				continue
			}
			phase, _, _ := unstructured.NestedString(live.Object, "status", "phase")
			phases = append(phases, phase)
		}
		return phases, nil
	})
	w.Env.ExecuteWorkflow(wf, struct{}{})
	require.NoError(t, w.Env.GetWorkflowError())
	var got []string
	require.NoError(t, w.Env.GetWorkflowResult(&got))
	require.Equal(t, []string{"named", "default"}, got)
	require.Equal(t, []string{"k8s..v1.ConfigMap/unnamed"}, seen, "the default is asked only for objects without a fixture")
	w.AssertNoLeaks(t)
}

// A cloud's refusal reaches the failure: the last observed conditions are
// in the timeout's text, and WithFailed ends the declaration at once with
// the hook's own error — no timeout waited out.
func TestFailureCarriesTheControllersReason(t *testing.T) {
	quota := map[string]any{"status": map[string]any{"conditions": []any{
		map[string]any{"type": "Synced", "status": "False", "reason": "ReconcileError", "message": "Quota limit compute.ssdDisks.size exceeded"},
		map[string]any{"type": "Ready", "status": "False", "reason": "Creating"},
	}}}
	for _, tc := range []struct {
		name   string
		failed bool
		want   []string
	}{
		{"timeout", false, []string{"did not become ready within 3s", "Synced=False (ReconcileError: Quota limit compute.ssdDisks.size exceeded)", "Ready=False (Creating)"}},
		{"failed hook", true, []string{"failed: quota exceeded", "Quota limit compute.ssdDisks.size exceeded"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var suite testsuite.WorkflowTestSuite
			w := pipelinetest.Install(t, suite.NewTestWorkflowEnvironment())
			objects := k8stest.Install(w)
			require.NoError(t, objects.Set("k8s..v1.ConfigMap/config", quota))
			wf := pipelinetest.Workflow(w, id.PipelineId("refused-"+strings.ReplaceAll(tc.name, " ", "-")), func(ctx pipeline.Context, _ struct{}) (string, error) {
				client := k8slib.NewClientFromSecret(pipeline.UseSecret("cluster"))
				object := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "v1", "kind": "ConfigMap"}}
				opts := []k8slib.Option[unstructured.Unstructured]{
					k8slib.WithPollInterval[unstructured.Unstructured](time.Second),
					k8slib.WithTimeout[unstructured.Unstructured](3 * time.Second),
					k8slib.WithReady(func(_ *unstructured.Unstructured) bool { return false }),
				}
				if tc.failed {
					opts = append(opts, k8slib.WithFailed(func(live *unstructured.Unstructured) error {
						conds, _, _ := unstructured.NestedSlice(live.Object, "status", "conditions")
						for _, c := range conds {
							if m := c.(map[string]any); strings.Contains(fmt.Sprint(m["message"]), "Quota") {
								return errors.New("quota exceeded")
							}
						}
						return nil
					}))
				}
				_ = k8slib.Resource(ctx, client, "config", object, opts...).Ready(ctx)
				return "", nil
			})
			started := w.Env.Now()
			w.Env.ExecuteWorkflow(wf, struct{}{})
			err := w.Env.GetWorkflowError()
			require.Error(t, err)
			for _, want := range tc.want {
				require.ErrorContains(t, err, want)
			}
			if tc.failed {
				require.Less(t, w.Env.Now().Sub(started), 3*time.Second, "a broken object fails at once")
			}
		})
	}
}
