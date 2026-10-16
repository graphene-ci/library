package k8slib

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/graphene-ci/temporal-entity/pkg/entity"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/testsuite"
)

// This exercises the actual entity workflow, independently of the pipeline
// simulator. Only calls to the external Kubernetes API are replaced.
func TestEntityApplyHealFinalize(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	entry := newKind("k8s..v1.ConfigMap", kindConfig{reconcileEvery: time.Second, pollInterval: time.Second})
	require.NoError(t, entry.def.Register(env))
	var mu sync.Mutex
	exists, applies, deletes := false, 0, 0
	env.RegisterActivityWithOptions(func(_ context.Context, req opRequest) error {
		require.True(t, req.InCluster)
		require.Empty(t, req.Kubeconfig.Name)
		mu.Lock()
		defer mu.Unlock()
		exists = true
		applies++
		return nil
	}, activity.RegisterOptions{Name: applyActivityName})
	env.RegisterActivityWithOptions(func(_ context.Context, req opRequest) (observation, error) {
		require.True(t, req.InCluster)
		require.Empty(t, req.Kubeconfig.Name)
		mu.Lock()
		defer mu.Unlock()
		return observation{Exists: exists, Manifest: req.Manifest}, nil
	}, activity.RegisterOptions{Name: observeActivityName})
	env.RegisterActivityWithOptions(func(_ context.Context, req opRequest) error {
		require.True(t, req.InCluster)
		require.Empty(t, req.Kubeconfig.Name)
		mu.Lock()
		defer mu.Unlock()
		exists = false
		deletes++
		return nil
	}, activity.RegisterOptions{Name: deleteActivityName})
	env.RegisterDelayedCallback(func() { mu.Lock(); exists = false; mu.Unlock() }, 1500*time.Millisecond)
	env.RegisterDelayedCallback(func() { env.SignalWorkflow(entity.DeleteSignalName, nil) }, 4*time.Second)
	env.ExecuteWorkflow("k8s..v1.ConfigMap", map[string]any{"spec": k8sSpec{InCluster: true, Manifest: map[string]any{"apiVersion": "v1", "kind": "ConfigMap", "metadata": map[string]any{"name": "config"}}, Owner: "run/test"}})
	require.NoError(t, env.GetWorkflowError())
	mu.Lock()
	defer mu.Unlock()
	require.False(t, exists)
	require.Equal(t, 2, applies)
	require.Equal(t, 1, deletes)
}

// The real entity: what the controller says reaches a person while the
// object converges (one note per change), and a broken object fails its
// init at once with the hook's error and the conditions as the reason.
func TestEntityReportsConditionsAndFailsOnTheHook(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	quota := map[string]any{"apiVersion": "v1", "kind": "ConfigMap", "metadata": map[string]any{"name": "vm"},
		"status": map[string]any{"conditions": []any{
			map[string]any{"type": "Synced", "status": "False", "reason": "ReconcileError", "message": "Quota limit compute.ssdDisks.size exceeded"},
		}}}
	observed := 0
	entry := newKind("k8s..v1.ConfigMap", kindConfig{pollInterval: time.Second, timeout: time.Hour,
		failed: func(live map[string]any) error {
			if observed < 3 {
				return nil
			}
			return errors.New("quota will not change")
		}})
	require.NoError(t, entry.def.Register(env))
	var notes []conditionsNote
	env.RegisterActivityWithOptions(func(context.Context, opRequest) error { return nil }, activity.RegisterOptions{Name: applyActivityName})
	env.RegisterActivityWithOptions(func(context.Context, opRequest) (observation, error) {
		observed++
		return observation{Exists: true, Manifest: quota}, nil
	}, activity.RegisterOptions{Name: observeActivityName})
	env.RegisterActivityWithOptions(func(_ context.Context, n conditionsNote) error { notes = append(notes, n); return nil },
		activity.RegisterOptions{Name: conditionsActivityName})
	env.RegisterActivityWithOptions(func(context.Context, opRequest) error { return nil }, activity.RegisterOptions{Name: deleteActivityName})
	env.ExecuteWorkflow("k8s..v1.ConfigMap", map[string]any{"spec": k8sSpec{InCluster: true, Manifest: quota, Owner: "run/test"}})
	err := env.GetWorkflowError()
	require.Error(t, err)
	require.ErrorContains(t, err, "failed: quota will not change")
	require.ErrorContains(t, err, "Synced=False (ReconcileError: Quota limit compute.ssdDisks.size exceeded)")
	require.Len(t, notes, 1, "one note per change of the conditions, not per poll")
	require.Equal(t, "Quota limit compute.ssdDisks.size exceeded", notes[0].Conditions[0].Message)
	require.Equal(t, 3, observed, "failed at the first observation the hook refused, not at the timeout")
}
