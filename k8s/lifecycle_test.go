package k8slib

import (
	"context"
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
