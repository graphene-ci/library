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
	observed, gone := 0, false
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
		if gone {
			return observation{}, nil
		}
		observed++
		return observation{Exists: true, Manifest: quota}, nil
	}, activity.RegisterOptions{Name: observeActivityName})
	env.RegisterActivityWithOptions(func(_ context.Context, n conditionsNote) error { notes = append(notes, n); return nil },
		activity.RegisterOptions{Name: conditionsActivityName})
	env.RegisterActivityWithOptions(func(context.Context, opRequest) error { gone = true; return nil }, activity.RegisterOptions{Name: deleteActivityName})
	env.ExecuteWorkflow("k8s..v1.ConfigMap", map[string]any{"spec": k8sSpec{InCluster: true, Manifest: quota, Owner: "run/test"}})
	err := env.GetWorkflowError()
	require.Error(t, err)
	require.ErrorContains(t, err, "failed: quota will not change")
	require.ErrorContains(t, err, "Synced=False (ReconcileError: Quota limit compute.ssdDisks.size exceeded)")
	require.Len(t, notes, 1, "one note per change of the conditions, not per poll")
	require.Equal(t, "Quota limit compute.ssdDisks.size exceeded", notes[0].Conditions[0].Message)
	require.Equal(t, 3, observed, "failed at the first observation the hook refused, not at the timeout")
}

// cluster is a fake API server with a controller that keeps creating
// what was applied until it is deleted — Crossplane against a quota.
type cluster struct {
	mu                  sync.Mutex
	exists              bool
	applies, deletes    int
	lingers             int // observations an object survives its delete
	stuck               bool
	notes               []conditionsNote
	afterFailureApplies int
	failed              bool
	refused             map[string]any
}

func (c *cluster) register(env *testsuite.TestWorkflowEnvironment) {
	env.RegisterActivityWithOptions(func(context.Context, opRequest) error {
		c.mu.Lock()
		defer c.mu.Unlock()
		c.exists = true
		c.applies++
		if c.failed {
			c.afterFailureApplies++
		}
		return nil
	}, activity.RegisterOptions{Name: applyActivityName})
	env.RegisterActivityWithOptions(func(context.Context, opRequest) (observation, error) {
		c.mu.Lock()
		defer c.mu.Unlock()
		if c.deletes > 0 && !c.stuck {
			if c.lingers > 0 {
				c.lingers--
			} else {
				c.exists = false
			}
		}
		if !c.exists {
			return observation{}, nil
		}
		return observation{Exists: true, Manifest: c.refused}, nil
	}, activity.RegisterOptions{Name: observeActivityName})
	env.RegisterActivityWithOptions(func(context.Context, opRequest) error {
		c.mu.Lock()
		defer c.mu.Unlock()
		c.deletes++
		c.failed = true
		return nil
	}, activity.RegisterOptions{Name: deleteActivityName})
	env.RegisterActivityWithOptions(func(_ context.Context, n conditionsNote) error {
		c.mu.Lock()
		defer c.mu.Unlock()
		c.notes = append(c.notes, n)
		return nil
	}, activity.RegisterOptions{Name: conditionsActivityName})
}

func refusedNetwork() map[string]any {
	return map[string]any{"apiVersion": "v1", "kind": "ConfigMap", "metadata": map[string]any{"name": "net"},
		"status": map[string]any{"conditions": []any{
			map[string]any{"type": "Synced", "status": "False", "reason": "ReconcileError", "message": "Quota limit vpc.networks.count exceeded"},
		}}}
}

// A declaration the cloud refuses takes its object out of the cluster
// before the record closes, and waits for it to be gone: the controller
// cannot finish creating it an hour later, when the quota frees up,
// for a record that no longer exists. Nothing applies it again.
func TestFailedDeclarationWithdrawsItsObject(t *testing.T) {
	for name, cfg := range map[string]kindConfig{
		"failed hook": {pollInterval: time.Second, timeout: time.Hour,
			failed: func(map[string]any) error { return errors.New("quota will not change") }},
		"timeout": {pollInterval: time.Second, timeout: 5 * time.Second},
	} {
		t.Run(name, func(t *testing.T) {
			var suite testsuite.WorkflowTestSuite
			env := suite.NewTestWorkflowEnvironment()
			entry := newKind("k8s..v1.ConfigMap", cfg)
			require.NoError(t, entry.def.Register(env))
			c := &cluster{refused: refusedNetwork(), lingers: 3}
			c.register(env)
			env.ExecuteWorkflow("k8s..v1.ConfigMap", map[string]any{"spec": k8sSpec{InCluster: true, Manifest: c.refused, Owner: "run/test"}})
			require.Error(t, env.GetWorkflowError(), "the declaration still fails")
			c.mu.Lock()
			defer c.mu.Unlock()
			require.False(t, c.exists, "the object is gone from the cluster")
			require.Equal(t, 1, c.deletes)
			require.Zero(t, c.afterFailureApplies, "nothing re-creates it")
			for _, n := range c.notes {
				require.Empty(t, n.Note, "no orphan to report")
			}
		})
	}
}

// An object that will not go away within the bound is said aloud on the
// record — not left silently behind.
func TestFailedDeclarationSaysWhatItLeft(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	entry := newKind("k8s..v1.ConfigMap", kindConfig{pollInterval: time.Second, timeout: time.Hour,
		failed: func(map[string]any) error { return errors.New("quota will not change") }})
	require.NoError(t, entry.def.Register(env))
	c := &cluster{refused: refusedNetwork(), stuck: true}
	c.register(env)
	env.ExecuteWorkflow("k8s..v1.ConfigMap", map[string]any{"spec": k8sSpec{InCluster: true, Manifest: c.refused, Owner: "run/test"}})
	err := env.GetWorkflowError()
	require.ErrorContains(t, err, "failed: quota will not change", "the reason stays the declaration's own")
	c.mu.Lock()
	defer c.mu.Unlock()
	last := c.notes[len(c.notes)-1]
	require.Contains(t, last.Note, "still in the cluster")
	require.Contains(t, last.Note, "still present after 10m0s")
}

// An init cut by deletion is the finalizer's: withdraw does not run twice.
func TestDeletionDuringInitIsTheFinalizers(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	entry := newKind("k8s..v1.ConfigMap", kindConfig{pollInterval: time.Second, timeout: time.Hour})
	require.NoError(t, entry.def.Register(env))
	c := &cluster{refused: refusedNetwork()}
	c.register(env)
	env.RegisterDelayedCallback(func() { env.SignalWorkflow(entity.DeleteSignalName, nil) }, 3*time.Second)
	env.ExecuteWorkflow("k8s..v1.ConfigMap", map[string]any{"spec": k8sSpec{InCluster: true, Manifest: c.refused, Owner: "run/test"}})
	require.NoError(t, env.GetWorkflowError())
	c.mu.Lock()
	defer c.mu.Unlock()
	require.False(t, c.exists)
	require.Equal(t, 1, c.deletes, "one delete: the finalizer's")
}
