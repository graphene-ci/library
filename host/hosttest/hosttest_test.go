package hosttest_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/testsuite"

	hostlib "github.com/graphene-ci/library/host"
	"github.com/graphene-ci/library/host/hosttest"
	"github.com/graphene-ci/pipeline/pkg/pipeline"
	"github.com/graphene-ci/pipeline/pkg/pipelinetest"
	"github.com/graphene-ci/pipeline/pkg/ref"
)

// Host settings declared on our agent are records under it: the run's
// cascade takes them down with the agent and nothing leaks.
func TestHostSettingsDieWithTheirAgent(t *testing.T) {
	t.Parallel()
	var suite testsuite.WorkflowTestSuite
	w := pipelinetest.Install(t, suite.NewTestWorkflowEnvironment())
	hosttest.Install(w)
	w.ConnectAfter("db", 0)
	wf := pipelinetest.Workflow(w, "prep", func(ctx pipeline.Context, _ struct{}) (string, error) {
		a := pipeline.NewAgent(ctx, "db")
		hostlib.Sysctl(ctx, a, "db", map[string]string{"vm.swappiness": "10"}).Ready(ctx)
		hostlib.Sysfs(ctx, a, "/sys/kernel/mm/transparent_hugepage/enabled", "never").Ready(ctx)
		hostlib.UnitState(ctx, a, "apt-daily.timer", hostlib.UnitMasked).Ready(ctx)
		hostlib.Unit(ctx, a, "node-exporter.service", []byte("[Service]\n")).Ready(ctx)
		return hostlib.Dir(ctx, a, "/data/run/pg", hostlib.DirAccess{}).Ready(ctx).Path, nil
	})
	w.Env.ExecuteWorkflow(wf, struct{}{})
	require.NoError(t, w.Env.GetWorkflowError())
	var dir string
	require.NoError(t, w.Env.GetWorkflowResult(&dir))
	require.Equal(t, "/data/run/pg", dir)

	for _, name := range []ref.OwnerRef{
		"host-sysctl/db-db",
		"host-sysfs/db-sys-kernel-mm-transparent-hugepage-enabled",
		"host-unit-state/db-apt-daily.timer",
		"host-unit/db-node-exporter.service",
		"host-dir/db-data-run-pg",
	} {
		r, ok := w.Resource(name)
		require.True(t, ok, "missing %s", name)
		require.Equal(t, "deleted", r.Phase, "%s must die with the agent", name)
	}
	w.AssertNoLeaks(t)
}

// An invalid declaration fails at the handle, before any work is sent.
func TestInvalidDeclarationFails(t *testing.T) {
	t.Parallel()
	var suite testsuite.WorkflowTestSuite
	w := pipelinetest.Install(t, suite.NewTestWorkflowEnvironment())
	hosttest.Install(w)
	w.ConnectAfter("db", 0)
	wf := pipelinetest.Workflow(w, "bad", func(ctx pipeline.Context, _ struct{}) (string, error) {
		a := pipeline.NewAgent(ctx, "db")
		_, err := hostlib.Dir(ctx, a, "relative/path", hostlib.DirAccess{}).TryReady(ctx)
		if err == nil {
			return "", nil
		}
		return err.Error(), nil
	})
	w.Env.ExecuteWorkflow(wf, struct{}{})
	require.NoError(t, w.Env.GetWorkflowError())
	var msg string
	require.NoError(t, w.Env.GetWorkflowResult(&msg))
	require.Contains(t, msg, "clean absolute path")
}
