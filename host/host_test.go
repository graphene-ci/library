package host

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/graphene-ci/temporal-entity/pkg/entity"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"

	"github.com/graphene-ci/pipeline/pkg/pipeline"
	"github.com/graphene-ci/pipeline/pkg/wire"
)

func TestFileSafe(t *testing.T) {
	t.Parallel()
	cases := []struct{ in, want string }{
		{in: "host-sysctl/m-db", want: "host-sysctl-m-db"},
		{in: "/data/run 1/pg", want: "-data-run-1-pg"},
		{in: "node-exporter.service", want: "node-exporter.service"},
		{in: "", want: ""},
	}
	for _, c := range cases {
		t.Run(c.in, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, c.want, fileSafe(c.in))
		})
	}
}

func TestIndependentPreparationsRegisterHostKinds(t *testing.T) {
	for _, name := range []string{"first", "second"} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			d, err := pipeline.Prepare("host", func(ctx pipeline.Context, _ struct{}) (string, error) {
				a := pipeline.NewAgent(ctx, "db")
				Sysctl(ctx, a, "db", map[string]string{"vm.swappiness": "10"})
				Sysfs(ctx, a, "/sys/kernel/mm/transparent_hugepage/enabled", "never")
				Dir(ctx, a, "/data/pg", DirAccess{})
				Unit(ctx, a, "node-exporter.service", []byte("[Service]\n"))
				UnitState(ctx, a, "apt-daily.timer", UnitMasked)
				return "", nil
			})
			require.NoError(t, err)
			for _, act := range []string{"host.sysctl.apply", "host.sysfs.remove", "host.dir.apply", "host.unit.apply", "host.unit-state.remove"} {
				require.Contains(t, d.Activities(), act)
			}
		})
	}
}

// The shared record shape: Finalize restores, and an apply that fails
// part-way undoes what it got to before the record reports the failure.
func TestHostRecordLifecycle(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name     string
		applyErr error
		wantErr  bool
	}{
		{name: "applied then deleted"},
		{name: "failed apply rolls back", applyErr: temporal.NewNonRetryableApplicationError("held", ConflictCode, nil), wantErr: true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			k := sysctlKind()
			var suite testsuite.WorkflowTestSuite
			env := suite.NewTestWorkflowEnvironment()
			require.NoError(t, k.definition().Register(env))
			env.RegisterActivityWithOptions(func(context.Context, applyRequest[sysctlSpec]) (SysctlInfo, error) { return SysctlInfo{}, nil },
				activity.RegisterOptions{Name: k.apply})
			env.RegisterActivityWithOptions(func(context.Context, applyRequest[sysctlSpec]) error { return nil },
				activity.RegisterOptions{Name: k.remove})
			env.OnActivity(k.apply, mock.Anything, mock.Anything).Return(SysctlInfo{Prior: map[string]string{"vm.swappiness": "60"}}, c.applyErr).Once()
			env.OnActivity(k.remove, mock.Anything, mock.MatchedBy(func(r applyRequest[sysctlSpec]) bool {
				return r.Spec.Values["vm.swappiness"] == "10"
			})).Return(nil).Once()
			if c.applyErr == nil {
				env.RegisterDelayedCallback(func() { env.SignalWorkflow(entity.DeleteSignalName, nil) }, time.Second)
			}
			env.ExecuteWorkflow(string(SysctlKind), map[string]any{"spec": sysctlSpec{Values: map[string]string{"vm.swappiness": "10"}}})
			err := env.GetWorkflowError()
			require.Equal(t, c.wantErr, err != nil, "workflow error: %v", err)
			env.AssertExpectations(t)
		})
	}
}

// A removal that keeps failing gives up instead of wedging the cascade,
// and reports the leftover on its machine.
func TestHostRecordRemovalGivesUp(t *testing.T) {
	t.Parallel()
	k := sysctlKind()
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	require.NoError(t, k.definition().Register(env))
	env.RegisterActivityWithOptions(func(context.Context, applyRequest[sysctlSpec]) (SysctlInfo, error) { return SysctlInfo{}, nil },
		activity.RegisterOptions{Name: k.apply})
	env.RegisterActivityWithOptions(func(context.Context, applyRequest[sysctlSpec]) error { return errors.New("machine gone") },
		activity.RegisterOptions{Name: k.remove})
	reported := 0
	env.RegisterActivityWithOptions(func(_ context.Context, req wire.ReportLeakRequest) error {
		reported++
		require.Contains(t, req.Error, "machine gone")
		return nil
	}, activity.RegisterOptions{Name: wire.ReportLeakActivity})
	env.SetStartWorkflowOptions(client.StartWorkflowOptions{ID: "host-sysctl/db-db", TaskQueue: wire.AgentRunQueue("db", "run-1")})
	env.RegisterDelayedCallback(func() { env.SignalWorkflow(entity.DeleteSignalName, nil) }, time.Second)
	env.ExecuteWorkflow(string(SysctlKind), map[string]any{"spec": sysctlSpec{Values: map[string]string{"vm.swappiness": "10"}}})
	require.NoError(t, env.GetWorkflowError())
	require.Equal(t, 1, reported, "the leftover must be reported on the machine")
}

func BenchmarkFileSafe(b *testing.B) {
	for b.Loop() {
		_ = fileSafe("host-dir/agent-1-/data/run-42/ydb/pdisk")
	}
}
