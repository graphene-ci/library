package host

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/graphene-ci/temporal-entity/pkg/entity"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"

	"github.com/graphene-ci/library/host/internal/contract"
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

// redeclareClient is a Temporal client holding one existing record with
// a stored spec: create attaches, describe answers with that spec.
type redeclareClient struct {
	client.Client
	stored sysctlSpec
}

func (c redeclareClient) ExecuteWorkflow(context.Context, client.StartWorkflowOptions, any, ...any) (client.WorkflowRun, error) {
	return nil, nil
}

func (c redeclareClient) QueryWorkflow(context.Context, string, string, string, ...any) (converter.EncodedValue, error) {
	return describeOf{spec: c.stored}, nil
}

type describeOf struct{ spec sysctlSpec }

func (describeOf) HasValue() bool { return true }

func (d describeOf) Get(ptr any) error {
	raw, err := json.Marshal(map[string]any{"phase": entity.PhaseReady, "spec": d.spec, "state": map[string]any{"info": SysctlInfo{}}})
	if err != nil {
		return err
	}
	return json.Unmarshal(raw, ptr)
}

// Declaring a name that exists attaches only with the same spec: another
// value would silently not apply, so it is refused.
func TestRedeclare(t *testing.T) {
	t.Parallel()
	stored := sysctlSpec{Tree: contract.Tree{Owner: "stand/lab"}, Values: map[string]string{"vm.swappiness": "10"}}
	cases := []struct {
		name     string
		values   map[string]string
		mismatch bool
	}{
		{name: "same values, another owner", values: map[string]string{"vm.swappiness": "10"}},
		{name: "another value", values: map[string]string{"vm.swappiness": "1"}, mismatch: true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			k := sysctlKind()
			declare := k.declarer(k.definition())
			raw, err := json.Marshal(sysctlSpec{Tree: contract.Tree{Owner: "run/r2"}, Values: c.values})
			require.NoError(t, err)
			_, err = declare(context.Background(), redeclareClient{stored: stored}, "agent/db/run/r2",
				contract.DeclareRequest{Kind: contract.SysctlKind, Name: "db-db", Spec: raw}, nil)
			var app *temporal.ApplicationError
			if c.mismatch {
				require.True(t, errors.As(err, &app) && app.Type() == pipeline.CodeSpecMismatch, "err = %v", err)
				return
			}
			require.NoError(t, err)
		})
	}
}
