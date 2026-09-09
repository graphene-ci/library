package dockerlib

import (
	"context"
	"testing"
	"time"

	"github.com/docker/docker/api/types/container"
	"github.com/graphene-ci/pipeline/pkg/pipeline"
	"github.com/graphene-ci/temporal-entity/pkg/entity"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/testsuite"
)

// Test the production lifecycle, including the interrupted-init teardown path.
func TestContainerFinalize(t *testing.T) {
	for _, interrupted := range []bool{false, true} {
		t.Run(map[bool]string{false: "ready", true: "creating"}[interrupted], func(t *testing.T) {
			t.Parallel()
			var suite testsuite.WorkflowTestSuite
			env := suite.NewTestWorkflowEnvironment()
			require.NoError(t, containerDef().Register(env))
			env.RegisterActivityWithOptions(func(context.Context, Spec) (Info, error) { return Info{}, nil }, activity.RegisterOptions{Name: runActivityName})
			env.RegisterActivityWithOptions(func(context.Context, string) error { return nil }, activity.RegisterOptions{Name: removeActivityName})
			var runErr error
			if interrupted {
				runErr = activity.ErrResultPending
			}
			env.OnActivity(runActivityName, mock.Anything, mock.Anything).Return(Info{Id: "container-id"}, runErr).Once()
			env.OnActivity(removeActivityName, mock.Anything, "service").Return(nil).Once()
			env.RegisterDelayedCallback(func() { env.SignalWorkflow(entity.DeleteSignalName, nil) }, time.Second)
			env.ExecuteWorkflow(string(ContainerKind), map[string]any{"spec": containerSpec{Name: "service", Config: &container.Config{Image: "fixture"}, Owner: "run/test"}})
			require.NoError(t, env.GetWorkflowError())
			env.AssertExpectations(t)
		})
	}
}

func TestIndependentPreparationsRegisterDockerActivities(t *testing.T) {
	for _, name := range []string{"first", "second"} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			d, err := pipeline.Prepare("containers", func(ctx pipeline.Context, _ struct{}) (string, error) {
				a := pipeline.NewAgent(ctx, "agent")
				Container(ctx, a, Spec{Name: "service", Config: &container.Config{Image: "fixture"}})
				Container(ctx, a, Spec{Name: "other", Config: &container.Config{Image: "fixture"}})
				return "", nil
			})
			require.NoError(t, err)
			require.Contains(t, d.Activities(), runActivityName)
		})
	}
}
