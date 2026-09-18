package dockertest_test

import (
	"testing"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/volume"
	dockerlib "github.com/graphene-ci/library/docker"
	"github.com/graphene-ci/library/docker/dockertest"
	pa "github.com/graphene-ci/pipeline/pkg/activity"
	"github.com/graphene-ci/pipeline/pkg/pipeline"
	"github.com/graphene-ci/pipeline/pkg/pipelinetest"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/testsuite"
)

func TestInstallVolumeAndNetwork(t *testing.T) {
	t.Parallel()
	var suite testsuite.WorkflowTestSuite
	w := pipelinetest.Install(t, suite.NewTestWorkflowEnvironment())
	dockertest.Install(w, "28.5.2")
	w.ConnectAfter("machine", 0)
	type result struct {
		First, Second dockerlib.InstallReport
		Volume        dockerlib.VolumeInfo
		Network       dockerlib.NetworkInfo
	}
	wf := pipelinetest.Workflow(w, "docker", func(ctx pipeline.Context, _ struct{}) (result, error) {
		a := pipeline.NewAgent(ctx, "machine")
		first, err := pa.Activity(ctx, a, dockerlib.Install())
		if err != nil {
			return result{}, err
		}
		second, err := pa.Activity(ctx, a, dockerlib.Install())
		if err != nil {
			return result{}, err
		}
		v := dockerlib.Volume(ctx, a, volume.CreateOptions{Name: "data", Labels: map[string]string{"purpose": "fixture"}})
		n := dockerlib.Network(ctx, a, dockerlib.NetworkSpec{Name: "network"})
		out := result{First: first, Second: second, Volume: v.Ready(ctx), Network: n.Ready(ctx)}
		pipeline.ToStand(ctx, a)
		return out, nil
	})
	w.Env.ExecuteWorkflow(wf, struct{}{})
	require.NoError(t, w.Env.GetWorkflowError())
	var out result
	require.NoError(t, w.Env.GetWorkflowResult(&out))
	require.True(t, out.First.Installed)
	require.False(t, out.Second.Installed)
	require.Equal(t, out.First.Version, out.Second.Version)
	require.Equal(t, "data", out.Volume.Name)
	require.Equal(t, "network", out.Network.Name)
	w.AssertOwner(t, "docker-volume/data", "agent/machine")
	w.AssertOwner(t, "docker-network/network", "agent/machine")
	w.AssertOwner(t, "agent/machine", "stand/docker")
	w.AssertNoLeaks(t)
}

// Two jobs on one agent are one activity type: OnJob tells them apart by name.
func TestOnJobPicksByName(t *testing.T) {
	t.Parallel()
	var suite testsuite.WorkflowTestSuite
	w := pipelinetest.Install(t, suite.NewTestWorkflowEnvironment())
	dockertest.Install(w, "28.5.2")
	w.ConnectAfter("machine", 0)
	job := func(name string) pa.Call[dockerlib.JobReport] {
		return dockerlib.Job(dockerlib.JobSpec{Name: name, Config: &container.Config{Image: "fixture"}})
	}
	wf := pipelinetest.Workflow(w, "jobs", func(ctx pipeline.Context, _ struct{}) ([]dockerlib.JobReport, error) {
		a := pipeline.NewAgent(ctx, "machine")
		first, err := pa.Activity(ctx, a, job("first"))
		if err != nil {
			return nil, err
		}
		second, err := pa.Activity(ctx, a, job("second"))
		return []dockerlib.JobReport{first, second}, err
	})
	dockertest.OnJob(w, "machine", "second").Return(dockerlib.JobReport{ExitCode: 3, Tail: "boom"}, nil).Once()
	dockertest.OnJob(w, "machine", "first").Return(dockerlib.JobReport{Stdout: "42\n"}, nil).Once()

	w.Env.ExecuteWorkflow(wf, struct{}{})
	require.NoError(t, w.Env.GetWorkflowError())
	var out []dockerlib.JobReport
	require.NoError(t, w.Env.GetWorkflowResult(&out))
	require.Equal(t, "42\n", out[0].Stdout)
	require.Equal(t, 3, out[1].ExitCode, "a non-zero exit is an outcome, not an activity failure")
	w.Env.AssertExpectations(t)
	w.AssertNoLeaks(t)
}
