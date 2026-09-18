package filetest_test

import (
	"encoding/json"
	"testing"

	filelib "github.com/graphene-ci/library/file"
	"github.com/graphene-ci/library/file/filetest"
	"github.com/graphene-ci/pipeline/pkg/artifact"
	"github.com/graphene-ci/pipeline/pkg/file"
	"github.com/graphene-ci/pipeline/pkg/pipeline"
	"github.com/graphene-ci/pipeline/pkg/pipelinetest"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/testsuite"
)

// A declared file is a record under its agent, and its bytes are on the
// simulated machine — an artifact read from the same path gets them back.
func TestFileIsDeclaredAndReadable(t *testing.T) {
	t.Parallel()
	var suite testsuite.WorkflowTestSuite
	w := pipelinetest.Install(t, suite.NewTestWorkflowEnvironment())
	filetest.Install(w)
	w.ConnectAfter("machine", 0)
	wf := pipelinetest.Workflow(w, "files", func(ctx pipeline.Context, _ struct{}) (string, error) {
		a := pipeline.NewAgent(ctx, "machine")
		cfg := filelib.File(ctx, a, "/opt/app/config.json", file.FromBytes([]byte(`{"ok":true}`)))
		path := cfg.Ready(ctx).Path
		copyBack := pipeline.NewArtifact(ctx, "config-copy", artifact.FromAgentFile(a, path))
		pipeline.ToStand(ctx, copyBack)
		return path, nil
	})
	w.Env.ExecuteWorkflow(wf, struct{}{})
	require.NoError(t, w.Env.GetWorkflowError())
	var path string
	require.NoError(t, w.Env.GetWorkflowResult(&path))
	require.Equal(t, "/opt/app/config.json", path)

	var state pipeline.ArtifactState
	record, ok := w.Resource("artifact/config-copy")
	require.True(t, ok)
	require.NoError(t, json.Unmarshal(record.State, &state))
	content, ok := w.Blob(state.Blob)
	require.True(t, ok)
	require.JSONEq(t, `{"ok":true}`, string(content))

	// Owned by the run's agent: the file died with it.
	f, ok := w.Resource("file/machine-opt-app-config.json")
	require.True(t, ok)
	require.Equal(t, "deleted", f.Phase)
	w.AssertNoLeaks(t)
}
