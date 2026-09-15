package dockerlib

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/graphene-ci/pipeline/pkg/wire"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/testsuite"
)

// Even an already installed engine can lose the capability-publication RPC.
// The activity must announce liveness before that work, so a lost completion
// is recoverable through the heartbeat timeout instead of the install timeout.
func TestInstallHeartbeatsBeforeExistingEngineProbe(t *testing.T) {
	beat := make(chan struct{})
	var once sync.Once
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		select {
		case <-beat:
		case <-time.After(time.Second):
			t.Error("engine contacted without an installation heartbeat")
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"Version":"29.8.1","ApiVersion":"1.44"}`))
	}))
	defer server.Close()
	t.Setenv("DOCKER_HOST", "tcp://"+strings.TrimPrefix(server.URL, "http://"))
	t.Setenv("DOCKER_API_VERSION", "1.44")
	t.Setenv("DOCKER_TLS_VERIFY", "")
	t.Setenv(wire.EnvAgentId, "")
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestActivityEnvironment()
	env.SetTestTimeout(5 * time.Second)
	env.SetOnActivityHeartbeatListener(func(_ *activity.Info, _ converter.EncodedValues) {
		once.Do(func() { close(beat) })
	})
	env.RegisterActivity(install)
	_, err := env.ExecuteActivity(install)
	if err == nil || !strings.Contains(err.Error(), wire.EnvAgentId) {
		t.Fatalf("capability publication failure lost: %v", err)
	}
}
