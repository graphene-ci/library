package dockerlib

import (
	"context"
	"net"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/docker/docker/api/types/container"
	"github.com/stretchr/testify/require"
	collogspb "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	"google.golang.org/grpc"

	"github.com/graphene-ci/pipeline/pkg/obs"
)

// door is the server's logs intake as the executor's receiver sees it.
type door struct {
	collogspb.UnimplementedLogsServiceServer
	mu   sync.Mutex
	runs []string
}

func (d *door) Export(_ context.Context, req *collogspb.ExportLogsServiceRequest) (*collogspb.ExportLogsServiceResponse, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	for _, rl := range req.GetResourceLogs() {
		for _, kv := range rl.GetResource().GetAttributes() {
			if kv.GetKey() == obs.AttrRun {
				d.runs = append(d.runs, kv.GetValue().GetStringValue())
			}
		}
	}
	return &collogspb.ExportLogsServiceResponse{}, nil
}

// The real daemon, opt-in: GRAPHENE_DOCKER_IT=1. A job container told
// nothing but what the library put in its environment reaches the
// executor's intake through the docker bridge, and what arrives at the
// door is stamped with the run.
func TestTelemetryFromContainerAgainstDaemon(t *testing.T) {
	if os.Getenv("GRAPHENE_DOCKER_IT") == "" {
		t.Skip("set GRAPHENE_DOCKER_IT=1 to run against the local docker daemon")
	}
	img := os.Getenv("GRAPHENE_DOCKER_IT_IMAGE")
	if img == "" {
		img = "busybox"
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	d := &door{}
	gs := grpc.NewServer()
	collogspb.RegisterLogsServiceServer(gs, d)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	go func() { _ = gs.Serve(ln) }()
	defer gs.Stop()

	intake, err := obs.StartReceiver(ctx, obs.Config{
		Endpoint: ln.Addr().String(), Token: "t", Insecure: true,
		Namespace: "it", RunId: "it-run", AgentId: "local", Role: "machine",
	})
	require.NoError(t, err)
	defer func() { _ = intake.Close(context.Background()) }()
	if intake.Bridge == "" {
		t.Skip("no docker bridge on this machine")
	}

	report, err := jobActivity(ctx, JobSpec{
		Name: "graphene-telemetry-it",
		Config: &container.Config{
			Image: img,
			Cmd: []string{"sh", "-c", `echo "endpoint=$OTEL_EXPORTER_OTLP_ENDPOINT"; ` +
				`wget -q -O - --header 'Content-Type: application/json' ` +
				`--post-data '{"resourceLogs":[{"resource":{"attributes":[{"key":"graphene.run","value":{"stringValue":"forged"}}]},"scopeLogs":[{"logRecords":[{"body":{"stringValue":"hi"}}]}]}]}' ` +
				`"$OTEL_EXPORTER_OTLP_ENDPOINT/v1/logs"`},
		},
	})
	require.NoError(t, err)
	require.Equal(t, 0, report.ExitCode, report.Tail)
	require.True(t, strings.HasPrefix(report.Stdout, "endpoint=http://"+intake.Bridge), report.Stdout)

	require.Eventually(t, func() bool { d.mu.Lock(); defer d.mu.Unlock(); return len(d.runs) == 1 }, 10*time.Second, 50*time.Millisecond)
	require.Equal(t, []string{"it-run"}, d.runs, "the run the container claimed is overwritten by the executor's")
}
