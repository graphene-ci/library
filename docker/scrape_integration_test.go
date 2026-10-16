package dockerlib

import (
	"context"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/docker/docker/api/types/container"
	dockerclient "github.com/docker/docker/client"
	"github.com/docker/go-connections/nat"
	"github.com/stretchr/testify/require"

	"github.com/graphene-ci/pipeline/pkg/obs"
)

// The real daemon, opt-in: GRAPHENE_DOCKER_IT=1. A container serving a
// histogram is scraped from its start at the spec's step, its buckets
// shipped as _bucket/_sum/_count; the drain takes a last reading and lets
// the scraper go.
func TestScrapeAgainstDaemon(t *testing.T) {
	if os.Getenv("GRAPHENE_DOCKER_IT") == "" {
		t.Skip("set GRAPHENE_DOCKER_IT=1 to run against the local docker daemon")
	}
	var mu sync.Mutex
	var at []time.Time
	var last []string
	prev := shipSeries
	shipSeries = func(_ context.Context, series ...obs.Series) error {
		mu.Lock()
		defer mu.Unlock()
		at = append(at, time.Now())
		last = names(series)
		return nil
	}
	t.Cleanup(func() { shipSeries = prev })
	prevScrapes := scrapes
	var lastErr error
	scrapes = newScraper(func(ctx context.Context, target string) error {
		err := shipScrape(ctx, target)
		if err != nil {
			t.Logf("scrape: %v", err)
		}
		mu.Lock()
		lastErr = err
		mu.Unlock()
		return err
	})
	t.Cleanup(func() { scrapes = prevScrapes })
	go scrapes.run()
	prevLogs := logs
	logs = newFollower(prevLogs.dial)
	t.Cleanup(func() { logs = prevLogs })
	go logs.run()

	const name = "graphene-scrape-it"
	cli, err := dockerclient.NewClientWithOpts(dockerclient.FromEnv, dockerclient.WithAPIVersionNegotiation())
	require.NoError(t, err)
	t.Cleanup(func() { _ = cli.Close() })
	_ = cli.ContainerRemove(context.Background(), name, container.RemoveOptions{Force: true})
	exposition := `# TYPE sql_service_latency histogram\nsql_service_latency_bucket{le="0.01"} 3\nsql_service_latency_bucket{le="0.1"} 8\nsql_service_latency_bucket{le="+Inf"} 9\nsql_service_latency_sum 0.7\nsql_service_latency_count 9\n`
	info, err := runActivity(context.Background(), Spec{Name: name,
		Config: &container.Config{Image: itImage(), ExposedPorts: nat.PortSet{"9100/tcp": {}},
			Cmd: []string{"sh", "-c", `mkdir -p /w && printf '` + exposition + `' > /w/metrics && exec httpd -f -p 9100 -h /w`}},
		Host: &container.HostConfig{PortBindings: nat.PortMap{"9100/tcp": {{HostIP: "127.0.0.1", HostPort: "19100"}}}},
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = removeActivity(context.Background(), name) })

	req := observeRequest{Id: info.Id, Entity: "docker/" + name, Scrape: "http://127.0.0.1:19100/metrics", ScrapeEvery: time.Second}
	require.NoError(t, followActivity(context.Background(), req))
	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(at) >= 4
	}, 15*time.Second, 50*time.Millisecond, "last scrape error: %v", func() error { mu.Lock(); defer mu.Unlock(); return lastErr }())
	mu.Lock()
	require.Equal(t, []string{"sql_service_latency_bucket", "sql_service_latency_sum", "sql_service_latency_count"}, last)
	for i := 2; i < len(at); i++ { // the first pulls may race httpd's start
		require.InDelta(t, float64(time.Second), float64(at[i].Sub(at[i-1])), float64(300*time.Millisecond))
	}
	before := len(at)
	mu.Unlock()

	require.NoError(t, drainActivity(context.Background(), req))
	mu.Lock()
	defer mu.Unlock()
	require.Greater(t, len(at), before, "the drain takes a last reading")
	scrapes.mu.Lock()
	require.Empty(t, scrapes.loops, "released by the drain")
	scrapes.mu.Unlock()
}
