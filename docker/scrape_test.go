package dockerlib

import (
	"context"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/docker/docker/api/types/container"
	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/graphene-ci/pipeline/pkg/obs"
)

func parse(t *testing.T, body string) map[string]*dto.MetricFamily {
	t.Helper()
	parser := expfmt.NewTextParser(model.UTF8Validation)
	fams, err := parser.TextToMetricFamilies(strings.NewReader(body))
	require.NoError(t, err)
	return fams
}

func byName(series []obs.Series) map[string]obs.Series {
	out := map[string]obs.Series{}
	for _, s := range series {
		out[s.Name] = s
	}
	return out
}

// point is a reading by its labels, for asserting one series member.
func point(t *testing.T, s obs.Series, attrs ...obs.KV) obs.Sample {
	t.Helper()
	for _, p := range s.Points {
		if equalAttrs(p.Attrs, attrs) {
			return p
		}
	}
	t.Fatalf("%s has no point %v; has %v", s.Name, attrs, s.Points)
	return obs.Sample{}
}

func equalAttrs(a, b []obs.KV) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// A prometheus exposition becomes series of readings taken at the scrape's
// moment, shaped as Prometheus stores them: a counter stays a counter,
// gauges and untyped values are gauges, a histogram is its cumulative
// buckets with le, its sum and count, a summary its quantiles with sum and
// count — the family's labels on every point, unchanged.
func TestScrapeSeriesShapesEveryFamily(t *testing.T) {
	fams := parse(t, `# TYPE node_cpu_seconds_total counter
node_cpu_seconds_total{cpu="0",mode="idle"} 632.3
node_cpu_seconds_total{cpu="0",mode="user"} 12.5
# TYPE pg_up gauge
pg_up 1
some_untyped 7
# TYPE sql_service_latency histogram
sql_service_latency_bucket{node="1",le="0.005"} 2
sql_service_latency_bucket{node="1",le="0.01"} 5
sql_service_latency_bucket{node="1",le="0.1"} 9
sql_service_latency_bucket{node="1",le="+Inf"} 10
sql_service_latency_sum{node="1"} 0.42
sql_service_latency_count{node="1"} 10
# TYPE go_gc_duration_seconds summary
go_gc_duration_seconds{quantile="0.5"} 0.0001
go_gc_duration_seconds{quantile="0.99"} 0.002
go_gc_duration_seconds_sum 0.05
go_gc_duration_seconds_count 40
`)
	at := time.Date(2026, 9, 27, 17, 13, 0, 0, time.UTC)
	series := scrapeSeries(fams, at)
	require.Equal(t, []string{
		"go_gc_duration_seconds", "go_gc_duration_seconds_sum", "go_gc_duration_seconds_count",
		"node_cpu_seconds_total", "pg_up", "some_untyped",
		"sql_service_latency_bucket", "sql_service_latency_sum", "sql_service_latency_count",
	}, names(series), "families sorted by name, each in exposition order")
	got := byName(series)

	cpu := got["node_cpu_seconds_total"]
	require.Equal(t, obs.CounterSeries, cpu.Kind, "a counter stays a counter")
	require.Len(t, cpu.Points, 2)
	require.Equal(t, at, cpu.Points[0].At, "every point carries the scrape's moment")
	require.InDelta(t, 632.3, point(t, cpu, obs.Str("cpu", "0"), obs.Str("mode", "idle")).Value, 1e-9)
	require.Equal(t, obs.GaugeSeries, got["pg_up"].Kind)
	require.Equal(t, obs.GaugeSeries, got["some_untyped"].Kind)

	buckets := got["sql_service_latency_bucket"]
	require.Equal(t, obs.CounterSeries, buckets.Kind, "buckets are cumulative counters")
	require.Len(t, buckets.Points, 4)
	for le, want := range map[string]float64{"0.005": 2, "0.01": 5, "0.1": 9, "+Inf": 10} {
		p := point(t, buckets, obs.Str("node", "1"), obs.Str("le", le))
		require.InDelta(t, want, p.Value, 0, "le=%s", le)
		require.Equal(t, at, p.At)
	}
	require.Equal(t, obs.CounterSeries, got["sql_service_latency_sum"].Kind)
	require.InDelta(t, 0.42, point(t, got["sql_service_latency_sum"], obs.Str("node", "1")).Value, 1e-9)
	require.Equal(t, obs.CounterSeries, got["sql_service_latency_count"].Kind)
	require.InDelta(t, 10, point(t, got["sql_service_latency_count"], obs.Str("node", "1")).Value, 0)

	q := got["go_gc_duration_seconds"]
	require.Equal(t, obs.GaugeSeries, q.Kind, "quantiles are gauges")
	require.InDelta(t, 0.0001, point(t, q, obs.Str("quantile", "0.5")).Value, 1e-12)
	require.InDelta(t, 0.002, point(t, q, obs.Str("quantile", "0.99")).Value, 1e-12)
	require.Equal(t, obs.CounterSeries, got["go_gc_duration_seconds_sum"].Kind)
	require.InDelta(t, 0.05, got["go_gc_duration_seconds_sum"].Points[0].Value, 1e-12)
	require.Equal(t, obs.CounterSeries, got["go_gc_duration_seconds_count"].Kind)
	require.InDelta(t, 40, got["go_gc_duration_seconds_count"].Points[0].Value, 0)
}

// A gauge histogram is a histogram whose buckets go up and down: gauges,
// with _gsum and _gcount; a histogram without its +Inf bucket still gets
// one, from its count.
func TestScrapeSeriesGaugeHistogramAndMissingInf(t *testing.T) {
	member := &dto.Metric{
		Label: []*dto.LabelPair{{Name: proto.String("queue"), Value: proto.String("raft")}},
		Histogram: &dto.Histogram{
			SampleCount: proto.Uint64(7),
			SampleSum:   proto.Float64(3.5),
			Bucket: []*dto.Bucket{
				{UpperBound: proto.Float64(1), CumulativeCount: proto.Uint64(4)},
				{UpperBound: proto.Float64(2.5), CumulativeCount: proto.Uint64(6)},
			},
		},
	}
	gh := dto.MetricType_GAUGE_HISTOGRAM
	h := dto.MetricType_HISTOGRAM
	got := byName(scrapeSeries(map[string]*dto.MetricFamily{
		"queue_size": {Name: proto.String("queue_size"), Type: &gh, Metric: []*dto.Metric{member}},
		"raft_wait":  {Name: proto.String("raft_wait"), Type: &h, Metric: []*dto.Metric{member}},
	}, time.Now()))

	gb := got["queue_size_bucket"]
	require.Equal(t, obs.GaugeSeries, gb.Kind)
	require.InDelta(t, 6, point(t, gb, obs.Str("queue", "raft"), obs.Str("le", "2.5")).Value, 0)
	require.InDelta(t, 7, point(t, gb, obs.Str("queue", "raft"), obs.Str("le", "+Inf")).Value, 0, "+Inf from the count")
	require.Equal(t, obs.GaugeSeries, got["queue_size_gsum"].Kind)
	require.Equal(t, obs.GaugeSeries, got["queue_size_gcount"].Kind)
	require.InDelta(t, 7, got["queue_size_gcount"].Points[0].Value, 0)
	require.NotContains(t, got, "queue_size_sum")

	rb := got["raft_wait_bucket"]
	require.Equal(t, obs.CounterSeries, rb.Kind)
	require.Len(t, rb.Points, 3)
	require.InDelta(t, 7, point(t, rb, obs.Str("queue", "raft"), obs.Str("le", "+Inf")).Value, 0)
}

// A family with nothing in it, a NaN sum, a NaN quantile and an infinite
// gauge are no readings — left out, and the rest of the scrape still ships.
func TestScrapeSeriesSkipsNonFinite(t *testing.T) {
	fams := parse(t, `# TYPE empty_total counter
# TYPE lat histogram
lat_bucket{le="1"} 1
lat_bucket{le="+Inf"} 1
lat_sum NaN
lat_count 1
# TYPE rpc summary
rpc{quantile="0.5"} NaN
rpc_sum +Inf
rpc_count 0
# TYPE temp gauge
temp -Inf
# TYPE ok gauge
ok 1
`)
	got := byName(scrapeSeries(fams, time.Now()))
	require.NotContains(t, got, "empty_total")
	require.NotContains(t, got, "lat_sum", "NaN sum left out")
	require.NotContains(t, got, "rpc", "NaN quantile left out")
	require.NotContains(t, got, "rpc_sum", "infinite sum left out")
	require.NotContains(t, got, "temp")
	require.Len(t, got["lat_bucket"].Points, 2)
	require.InDelta(t, 1, got["lat_count"].Points[0].Value, 0)
	require.InDelta(t, 0, got["rpc_count"].Points[0].Value, 0)
	require.InDelta(t, 1, got["ok"].Points[0].Value, 0)
}

// The scrape step is the spec's own: 15s when unset, never under a second.
func TestScrapeStep(t *testing.T) {
	require.Equal(t, 15*time.Second, scrapeStep(0))
	require.Equal(t, 5*time.Second, scrapeStep(5*time.Second))
	require.Equal(t, time.Second, scrapeStep(10*time.Millisecond))
}

// The scraper pulls a container's endpoint at the loop's step — at once,
// then every step — through the text exposition, ships every family, and
// stops when released. The stats beat is not involved.
func TestScraperPullsAtItsStep(t *testing.T) {
	var accept []string
	var mu sync.Mutex
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		accept = append(accept, r.Header.Get("Accept"))
		mu.Unlock()
		_, _ = w.Write([]byte("# TYPE lat histogram\nlat_bucket{le=\"1\"} 1\nlat_bucket{le=\"+Inf\"} 2\nlat_sum 1.5\nlat_count 2\n"))
	}))
	defer srv.Close()

	var shipped []time.Time
	var names []string
	prev := shipSeries
	shipSeries = func(_ context.Context, series ...obs.Series) error {
		mu.Lock()
		defer mu.Unlock()
		shipped = append(shipped, time.Now())
		names = nil
		for _, s := range series {
			names = append(names, s.Name)
		}
		return nil
	}
	defer func() { shipSeries = prev }()

	s := newScraper(shipScrape)
	go s.run()
	const step = 200 * time.Millisecond
	l := &scrapeLoop{id: "c1", target: srv.URL, every: step, stop: make(chan struct{}), done: make(chan struct{})}
	s.mu.Lock()
	s.loops["c1"] = l
	s.mu.Unlock()
	s.started <- l
	time.Sleep(5*step + step/2)
	s.release(context.Background(), "c1")

	mu.Lock()
	defer mu.Unlock()
	require.Len(t, shipped, 6, "at once, then every step")
	for i := 1; i < len(shipped); i++ {
		require.InDelta(t, float64(step), float64(shipped[i].Sub(shipped[i-1])), float64(step/4), "step %d", i)
	}
	require.Equal(t, []string{"lat_bucket", "lat_sum", "lat_count"}, names)
	require.True(t, strings.HasPrefix(accept[0], "text/plain"), "asks for the text exposition: %q", accept[0])

	n := len(shipped)
	mu.Unlock()
	time.Sleep(2 * step)
	mu.Lock()
	require.Len(t, shipped, n, "released: no more pulls")
}

// watch applies the spec's step and is idempotent; release lets go.
func TestScraperWatchRelease(t *testing.T) {
	var pulls sync.WaitGroup
	pulls.Add(1)
	var once sync.Once
	s := newScraper(func(context.Context, string) error {
		once.Do(pulls.Done)
		return nil
	})
	go s.run()
	s.watch("c1", "docker/db", "http://x/metrics", 5*time.Second)
	s.watch("c1", "docker/db", "http://x/metrics", time.Second)
	s.watch("c2", "docker/none", "", 0)
	pulls.Wait()
	s.mu.Lock()
	require.Len(t, s.loops, 1, "no endpoint, no loop; a second watch changes nothing")
	require.Equal(t, 5*time.Second, s.loops["c1"].every)
	s.mu.Unlock()
	s.release(context.Background(), "c1")
	s.release(context.Background(), "c1")
	require.Empty(t, s.loops)
}

// A docker stats sample is the beat's gauges: memory always, CPU when the
// deltas make sense.
func TestStatsSeries(t *testing.T) {
	at := time.Now()
	var s container.StatsResponse
	s.MemoryStats.Usage = 70 << 20
	s.CPUStats.CPUUsage.TotalUsage, s.PreCPUStats.CPUUsage.TotalUsage = 2_000_000_000, 1_000_000_000
	s.CPUStats.SystemUsage, s.PreCPUStats.SystemUsage = 10_000_000_000, 6_000_000_000
	s.CPUStats.OnlineCPUs = 2
	got := statsSeries(s, at)
	require.Len(t, got, 2)
	require.Equal(t, "docker.container.memory.bytes", got[0].Name)
	require.Equal(t, obs.GaugeSeries, got[0].Kind)
	require.InDelta(t, float64(70<<20), got[0].Points[0].Value, 0)
	require.Equal(t, "docker.container.cpu.percent", got[1].Name)
	require.InDelta(t, 50, got[1].Points[0].Value, 1e-9, "1s of 4s across 2 cpus")
	require.Equal(t, at, got[1].Points[0].At)

	s.CPUStats.SystemUsage = s.PreCPUStats.SystemUsage
	require.Len(t, statsSeries(s, at), 1, "no system delta, no cpu reading")
}

func TestFormatFloat(t *testing.T) {
	require.Equal(t, "+Inf", formatFloat(math.Inf(+1)))
	require.Equal(t, "0.005", formatFloat(0.005))
	require.Equal(t, "1", formatFloat(1))
	require.Equal(t, "1e-05", formatFloat(0.00001), "the exposition's own notation")
}

func names(series []obs.Series) []string {
	out := make([]string, len(series))
	for i, s := range series {
		out[i] = s.Name
	}
	return out
}
