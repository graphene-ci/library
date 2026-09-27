package dockerlib

import (
	"strings"
	"testing"
	"time"

	"github.com/docker/docker/api/types/container"
	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/require"

	"github.com/graphene-ci/pipeline/pkg/obs"
)

// A prometheus exposition becomes series of readings taken at the scrape's
// moment: a counter stays a counter, gauges and untyped values are gauges,
// labels ride on each point, histograms are not flattened into nonsense.
func TestScrapeSeriesKeepsKindTimeAndLabels(t *testing.T) {
	body := `# TYPE node_cpu_seconds_total counter
node_cpu_seconds_total{cpu="0",mode="idle"} 632.3
node_cpu_seconds_total{cpu="0",mode="user"} 12.5
# TYPE pg_up gauge
pg_up 1
# TYPE pg_stat_database_numbackends gauge
pg_stat_database_numbackends{datname="postgres"} 3
some_untyped 7
# TYPE http_request_duration_seconds histogram
http_request_duration_seconds_bucket{le="0.1"} 2
http_request_duration_seconds_bucket{le="+Inf"} 3
http_request_duration_seconds_sum 0.4
http_request_duration_seconds_count 3
`
	parser := expfmt.NewTextParser(model.UTF8Validation)
	fams, err := parser.TextToMetricFamilies(strings.NewReader(body))
	require.NoError(t, err)
	at := time.Date(2026, 9, 27, 17, 13, 0, 0, time.UTC)
	series := scrapeSeries(fams, at)
	byName := map[string]obs.Series{}
	for _, s := range series {
		byName[s.Name] = s
	}
	require.Equal(t, []string{"node_cpu_seconds_total", "pg_stat_database_numbackends", "pg_up", "some_untyped"}, names(series), "sorted, histogram skipped")

	cpu := byName["node_cpu_seconds_total"]
	require.Equal(t, obs.CounterSeries, cpu.Kind, "a counter stays a counter")
	require.Len(t, cpu.Points, 2)
	require.Equal(t, at, cpu.Points[0].At, "every point carries the scrape's moment")
	require.InDelta(t, 632.3, cpu.Points[0].Value, 1e-9)
	require.Equal(t, []obs.KV{obs.Str("cpu", "0"), obs.Str("mode", "idle")}, cpu.Points[0].Attrs)

	require.Equal(t, obs.GaugeSeries, byName["pg_up"].Kind)
	require.Equal(t, obs.GaugeSeries, byName["some_untyped"].Kind)
	require.InDelta(t, 3, byName["pg_stat_database_numbackends"].Points[0].Value, 1e-9)
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

func names(series []obs.Series) []string {
	out := make([]string, len(series))
	for i, s := range series {
		out[i] = s.Name
	}
	return out
}
