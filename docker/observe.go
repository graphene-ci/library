package dockerlib

// The container's OWN telemetry — not what the pipeline did to it, but
// what it does itself. The record's reconcile tick is its observation
// beat: each tick ships the log lines since the previous one, one
// stats sample, and the status transition if there was one. The obs
// interceptor on the executor stamps every signal with the record's
// own reference ("docker/<name>"), so `logs docker/db -f` answers with
// THIS container's output — end to end, zero code per call site.

import (
	"context"
	"encoding/json"
	"fmt"
	cerrdefs "github.com/containerd/errdefs"
	"io"
	"net/http"
	"sort"
	"strconv"
	"time"

	"github.com/docker/docker/api/types/container"
	dockerclient "github.com/docker/docker/client"
	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/common/model"

	"github.com/graphene-ci/pipeline/pkg/obs"
)

// observeEvery is the observation beat. Logs arrive with at most this
// delay; the entity chassis owns the history budget (continue-as-new).
const observeEvery = 30 * time.Second

const observeActivityName = "docker.container.observe"

// observeRequest asks for one observation beat.
type observeRequest struct {
	Id string `json:"id"`
	// SinceUnixNano bounds the log window: lines after the previous
	// beat only.
	SinceUnixNano int64 `json:"sinceUnixNano"`
	// PrevStatus is what the record believed; a transition is worth a
	// louder line than a sample.
	PrevStatus string `json:"prevStatus,omitempty"`
	// Scrape is the container's prometheus endpoint, pulled each beat.
	Scrape string `json:"scrape,omitempty"`
	// Entity is this record's reference ("docker/<name>"); the beat stamps
	// every sample, log and scraped metric with it, so `metrics docker
	// <name>` (and logs/events) find them under the record — not only
	// under the run/agent the executor happens to carry.
	Entity string `json:"entity,omitempty"`
}

// observeResult is what the beat saw.
type observeResult struct {
	Status string `json:"status"`
	// LastLogUnixNano is the next beat's window start.
	LastLogUnixNano int64 `json:"lastLogUnixNano"`
}

// observeActivity ships one beat of the container's own telemetry: a
// stats sample, a scrape, a status transition. The log is the follower's
// — the beat makes sure the container IS followed (after an executor
// restart nobody follows it until someone says so) and reports where
// the follower got to, so the record keeps the cursor.
func observeActivity(ctx context.Context, req observeRequest) (observeResult, error) {
	// Attribute this whole beat to the container's record, so its logs,
	// stats and scraped app-metrics land under docker/<name> — the
	// interceptor only carries the run/agent the executor runs as.
	if req.Entity != "" {
		ctx = obs.WithEntity(ctx, req.Entity)
	}
	res := observeResult{LastLogUnixNano: req.SinceUnixNano}
	cli, err := dockerclient.NewClientWithOpts(dockerclient.FromEnv, dockerclient.WithAPIVersionNegotiation())
	if err != nil {
		return res, err
	}
	defer func() { _ = cli.Close() }()

	insp, err := cli.ContainerInspect(ctx, req.Id)
	if err != nil {
		// The container is gone: that IS the observation.
		obs.Warn(ctx, "container is gone", obs.Str("container", req.Id))
		res.Status = "gone"
		return res, nil
	}
	res.Status = insp.State.Status
	reported := req.PrevStatus
	if t := logs.tailOf(req.Id); t != nil {
		cursor, status := t.position()
		res.LastLogUnixNano = max(cursor, req.SinceUnixNano)
		if status != "" {
			reported = status // the follower has already told this one
		}
	} else {
		logs.watch(req.Id, req.Entity, req.SinceUnixNano, res.Status)
	}
	if reported != "" && reported != res.Status {
		reportTransition(ctx, req.Id, reported, res.Status, insp.State.ExitCode)
	}

	// One stats sample — the metric dimension of a running container.
	if insp.State.Running {
		sample(ctx, cli, req.Id, req.Scrape)
	}
	return res, nil
}

// sample ships one stats reading and the container's own application
// metrics: pull its prometheus endpoint and ship every sample under this
// record's reference — no token and no sidecar collector (R-N27:
// telemetry flows through the executor).
func sample(ctx context.Context, cli *dockerclient.Client, id, scrape string) {
	if stats, serr := cli.ContainerStatsOneShot(ctx, id); serr == nil {
		shipStats(ctx, stats.Body)
		_ = stats.Body.Close()
	}
	if scrape != "" {
		shipScrape(ctx, scrape)
	}
}

// reportTransition makes a status change the record's news: a louder
// log line than a sample, a counter, and a milestone in the record's own
// history (dimension 2), where it survives every telemetry retention.
func reportTransition(ctx context.Context, id, from, to string, exitCode int) {
	attrs := []obs.KV{obs.Str("container", id)}
	payload := map[string]string{"from": from, "to": to, "container": id}
	if to == "exited" || to == "dead" {
		attrs = append(attrs, obs.Int("exitCode", exitCode))
		payload["exitCode"] = strconv.Itoa(exitCode)
	}
	obs.Warn(ctx, fmt.Sprintf("container %s -> %s", from, to), attrs...)
	obs.Count(ctx, "docker.container.transition", 1, obs.Str("from", from), obs.Str("to", to))
	_ = obs.Event(ctx, "container-"+to, payload)
}

const (
	followActivityName = "docker.container.follow"
	drainActivityName  = "docker.container.drain"
	// drainTimeout bounds a container's retirement — the stop's grace and
	// the last read: the teardown must not wait on a daemon that is going
	// away itself.
	drainTimeout = (stopGrace + 10) * time.Second
)

// followActivity starts following a container's log from a cursor. It
// returns at once; the follower streams from then on.
func followActivity(_ context.Context, req observeRequest) error {
	logs.watch(req.Id, req.Entity, req.SinceUnixNano, "running")
	return nil
}

// drainActivity retires a container right before it is removed: a final
// stats and scrape sample while it still runs, a stop with a grace period
// (SIGTERM — the process flushes what it buffers and says goodbye), then
// everything written after the follower's cursor is read and shipped —
// the lines and numbers that explain how a container ended are the last
// ones, and they must not go away with it. Bounded and best effort.
func drainActivity(ctx context.Context, req observeRequest) error {
	ctx, cancel := context.WithTimeout(ctx, drainTimeout)
	defer cancel()
	if req.Entity != "" {
		ctx = obs.WithEntity(ctx, req.Entity)
	}
	t := logs.release(ctx, req.Id)
	if t == nil {
		// Not followed (an executor that restarted and never beat since):
		// read on from the record's own cursor.
		t = &tail{id: req.Id, entity: req.Entity, cursor: req.SinceUnixNano, streams: map[string]int64{}}
	}
	cli, err := dockerclient.NewClientWithOpts(dockerclient.FromEnv, dockerclient.WithAPIVersionNegotiation())
	if err != nil {
		return err
	}
	defer func() { _ = cli.Close() }()
	if insp, ierr := cli.ContainerInspect(ctx, req.Id); ierr == nil && insp.State != nil && insp.State.Running {
		if t.status == "" {
			t.status = insp.State.Status
		}
		sample(ctx, cli, req.Id, req.Scrape)
		grace := stopGrace
		if serr := cli.ContainerStop(ctx, req.Id, container.StopOptions{Timeout: &grace}); serr != nil && !cerrdefs.IsNotFound(serr) {
			obs.Warn(ctx, "container stop failed: "+serr.Error(), obs.Str("container", req.Id))
		}
		if after, aerr := cli.ContainerInspect(ctx, req.Id); aerr == nil && after.State != nil && t.status != "" && t.status != after.State.Status {
			reportTransition(ctx, req.Id, t.status, after.State.Status, after.State.ExitCode)
		}
	}
	if err := drain(ctx, cli, t); err != nil && !cerrdefs.IsNotFound(err) {
		obs.Warn(ctx, "last log lines not read: "+err.Error(), obs.Str("container", req.Id))
	}
	return nil
}

// shipScrape pulls a prometheus endpoint and emits each sample as a
// gauge under the record's reference. Best effort: a scrape failure is
// a warning, never fatal to the beat.
func shipScrape(ctx context.Context, target string) {
	hctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(hctx, http.MethodGet, target, nil)
	if err != nil {
		obs.Warn(ctx, "scrape target invalid", obs.Str("target", target))
		return
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		obs.Warn(ctx, "scrape failed", obs.Str("target", target), obs.Str("error", err.Error()))
		return
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		obs.Warn(ctx, "scrape non-200", obs.Str("target", target), obs.Str("status", resp.Status))
		return
	}
	parser := expfmt.NewTextParser(model.UTF8Validation)
	families, err := parser.TextToMetricFamilies(resp.Body)
	if err != nil {
		obs.Warn(ctx, "scrape parse failed", obs.Str("target", target), obs.Str("error", err.Error()))
		return
	}
	// Each sample is shipped as READ — its value at the scrape's moment,
	// a counter as a counter — never re-exported: a series ends when its
	// exporter does, and rate() sees the source's own clock.
	if err := obs.Ship(ctx, scrapeSeries(families, time.Now())...); err != nil {
		obs.Warn(ctx, "scrape export failed", obs.Str("target", target), obs.Str("error", err.Error()))
	}
}

// scrapeSeries turns one exposition into series of readings taken at
// `at`: counters stay counters, gauges and untyped values are gauges;
// histograms and summaries need a shape of their own and are skipped.
// Family order is fixed by name so two scrapes ship alike.
func scrapeSeries(families map[string]*dto.MetricFamily, at time.Time) []obs.Series {
	names := make([]string, 0, len(families))
	for name := range families {
		names = append(names, name)
	}
	sort.Strings(names)
	out := make([]obs.Series, 0, len(names))
	for _, name := range names {
		fam := families[name]
		kind := obs.GaugeSeries
		if fam.GetType() == dto.MetricType_COUNTER {
			kind = obs.CounterSeries
		}
		series := obs.Series{Name: name, Kind: kind}
		for _, m := range fam.GetMetric() {
			v, ok := sampleValue(fam.GetType(), m)
			if !ok {
				continue
			}
			attrs := make([]obs.KV, 0, len(m.GetLabel()))
			for _, l := range m.GetLabel() {
				attrs = append(attrs, obs.Str(l.GetName(), l.GetValue()))
			}
			series.Points = append(series.Points, obs.Sample{At: at, Value: v, Attrs: attrs})
		}
		if len(series.Points) > 0 {
			out = append(out, series)
		}
	}
	return out
}

// sampleValue extracts a single number from a metric family member —
// gauge/counter/untyped directly, others skipped (histograms/summaries
// need their own shape, out of scope for the beat).
func sampleValue(t dto.MetricType, m *dto.Metric) (float64, bool) {
	switch t {
	case dto.MetricType_GAUGE:
		return m.GetGauge().GetValue(), true
	case dto.MetricType_COUNTER:
		return m.GetCounter().GetValue(), true
	case dto.MetricType_UNTYPED:
		return m.GetUntyped().GetValue(), true
	case dto.MetricType_SUMMARY, dto.MetricType_HISTOGRAM, dto.MetricType_GAUGE_HISTOGRAM:
		return 0, false
	default:
		return 0, false
	}
}

// splitDockerStamp cuts docker's RFC3339Nano prefix off a log line.
func splitDockerStamp(line string) (int64, string) {
	for i := range line {
		if line[i] == ' ' {
			if ts, err := time.Parse(time.RFC3339Nano, line[:i]); err == nil {
				return ts.UnixNano(), line[i+1:]
			}
			break
		}
	}
	return 0, line
}

// shipStats renders one docker stats sample as gauges of this beat's
// moment: what the container uses NOW, not a distribution and not a
// reading repeated until the next beat.
func shipStats(ctx context.Context, body io.Reader) {
	var s container.StatsResponse
	if json.NewDecoder(body).Decode(&s) != nil {
		return
	}
	if err := obs.Ship(ctx, statsSeries(s, time.Now())...); err != nil {
		obs.Warn(ctx, "stats export failed", obs.Str("error", err.Error()))
	}
}

// statsSeries is the beat's reading of a docker stats sample.
func statsSeries(s container.StatsResponse, at time.Time) []obs.Series {
	out := []obs.Series{{Name: "docker.container.memory.bytes", Kind: obs.GaugeSeries,
		Points: []obs.Sample{{At: at, Value: float64(s.MemoryStats.Usage)}}}}
	cpuDelta := float64(s.CPUStats.CPUUsage.TotalUsage - s.PreCPUStats.CPUUsage.TotalUsage)
	sysDelta := float64(s.CPUStats.SystemUsage - s.PreCPUStats.SystemUsage)
	if sysDelta > 0 && cpuDelta >= 0 {
		out = append(out, obs.Series{Name: "docker.container.cpu.percent", Kind: obs.GaugeSeries,
			Points: []obs.Sample{{At: at, Value: cpuDelta / sysDelta * float64(s.CPUStats.OnlineCPUs) * 100}}})
	}
	return out
}
