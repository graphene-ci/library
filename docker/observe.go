package dockerlib

// The container's OWN telemetry — not what the pipeline did to it, but
// what it does itself. The record's reconcile tick is its observation
// beat: each tick ships the log lines since the previous one, one
// stats sample, and the status transition if there was one. The obs
// interceptor on the executor stamps every signal with the record's
// own reference ("docker/<name>"), so `logs docker/db -f` answers with
// THIS container's output — end to end, zero code per call site.

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/docker/docker/api/types/container"
	dockerclient "github.com/docker/docker/client"
	"github.com/docker/docker/pkg/stdcopy"
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

// observeActivity ships one beat of the container's own telemetry.
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
	if req.PrevStatus != "" && req.PrevStatus != res.Status {
		obs.Warn(ctx, fmt.Sprintf("container %s -> %s", req.PrevStatus, res.Status),
			obs.Str("container", req.Id))
		obs.Count(ctx, "docker.container.transition", 1,
			obs.Str("from", req.PrevStatus), obs.Str("to", res.Status))
		// A status transition is a MILESTONE of the record, not only a
		// log line: it goes into the entity's own history (dimension
		// 2), where it survives every telemetry retention.
		_ = obs.Event(ctx, "container-"+res.Status, map[string]string{
			"from": req.PrevStatus, "to": res.Status, "container": req.Id,
		})
	}

	// The container's own lines since the previous beat.
	since := ""
	if req.SinceUnixNano > 0 {
		since = time.Unix(0, req.SinceUnixNano).UTC().Format(time.RFC3339Nano)
	}
	if logs, lerr := cli.ContainerLogs(ctx, req.Id, container.LogsOptions{
		ShowStdout: true, ShowStderr: true, Since: since, Timestamps: true,
	}); lerr == nil {
		res.LastLogUnixNano = shipLogs(ctx, logs, req.SinceUnixNano)
		_ = logs.Close()
	}

	// One stats sample — the metric dimension of a running container.
	if insp.State.Running {
		if stats, serr := cli.ContainerStatsOneShot(ctx, req.Id); serr == nil {
			shipStats(ctx, stats.Body)
			_ = stats.Body.Close()
		}
		// The container's OWN application metrics: pull its prometheus
		// endpoint and ship every sample under this record's reference —
		// the obs interceptor already stamps entity=docker/<name>, so the
		// scraped metrics correlate to the container with no token and no
		// sidecar collector (Р-Н27: telemetry flows through the beat).
		if req.Scrape != "" {
			shipScrape(ctx, req.Scrape)
		}
	}
	return res, nil
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
	for name, fam := range families {
		for _, m := range fam.GetMetric() {
			v, ok := sampleValue(fam.GetType(), m)
			if !ok {
				continue
			}
			attrs := make([]obs.KV, 0, len(m.GetLabel()))
			for _, l := range m.GetLabel() {
				attrs = append(attrs, obs.Str(l.GetName(), l.GetValue()))
			}
			obs.Gauge(ctx, name, v, attrs...)
		}
	}
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
	default:
		return 0, false
	}
}

// shipLogs turns the docker log stream into obs log lines and returns
// the newest timestamp seen.
func shipLogs(ctx context.Context, logs io.Reader, last int64) int64 {
	pr, pw := io.Pipe()
	go func() {
		_, _ = stdcopy.StdCopy(pw, pw, logs)
		_ = pw.Close()
	}()
	scan := bufio.NewScanner(pr)
	scan.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	for scan.Scan() {
		line := scan.Text()
		// "2026-08-26T12:00:00.000000000Z body" — docker's own stamp.
		ts, body := splitDockerStamp(line)
		if ts > 0 && ts <= last {
			continue // the Since window is second-coarse; drop the overlap
		}
		obs.Info(ctx, body, obs.Str("stream", "container"))
		if ts > last {
			last = ts
		}
	}
	return last
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

// shipStats renders one docker stats sample as metrics.
func shipStats(ctx context.Context, body io.Reader) {
	var s container.StatsResponse
	if json.NewDecoder(body).Decode(&s) != nil {
		return
	}
	cpuDelta := float64(s.CPUStats.CPUUsage.TotalUsage - s.PreCPUStats.CPUUsage.TotalUsage)
	sysDelta := float64(s.CPUStats.SystemUsage - s.PreCPUStats.SystemUsage)
	if sysDelta > 0 && cpuDelta >= 0 {
		obs.Measure(ctx, "docker.container.cpu.percent",
			cpuDelta/sysDelta*float64(s.CPUStats.OnlineCPUs)*100)
	}
	obs.Measure(ctx, "docker.container.memory.bytes", float64(s.MemoryStats.Usage))
}
