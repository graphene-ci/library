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
	"strconv"
	"time"

	"github.com/docker/docker/api/types/container"
	dockerclient "github.com/docker/docker/client"

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
	// Scrape is the container's prometheus endpoint; the scraper pulls it
	// every ScrapeEvery, apart from the beat.
	Scrape      string        `json:"scrape,omitempty"`
	ScrapeEvery time.Duration `json:"scrapeEvery,omitempty"`
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
		scrapes.release(ctx, req.Id)
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

	// One stats sample — the metric dimension of a running container. Its
	// application metrics are the scraper's, at their own step: the beat
	// only makes sure a running container IS scraped (after an executor
	// restart nobody scrapes it until someone says so) and that a stopped
	// one is not pulled in vain.
	if insp.State.Running {
		shipStatsOf(ctx, cli, req.Id)
		scrapes.watch(req.Id, req.Entity, req.Scrape, req.ScrapeEvery)
	} else {
		scrapes.release(ctx, req.Id)
	}
	return res, nil
}

// shipStatsOf ships one docker stats reading of a container.
func shipStatsOf(ctx context.Context, cli *dockerclient.Client, id string) {
	if stats, serr := cli.ContainerStatsOneShot(ctx, id); serr == nil {
		shipStats(ctx, stats.Body)
		_ = stats.Body.Close()
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

// followActivity starts following a container's log from a cursor and
// scraping its endpoint. It returns at once; the follower and the scraper
// stream from then on.
func followActivity(_ context.Context, req observeRequest) error {
	logs.watch(req.Id, req.Entity, req.SinceUnixNano, "running")
	scrapes.watch(req.Id, req.Entity, req.Scrape, req.ScrapeEvery)
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
	scrapes.release(ctx, req.Id)
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
		shipStatsOf(ctx, cli, req.Id)
		scrapeOnce(ctx, req.Scrape)
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
