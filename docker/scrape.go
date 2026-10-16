package dockerlib

// A container's application metrics are pulled at their OWN pace, not at
// the observation beat's: a benchmark needs a reading every 10–15s to see
// a checkpoint or a GC pause, and the beat (stats, the log cursor) stays a
// workflow tick that must not grow the record's history that fast. The
// scraper belongs to the executor PROCESS like the log follower: started
// once with its worker, told by activities which container to scrape, at
// what step, and when to stop.

import (
	"context"
	"errors"
	"fmt"
	"math"
	"net/http"
	"sort"
	"strconv"
	"sync"
	"time"

	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/common/model"

	"github.com/graphene-ci/pipeline/pkg/obs"
)

const (
	// defaultScrapeEvery is the scrape step when the spec names none.
	defaultScrapeEvery = 15 * time.Second
	// minScrapeEvery keeps a typo from hammering an exporter.
	minScrapeEvery = time.Second
	// scrapeTimeout bounds one pull.
	scrapeTimeout = 10 * time.Second
)

// scrapeStep is the step a spec asks for, defaulted and bounded.
func scrapeStep(every time.Duration) time.Duration {
	if every <= 0 {
		return defaultScrapeEvery
	}
	return max(every, minScrapeEvery)
}

// scrapeLoop pulls one container's endpoint.
type scrapeLoop struct {
	id, entity, target string
	every              time.Duration
	stop, done         chan struct{}
}

// scraper is every scrape loop of this process.
type scraper struct {
	pull    func(ctx context.Context, target string) error
	mu      sync.Mutex
	loops   map[string]*scrapeLoop
	started chan *scrapeLoop
}

func newScraper(pull func(ctx context.Context, target string) error) *scraper {
	return &scraper{pull: pull, loops: map[string]*scrapeLoop{}, started: make(chan *scrapeLoop)}
}

// scrapes is the process's scraper; run starts it.
var scrapes = newScraper(shipScrape)

// run owns every scrape loop goroutine: started once from the worker's
// construction, it lives as long as the process.
func (s *scraper) run() {
	for l := range s.started {
		go s.loop(l)
	}
}

// watch scrapes a container's endpoint every step; scraping one already
// scraped changes nothing. It returns at once.
func (s *scraper) watch(id, entity, target string, every time.Duration) {
	if target == "" {
		return
	}
	s.mu.Lock()
	if _, ok := s.loops[id]; ok {
		s.mu.Unlock()
		return
	}
	l := &scrapeLoop{id: id, entity: entity, target: target, every: scrapeStep(every), stop: make(chan struct{}), done: make(chan struct{})}
	s.loops[id] = l
	s.mu.Unlock()
	s.started <- l
}

// release stops scraping a container; the wait for its loop to let go is
// bounded by ctx.
func (s *scraper) release(ctx context.Context, id string) {
	s.mu.Lock()
	l, ok := s.loops[id]
	delete(s.loops, id)
	s.mu.Unlock()
	if !ok {
		return
	}
	close(l.stop)
	select {
	case <-l.done:
	case <-ctx.Done():
	}
}

// loop scrapes at once and then every step until released. A failing
// endpoint is said once per distinct failure, not every step: an exporter
// that is still starting or already gone must not flood the record's log.
func (s *scraper) loop(l *scrapeLoop) {
	defer close(l.done)
	ctx, cancel := context.WithCancel(obs.WithEntity(context.Background(), l.entity))
	defer cancel()
	tick := time.NewTicker(l.every)
	defer tick.Stop()
	said := ""
	for {
		if err := s.pull(ctx, l.target); err != nil && ctx.Err() == nil {
			if err.Error() != said {
				obs.Warn(ctx, "scrape failed: "+err.Error(), obs.Str("container", l.id), obs.Str("target", l.target))
				said = err.Error()
			}
		} else {
			said = ""
		}
		select {
		case <-l.stop:
			return
		case <-tick.C:
		}
	}
}

// scrapeOnce pulls an endpoint now, outside any loop — the last reading
// before a container is stopped.
func scrapeOnce(ctx context.Context, target string) {
	if target == "" {
		return
	}
	if err := shipScrape(ctx, target); err != nil {
		obs.Warn(ctx, "scrape failed: "+err.Error(), obs.Str("target", target))
	}
}

// shipScrape pulls a prometheus endpoint and ships every sample under the
// record's reference — no token and no sidecar collector (Р-Н27:
// telemetry flows through the executor).
func shipScrape(ctx context.Context, target string) error {
	hctx, cancel := context.WithTimeout(ctx, scrapeTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(hctx, http.MethodGet, target, nil)
	if err != nil {
		return fmt.Errorf("target invalid: %w", err)
	}
	// The classic text exposition: every exporter speaks it, and it carries
	// histograms as the buckets Prometheus stores.
	req.Header.Set("Accept", string(expfmt.NewFormat(expfmt.TypeTextPlain)))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return errors.New("status " + resp.Status)
	}
	parser := expfmt.NewTextParser(model.UTF8Validation)
	families, err := parser.TextToMetricFamilies(resp.Body)
	if err != nil {
		return fmt.Errorf("parse: %w", err)
	}
	// Each sample is shipped as READ — its value at the scrape's moment, a
	// counter as a counter — never re-exported: a series ends when its
	// exporter does, and rate() sees the source's own clock.
	if err := shipSeries(ctx, scrapeSeries(families, time.Now())...); err != nil {
		return fmt.Errorf("export: %w", err)
	}
	return nil
}

// shipSeries exports scraped readings; a variable so tests can see what
// was shipped.
var shipSeries = obs.Ship

// scrapeSeries turns one exposition into series of readings taken at
// `at`, shaped the way Prometheus stores them, so histogram_quantile and
// rate() read them as from any Prometheus:
//
//   - counter — a counter; gauge and untyped — a gauge;
//   - histogram — <name>_bucket{le} (cumulative, "+Inf" included),
//     <name>_sum and <name>_count, all counters;
//   - gauge histogram — <name>_bucket{le}, <name>_gsum and <name>_gcount,
//     all gauges;
//   - summary — <name>{quantile} as gauges, <name>_sum and <name>_count
//     as counters.
//
// The family's labels ride on every point unchanged; le and quantile are
// added in the exposition's own float notation. A non-finite value (a
// summary's NaN quantile before any observation, a NaN sum) is not a
// reading and is left out; the rest of the family still ships.
//
// Native (sparse) histograms are not converted: the scrape asks for the
// text exposition, where they do not exist — a client that keeps a
// classic shape exposes its buckets there, one that is native-only
// exposes _sum, _count and the "+Inf" bucket, and that is what ships.
//
// Family order is fixed by name so two scrapes ship alike.
func scrapeSeries(families map[string]*dto.MetricFamily, at time.Time) []obs.Series {
	names := make([]string, 0, len(families))
	for name := range families {
		names = append(names, name)
	}
	sort.Strings(names)
	var out []obs.Series
	for _, name := range names {
		for _, s := range familySeries(families[name], at) {
			if len(s.Points) > 0 {
				out = append(out, s)
			}
		}
	}
	return out
}

// familySeries is one family's series, in exposition order.
func familySeries(fam *dto.MetricFamily, at time.Time) []obs.Series {
	name := fam.GetName()
	switch fam.GetType() {
	case dto.MetricType_COUNTER:
		s := obs.Series{Name: name, Kind: obs.CounterSeries}
		for _, m := range fam.GetMetric() {
			add(&s, at, m.GetCounter().GetValue(), labels(m))
		}
		return []obs.Series{s}
	case dto.MetricType_GAUGE:
		s := obs.Series{Name: name, Kind: obs.GaugeSeries}
		for _, m := range fam.GetMetric() {
			add(&s, at, m.GetGauge().GetValue(), labels(m))
		}
		return []obs.Series{s}
	case dto.MetricType_UNTYPED:
		s := obs.Series{Name: name, Kind: obs.GaugeSeries}
		for _, m := range fam.GetMetric() {
			add(&s, at, m.GetUntyped().GetValue(), labels(m))
		}
		return []obs.Series{s}
	case dto.MetricType_SUMMARY:
		q := obs.Series{Name: name, Kind: obs.GaugeSeries}
		sum := obs.Series{Name: name + "_sum", Kind: obs.CounterSeries}
		count := obs.Series{Name: name + "_count", Kind: obs.CounterSeries}
		for _, m := range fam.GetMetric() {
			sm := m.GetSummary()
			for _, qt := range sm.GetQuantile() {
				add(&q, at, qt.GetValue(), labels(m, obs.Str(model.QuantileLabel, formatFloat(qt.GetQuantile()))))
			}
			add(&sum, at, sm.GetSampleSum(), labels(m))
			add(&count, at, float64(sm.GetSampleCount()), labels(m))
		}
		return []obs.Series{q, sum, count}
	case dto.MetricType_HISTOGRAM:
		return histogramSeries(fam, at, obs.CounterSeries, "_sum", "_count")
	case dto.MetricType_GAUGE_HISTOGRAM:
		return histogramSeries(fam, at, obs.GaugeSeries, "_gsum", "_gcount")
	default:
		return nil
	}
}

// histogramSeries is a histogram family's buckets, sum and count. The
// "+Inf" bucket is always there: an exposition that leaves it out still
// counts every observation in its count.
func histogramSeries(fam *dto.MetricFamily, at time.Time, kind obs.SeriesKind, sumSuffix, countSuffix string) []obs.Series {
	name := fam.GetName()
	buckets := obs.Series{Name: name + "_bucket", Kind: kind}
	sum := obs.Series{Name: name + sumSuffix, Kind: kind}
	count := obs.Series{Name: name + countSuffix, Kind: kind}
	for _, m := range fam.GetMetric() {
		h := m.GetHistogram()
		total := float64(h.GetSampleCount())
		if h.SampleCountFloat != nil {
			total = h.GetSampleCountFloat()
		}
		inf := false
		for _, b := range h.GetBucket() {
			v := float64(b.GetCumulativeCount())
			if b.CumulativeCountFloat != nil {
				v = b.GetCumulativeCountFloat()
			}
			inf = inf || math.IsInf(b.GetUpperBound(), +1)
			add(&buckets, at, v, labels(m, obs.Str(model.BucketLabel, formatFloat(b.GetUpperBound()))))
		}
		if !inf {
			add(&buckets, at, total, labels(m, obs.Str(model.BucketLabel, formatFloat(math.Inf(+1)))))
		}
		add(&sum, at, h.GetSampleSum(), labels(m))
		add(&count, at, total, labels(m))
	}
	return []obs.Series{buckets, sum, count}
}

// add appends a finite reading.
func add(s *obs.Series, at time.Time, v float64, attrs []obs.KV) {
	if math.IsNaN(v) || math.IsInf(v, 0) {
		return
	}
	s.Points = append(s.Points, obs.Sample{At: at, Value: v, Attrs: attrs})
}

// labels is a family member's labels as attributes, plus extra.
func labels(m *dto.Metric, extra ...obs.KV) []obs.KV {
	out := make([]obs.KV, 0, len(m.GetLabel())+len(extra))
	for _, l := range m.GetLabel() {
		out = append(out, obs.Str(l.GetName(), l.GetValue()))
	}
	return append(out, extra...)
}

// formatFloat writes a bound or a quantile as the exposition does:
// shortest form, "+Inf" for the top bucket.
func formatFloat(f float64) string {
	switch {
	case math.IsInf(f, +1):
		return "+Inf"
	case math.IsInf(f, -1):
		return "-Inf"
	default:
		return strconv.FormatFloat(f, 'g', -1, 64)
	}
}
