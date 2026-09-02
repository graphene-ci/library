package dockerlib

import (
	"github.com/prometheus/common/model"
	"strings"
	"testing"

	"github.com/prometheus/common/expfmt"
)

// A prometheus exposition parses into families and each simple sample
// yields a value — the shape shipScrape ships as gauges.
func TestScrapeParse(t *testing.T) {
	body := `# HELP pg_up Whether the last scrape of metrics from PostgreSQL was able to connect.
# TYPE pg_up gauge
pg_up 1
# HELP pg_stat_database_numbackends Number of backends.
# TYPE pg_stat_database_numbackends gauge
pg_stat_database_numbackends{datname="postgres"} 3
# TYPE pg_settings_max_connections gauge
pg_settings_max_connections 100
`
	p := expfmt.NewTextParser(model.UTF8Validation)
	fams, err := p.TextToMetricFamilies(strings.NewReader(body))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	up, ok := fams["pg_up"]
	if !ok {
		t.Fatal("pg_up family missing")
	}
	v, got := sampleValue(up.GetType(), up.GetMetric()[0])
	if !got || v != 1 {
		t.Fatalf("pg_up = %v (ok=%v), want 1", v, got)
	}
	nb := fams["pg_stat_database_numbackends"].GetMetric()[0]
	if len(nb.GetLabel()) != 1 || nb.GetLabel()[0].GetValue() != "postgres" {
		t.Fatalf("label not parsed: %+v", nb.GetLabel())
	}
	v2, _ := sampleValue(fams["pg_stat_database_numbackends"].GetType(), nb)
	if v2 != 3 {
		t.Fatalf("numbackends = %v, want 3", v2)
	}
}
