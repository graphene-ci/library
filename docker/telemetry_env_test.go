package dockerlib

import (
	"testing"

	"github.com/docker/docker/api/types/container"
	"github.com/stretchr/testify/require"
)

// A container learns the intake from the executor's environment, the
// bridge address unless it shares the host network; a user's own endpoint
// is respected; without an intake nothing is added.
func TestWithTelemetry(t *testing.T) {
	t.Setenv("GRAPHENE_OTLP_ENDPOINT", "127.0.0.1:41000")
	t.Setenv("GRAPHENE_OTLP_ENDPOINT_BRIDGE", "172.17.0.1:41000")

	base := &container.Config{Image: "x", Env: []string{"A=1"}}
	got := withTelemetry(base, nil)
	require.Equal(t, []string{"A=1", "OTEL_EXPORTER_OTLP_ENDPOINT=http://172.17.0.1:41000", "OTEL_EXPORTER_OTLP_INSECURE=true"}, got.Env)
	require.Equal(t, []string{"A=1"}, base.Env, "the spec is not mutated")

	got = withTelemetry(base, &container.HostConfig{NetworkMode: "host"})
	require.Equal(t, "OTEL_EXPORTER_OTLP_ENDPOINT=http://127.0.0.1:41000", got.Env[1])

	own := &container.Config{Env: []string{"OTEL_EXPORTER_OTLP_ENDPOINT=http://mine:4317"}}
	require.Same(t, own, withTelemetry(own, nil))

	t.Setenv("GRAPHENE_OTLP_ENDPOINT_BRIDGE", "")
	require.Same(t, base, withTelemetry(base, nil), "no bridge address, nothing a bridged container could reach")
	t.Setenv("GRAPHENE_OTLP_ENDPOINT", "")
	require.Same(t, base, withTelemetry(base, &container.HostConfig{NetworkMode: "host"}))
	require.Nil(t, withTelemetry(nil, nil))
}
