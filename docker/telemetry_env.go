package dockerlib

import (
	"slices"
	"strings"

	"github.com/docker/docker/api/types/container"

	"github.com/graphene-ci/pipeline/pkg/machine"
)

// The standard OTLP exporter variables every OpenTelemetry SDK reads.
const (
	envOTLPEndpoint = "OTEL_EXPORTER_OTLP_ENDPOINT"
	envOTLPInsecure = "OTEL_EXPORTER_OTLP_INSECURE"
)

// withTelemetry points a container's OTLP exporter at the executor's local
// intake, so a tool that speaks OpenTelemetry reports into the run without
// being told anything: no door address, no token. A container on the host
// network reaches the intake on the loopback, any other through the docker
// bridge. Nothing is added when the user set the endpoint themselves (their
// collector is their business) or when no intake runs (outside an
// executor). The spec's own Config is left untouched.
func withTelemetry(cfg *container.Config, host *container.HostConfig) *container.Config {
	if cfg == nil || hasEnv(cfg.Env, envOTLPEndpoint) {
		return cfg
	}
	onHost := host != nil && host.NetworkMode.IsHost()
	ep := machine.OTLPEndpoint(!onHost)
	if ep == "" {
		return cfg
	}
	out := *cfg
	out.Env = append(slices.Clone(cfg.Env), envOTLPEndpoint+"=http://"+ep, envOTLPInsecure+"=true")
	return &out
}

func hasEnv(env []string, key string) bool {
	for _, kv := range env {
		if k, _, _ := strings.Cut(kv, "="); k == key {
			return true
		}
	}
	return false
}
