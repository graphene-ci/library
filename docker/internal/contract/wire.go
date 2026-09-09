// Package contract shares Docker resource wire types with adapters.
package contract

import (
	"encoding/json"
	"github.com/docker/docker/api/types/container"
	"github.com/graphene-ci/pipeline/pkg/flow/ownership"
	"github.com/graphene-ci/pipeline/pkg/ref"
	entity "github.com/graphene-ci/temporal-entity/pkg/entity"
)

// DeclareActivity is the Docker resource declaration activity.
const DeclareActivity = "docker.entity.declare"

// ContainerSpec is the native Docker configuration and its ownership metadata.
type ContainerSpec struct {
	Name   string                `json:"name"`
	Config *container.Config     `json:"config"`
	Host   *container.HostConfig `json:"host,omitempty"`
	Owner  ref.OwnerRef          `json:"owner,omitempty"`
	// Flows are the declared outgoing edges of this container (Р-Н25) —
	// carried into the record's state for the topology view.
	Flows []ownership.Flow `json:"flows,omitempty"`
	// Scrape is a prometheus metrics endpoint the container exposes; the
	// observation beat pulls it and ships the samples as the container's
	// own metrics (Р-Н27). Empty disables scraping.
	Scrape string `json:"scrape,omitempty"`
}

// DeclareRequest starts or attaches to a Docker resource.
type DeclareRequest struct {
	Kind   entity.KindName   `json:"kind"`
	Name   string            `json:"name"`
	Labels map[string]string `json:"labels,omitempty"`
	RunId  string            `json:"runId,omitempty"`
	// Spec is the kind-shaped spec as JSON.
	Spec json.RawMessage `json:"spec"`
}
