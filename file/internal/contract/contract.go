// Package contract shares the File resource wire types with adapters.
package contract

import (
	"encoding/json"

	"github.com/graphene-ci/pipeline/pkg/flow/ownership"
	"github.com/graphene-ci/pipeline/pkg/ref"
)

// DeclareActivity is the File resource declaration activity.
const DeclareActivity = "file.declare"

// FileSpec is what a File record IS.
type FileSpec struct {
	Path string `json:"path"`
	// Content is inline bytes (FromBytes/FromEmbed). Small configs only;
	// a secret's value never travels here.
	Content []byte `json:"content,omitempty"`
	// Secret is the NAME of a secret; the agent resolves the value on the
	// machine (worker plane), so it never sits in the spec.
	Secret string `json:"secret,omitempty"`
	// ArtifactLocation is the resolved blob location of an artifact
	// source (name → location done client-side); the agent streams it.
	ArtifactLocation string           `json:"artifactLocation,omitempty"`
	Mode             uint32           `json:"mode,omitempty"`
	Owner            ref.OwnerRef     `json:"owner,omitempty"`
	Flows            []ownership.Flow `json:"flows,omitempty"`
}

// DeclareRequest starts or attaches to a File resource.
type DeclareRequest struct {
	Name   string            `json:"name"`
	Labels map[string]string `json:"labels,omitempty"`
	RunId  string            `json:"runId,omitempty"`
	Spec   json.RawMessage   `json:"spec"`
}
