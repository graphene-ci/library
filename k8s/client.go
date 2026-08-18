// Package k8slib is the resource library over a user's Kubernetes
// cluster: any typed Kubernetes object — including CRD types generated
// by third parties (crossplane providers) — becomes a graphene Resource.
// The library's activities run on the run's worker, so the user's
// kubeconfig never leaves the run's contour; only the secret NAME
// travels through workflow history.
package k8slib

import (
	"github.com/graphene-ci/pipeline/pkg/pipeline"
)

// Client scopes resources to one cluster: the kubeconfig secret it was
// built from. It holds a reference, never the value — a pure spec
// factory, safe in workflow code.
type Client struct {
	kubeconfig pipeline.SecretRef
}

// NewClientFromSecret builds the client from the kubeconfig secret ref.
func NewClientFromSecret(kubeconfig pipeline.SecretRef) *Client {
	return &Client{kubeconfig: kubeconfig}
}

// Object is the output of a converged Kubernetes resource.
type Object struct {
	// Id is the crossplane external name when the object is a managed
	// resource (the provider's real-world id), the object name otherwise.
	Id string `json:"id"`
	// Manifest is the live object.
	Manifest map[string]any `json:"manifest,omitempty"`
}
