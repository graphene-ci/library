// Package k8slib is the resource library over a user's Kubernetes
// cluster: any typed Kubernetes object — including CRD types generated
// by third parties (crossplane providers) — becomes a graphene Resource.
// The library's activities run on the run's worker, so the user's
// kubeconfig never leaves the run's contour; only the secret NAME
// travels through workflow history.
package k8slib

import (
	"k8s.io/apimachinery/pkg/runtime"

	"github.com/graphene-ci/pipeline/pkg/pipeline"
)

// Client scopes resources to one cluster: the kubeconfig secret it was
// built from. It holds a reference, never the value — a pure spec
// factory, safe in workflow code.
type Client struct {
	kubeconfig pipeline.SecretRef
	scheme     *runtime.Scheme
}

// ClientOption tunes the client.
type ClientOption func(*Client)

// WithScheme teaches the client the types of a provider (its generated
// AddToScheme), so objects need no hand-written TypeMeta — the
// apiVersion and kind are derived from the Go type.
func WithScheme(builders ...func(*runtime.Scheme) error) ClientOption {
	return func(c *Client) {
		for _, add := range builders {
			// A type set that cannot register is a programming error in
			// the provider package; surface it at first use instead.
			_ = add(c.scheme)
		}
	}
}

// NewClientFromSecret builds the client from the kubeconfig secret ref.
func NewClientFromSecret(kubeconfig pipeline.SecretRef, opts ...ClientOption) *Client {
	c := &Client{kubeconfig: kubeconfig, scheme: runtime.NewScheme()}
	for _, opt := range opts {
		opt(c)
	}
	return c
}
