package contract

import (
	"github.com/graphene-ci/pipeline/pkg/flow/ownership"
	"github.com/graphene-ci/pipeline/pkg/pipeline"
	"github.com/graphene-ci/pipeline/pkg/ref"
)

// DeclareActivity is the Kubernetes resource declaration activity.
const DeclareActivity = "k8s.entity.declare"

// Spec is the desired manifest, credential reference and owner.
type Spec struct {
	Manifest   map[string]any     `json:"manifest"`
	InCluster  bool               `json:"in_cluster,omitempty"`
	Kubeconfig pipeline.SecretRef `json:"kubeconfig"`
	// Owner is the initial owner in the tree (the run by default).
	Owner ref.OwnerRef `json:"owner,omitempty"`
}

// k8sState is the entity state: the live object and the heal history.
// Kubeconfig is the teardown copy — the finalizer only sees State in the
// current chassis (its documented limitation), so what teardown needs
// lives here. TODO(chassis): let Finalize see Spec and drop it.
type State struct {
	Live       map[string]any     `json:"live,omitempty"`
	Heals      int                `json:"heals,omitempty"`
	Drifted    bool               `json:"drifted,omitempty"`
	InCluster  bool               `json:"in_cluster,omitempty"`
	Kubeconfig pipeline.SecretRef `json:"kubeconfig"`
	// Owned is the tree half: current owner, transfer command, the
	// EntityOwner/KeepUntil mirrors.
	ownership.State
}

// DeclareRequest starts or attaches to a Kubernetes resource.
type DeclareRequest struct {
	Kind      string            `json:"kind"`
	Name      string            `json:"name"`
	TaskQueue string            `json:"taskQueue"`
	Labels    map[string]string `json:"labels,omitempty"`
	RunId     string            `json:"runId,omitempty"`
	Spec      Spec              `json:"spec"`
}
