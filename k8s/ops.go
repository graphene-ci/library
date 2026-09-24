package k8slib

import (
	"github.com/graphene-ci/pipeline/pkg/obs"

	"context"
	"fmt"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/discovery/cached/memory"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/restmapper"
	"k8s.io/client-go/tools/clientcmd"

	"github.com/graphene-ci/pipeline/pkg/pipeline"
	"github.com/graphene-ci/pipeline/pkg/secretsapi"
)

// Ops activity names — the library's wire identities. The bodies run on
// the run's worker: the kubeconfig resolves at the point of use and
// never leaves the run's contour.
const (
	applyActivityName   = "k8s.apply"
	observeActivityName = "k8s.observe"
	deleteActivityName  = "k8s.delete"
)

// opRequest travels to every ops activity: which cluster (by secret
// NAME) and which object.
type opRequest struct {
	InCluster  bool               `json:"in_cluster,omitempty"`
	Kubeconfig pipeline.SecretRef `json:"kubeconfig"`
	Manifest   map[string]any     `json:"manifest"`
}

// observation is what observe reports back.
type observation struct {
	Exists   bool           `json:"exists"`
	Manifest map[string]any `json:"manifest,omitempty"`
}

// applyActivity server-side-applies the manifest. Idempotent by
// construction — SSA converges.
func applyActivity(ctx context.Context, req opRequest) error {
	cli, gvr, u, err := dial(ctx, req)
	if err != nil {
		return err
	}
	_, err = cli.Resource(gvr).Namespace(u.GetNamespace()).Apply(
		ctx, u.GetName(), u, metav1.ApplyOptions{FieldManager: "graphene", Force: true})
	if err == nil {
		obs.Info(ctx, "manifest applied", obs.Str("object", u.GetName()))
	}
	return err
}

// observeActivity reads the live object.
func observeActivity(ctx context.Context, req opRequest) (observation, error) {
	cli, gvr, u, err := dial(ctx, req)
	if err != nil {
		return observation{}, err
	}
	live, err := cli.Resource(gvr).Namespace(u.GetNamespace()).Get(ctx, u.GetName(), metav1.GetOptions{})
	if errors.IsNotFound(err) {
		// The object's absence is the record's own news, not only the
		// reconcile loop's private knowledge.
		obs.Warn(ctx, "object missing from the cluster", obs.Str("object", u.GetName()))
		return observation{Exists: false}, nil
	}
	if err != nil {
		return observation{}, err
	}
	return observation{Exists: true, Manifest: live.Object}, nil
}

// deleteActivity removes the object; absence is success.
func deleteActivity(ctx context.Context, req opRequest) error {
	cli, gvr, u, err := dial(ctx, req)
	if err != nil {
		return err
	}
	err = cli.Resource(gvr).Namespace(u.GetNamespace()).Delete(ctx, u.GetName(), metav1.DeleteOptions{})
	if err == nil {
		obs.Info(ctx, "object deleted", obs.Str("object", u.GetName()))
	}
	if errors.IsNotFound(err) {
		return nil
	}
	return err
}

// dial resolves the kubeconfig secret AT THE POINT OF USE and builds the
// dynamic client plus the REST mapping for the object.
func dial(ctx context.Context, req opRequest) (dynamic.Interface, schema.GroupVersionResource, *unstructured.Unstructured, error) {
	u := &unstructured.Unstructured{Object: req.Manifest}
	cfg, err := connectionConfig(ctx, req, secretsapi.Resolve, rest.InClusterConfig)
	if err != nil {
		return nil, schema.GroupVersionResource{}, nil, err
	}
	cli, err := dynamic.NewForConfig(cfg)
	if err != nil {
		return nil, schema.GroupVersionResource{}, nil, err
	}
	disco, err := discovery.NewDiscoveryClientForConfig(cfg)
	if err != nil {
		return nil, schema.GroupVersionResource{}, nil, err
	}
	mapper := restmapper.NewDeferredDiscoveryRESTMapper(memory.NewMemCacheClient(disco))
	gvk := u.GroupVersionKind()
	mapping, err := mapper.RESTMapping(gvk.GroupKind(), gvk.Version)
	if err != nil {
		return nil, schema.GroupVersionResource{}, nil, fmt.Errorf("no mapping for %s: %w", gvk, err)
	}
	return cli, mapping.Resource, u, nil
}

// isReady follows the Ready condition convention (crossplane/kstatus);
// an object without conditions is ready by existing.
func isReady(u *unstructured.Unstructured) bool {
	conditions, found, _ := unstructured.NestedSlice(u.Object, "status", "conditions")
	if !found {
		return true
	}
	for _, c := range conditions {
		m, ok := c.(map[string]any)
		if !ok {
			continue
		}
		if m["type"] == "Ready" {
			return m["status"] == "True"
		}
	}
	return false
}

func connectionConfig(ctx context.Context, req opRequest, resolve func(context.Context, pipeline.SecretRef) (string, error), inCluster func() (*rest.Config, error)) (*rest.Config, error) {
	if req.InCluster {
		if req.Kubeconfig.Name != "" {
			return nil, fmt.Errorf("in-cluster and kubeconfig identities are mutually exclusive")
		}
		return inCluster()
	}
	if req.Kubeconfig.Name == "" {
		return nil, fmt.Errorf("kubeconfig secret reference is required")
	}
	value, err := resolve(ctx, req.Kubeconfig)
	if err != nil {
		return nil, err
	}
	cfg, err := clientcmd.RESTConfigFromKubeConfig([]byte(value))
	if err != nil {
		return nil, fmt.Errorf("kubeconfig: %w", err)
	}
	return cfg, nil
}

// ObjectClient resolves this connection inside an activity and returns the
// native API for one object's kind and namespace. Use Resource for run-owned
// objects. This activity-only escape hatch serves externally owned configuration
// with an explicit application lifecycle; it does not register an entity or
// provide automatic cleanup. Never pass secret contents to workflow arguments
// or return them as activity results.
func (c *Client) ObjectClient(ctx context.Context, object *unstructured.Unstructured) (dynamic.ResourceInterface, error) {
	cli, gvr, u, err := dial(ctx, opRequest{Kubeconfig: c.kubeconfig, InCluster: c.inCluster, Manifest: object.Object})
	if err != nil {
		return nil, err
	}
	return cli.Resource(gvr).Namespace(u.GetNamespace()), nil
}
