package k8slib

import (
	"context"
	"fmt"

	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/discovery/cached/memory"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/restmapper"
	"k8s.io/client-go/tools/clientcmd"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/graphene-ci/pipeline/pkg/pipeline"
	"github.com/graphene-ci/pipeline/pkg/secretsapi"
)

// Activity names — the library's wire identities.
const (
	applyActivityName   = "k8s.apply"
	observeActivityName = "k8s.observe"
	deleteActivityName  = "k8s.delete"
)

// request travels to every k8s activity: which cluster (by secret NAME)
// and which object.
type request struct {
	Kubeconfig pipeline.SecretRef `json:"kubeconfig"`
	Manifest   map[string]any     `json:"manifest"`
}

// observation is what observe reports back.
type observation struct {
	Ready    bool           `json:"ready"`
	Id       string         `json:"id,omitempty"`
	Manifest map[string]any `json:"manifest,omitempty"`
}

// applyActivity server-side-applies the manifest. Idempotent by
// construction — SSA converges.
func applyActivity(ctx context.Context, req request) error {
	cli, gvr, u, err := dial(ctx, req)
	if err != nil {
		return err
	}
	_, err = cli.Resource(gvr).Namespace(u.GetNamespace()).Apply(
		ctx, u.GetName(), u, metav1.ApplyOptions{FieldManager: "graphene", Force: true})
	return err
}

// observeActivity reads the live object and derives readiness: the
// crossplane/kstatus convention — condition Ready=True — with plain
// existence as the fallback for objects without conditions.
func observeActivity(ctx context.Context, req request) (observation, error) {
	cli, gvr, u, err := dial(ctx, req)
	if err != nil {
		return observation{}, err
	}
	live, err := cli.Resource(gvr).Namespace(u.GetNamespace()).Get(ctx, u.GetName(), metav1.GetOptions{})
	if errors.IsNotFound(err) {
		return observation{Ready: false}, nil
	}
	if err != nil {
		return observation{}, err
	}
	return observation{
		Ready:    isReady(live),
		Id:       externalName(live),
		Manifest: live.Object,
	}, nil
}

// deleteActivity removes the object; absence is success.
func deleteActivity(ctx context.Context, req request) error {
	cli, gvr, u, err := dial(ctx, req)
	if err != nil {
		return err
	}
	err = cli.Resource(gvr).Namespace(u.GetNamespace()).Delete(ctx, u.GetName(), metav1.DeleteOptions{})
	if errors.IsNotFound(err) {
		return nil
	}
	return err
}

// dial resolves the kubeconfig secret AT THE POINT OF USE and builds the
// dynamic client plus the REST mapping for the object.
func dial(ctx context.Context, req request) (dynamic.Interface, schema.GroupVersionResource, *unstructured.Unstructured, error) {
	u := &unstructured.Unstructured{Object: req.Manifest}
	value, err := secretsapi.Resolve(ctx, req.Kubeconfig)
	if err != nil {
		return nil, schema.GroupVersionResource{}, nil, err
	}
	cfg, err := clientcmd.RESTConfigFromKubeConfig([]byte(value))
	if err != nil {
		return nil, schema.GroupVersionResource{}, nil, fmt.Errorf("kubeconfig: %w", err)
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

// isReady follows the Ready condition convention; an object without
// conditions is ready by existing.
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

// externalName is the crossplane external name — the provider's real
// id — with the object name as the fallback.
func externalName(u *unstructured.Unstructured) string {
	if v := u.GetAnnotations()["crossplane.io/external-name"]; v != "" {
		return v
	}
	return u.GetName()
}
