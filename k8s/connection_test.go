package k8slib

import (
	"context"
	"errors"
	"github.com/graphene-ci/pipeline/pkg/pipeline"
	"github.com/stretchr/testify/require"
	"k8s.io/client-go/rest"
	"testing"
)

func TestConnectionIdentityIsExplicit(t *testing.T) {
	ctx := context.Background()
	resolved, clustered := 0, 0
	denied := errors.New("secret denied")
	resolve := func(context.Context, pipeline.SecretRef) (string, error) { resolved++; return "", denied }
	cluster := func() (*rest.Config, error) { clustered++; return &rest.Config{Host: "https://installation"}, nil }
	cfg, err := connectionConfig(ctx, opRequest{InCluster: true}, resolve, cluster)
	require.NoError(t, err)
	require.Equal(t, "https://installation", cfg.Host)
	require.Equal(t, 0, resolved)
	require.Equal(t, 1, clustered)
	_, err = connectionConfig(ctx, opRequest{Kubeconfig: pipeline.UseSecret("user-cluster")}, resolve, cluster)
	require.ErrorIs(t, err, denied)
	require.Equal(t, 1, resolved)
	require.Equal(t, 1, clustered)
	_, err = connectionConfig(ctx, opRequest{InCluster: true, Kubeconfig: pipeline.UseSecret("user-cluster")}, resolve, cluster)
	require.ErrorContains(t, err, "mutually exclusive")
	_, err = connectionConfig(ctx, opRequest{}, resolve, cluster)
	require.ErrorContains(t, err, "required")
	require.Equal(t, 1, clustered)
}
