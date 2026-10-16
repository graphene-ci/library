package dockerlib

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/docker/docker/api/types/container"
	dockerclient "github.com/docker/docker/client"
	"github.com/stretchr/testify/require"
)

func itImage() string {
	if img := os.Getenv("GRAPHENE_DOCKER_IT_IMAGE"); img != "" {
		return img
	}
	return "busybox"
}

func startIT(t *testing.T, name string, cmd string) string {
	t.Helper()
	cli, err := dockerclient.NewClientWithOpts(dockerclient.FromEnv, dockerclient.WithAPIVersionNegotiation())
	require.NoError(t, err)
	t.Cleanup(func() { _ = cli.Close() })
	_ = cli.ContainerRemove(context.Background(), name, container.RemoveOptions{Force: true})
	info, err := runActivity(context.Background(), Spec{Name: name, Config: &container.Config{Image: itImage(), Cmd: []string{"sh", "-c", cmd}}})
	require.NoError(t, err)
	t.Cleanup(func() { _ = removeActivity(context.Background(), name) })
	return info.Id
}

// The real daemon, opt-in: GRAPHENE_DOCKER_IT=1. A line reaches the
// shipper within a second or two of being written, not at a 30s beat.
func TestFollowLatencyAgainstDaemon(t *testing.T) {
	if os.Getenv("GRAPHENE_DOCKER_IT") == "" {
		t.Skip("set GRAPHENE_DOCKER_IT=1 to run against the local docker daemon")
	}
	var got shipped
	got.capture(t)
	f := newFollower(logs.dial)
	go f.run()
	id := startIT(t, "graphene-follow-latency", `sleep 1; echo marker; sleep 30`)
	f.watch(id, "docker/latency", 0, "running")
	defer f.release(context.Background(), id)
	written := time.Now().Add(time.Second)
	require.Eventually(t, func() bool {
		for _, l := range got.all() {
			if l == "stdout:marker" {
				return true
			}
		}
		return false
	}, 10*time.Second, 50*time.Millisecond)
	require.Less(t, time.Since(written), 3*time.Second, "line to shipper")
}

// A container that crashes and is torn down at once keeps its last
// words: the drain stops it with a grace period — its shutdown trap runs
// — and reads everything after the follower's cursor, the crash line and
// the goodbye included; nothing is shipped twice.
func TestDrainKeepsTheLastWordsAgainstDaemon(t *testing.T) {
	if os.Getenv("GRAPHENE_DOCKER_IT") == "" {
		t.Skip("set GRAPHENE_DOCKER_IT=1 to run against the local docker daemon")
	}
	var got shipped
	got.capture(t)
	prev := logs
	logs = newFollower(prev.dial)
	t.Cleanup(func() { logs = prev })
	go logs.run()
	script := `trap 'echo goodbye >&2; exit 0' TERM; for i in 1 2 3 4 5; do echo line-$i; done; echo "FATAL: backend crashed in create_indexes" >&2; while true; do sleep 0.1; done`
	id := startIT(t, "graphene-follow-drain", script)
	logs.watch(id, "docker/db", 0, "running")
	time.Sleep(300 * time.Millisecond) // teardown right after the crash, mid-follow
	require.NoError(t, drainActivity(context.Background(), observeRequest{Id: id, Entity: "docker/db"}))
	lines := got.all()
	joined := strings.Join(lines, "\n")
	for _, want := range []string{"stdout:line-1", "stdout:line-5", "stderr:FATAL: backend crashed in create_indexes", "stderr:goodbye"} {
		require.Contains(t, lines, want, "shipped:\n%s", joined)
	}
	seen := map[string]int{}
	for _, l := range lines {
		seen[l]++
		require.Equal(t, 1, seen[l], "shipped twice: %s", l)
	}
	require.Nil(t, logs.tailOf(id), "released by the drain")
	fmt.Println(joined)
}
