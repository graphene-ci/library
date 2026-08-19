package gitlib

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"strings"

	"go.temporal.io/sdk/temporal"

	"github.com/graphene-ci/pipeline/pkg/activity"
	"github.com/graphene-ci/pipeline/pkg/machine"
)

// Spec describes one checkout. The workspace is the natural home: the
// default Dir is <workspace>/src/<repo>, a path valid in the container,
// on the machine, and for the docker daemon at once.
type Spec struct {
	// Repository is the clone URL (https or ssh).
	Repository string `json:"repository"`
	// Ref is a branch, tag, or SHA; empty means the remote's HEAD.
	Ref string `json:"ref,omitempty"`
	// ExpectedCommit pins the result: a checkout that resolves to any
	// other SHA fails WITHOUT retries — a moved branch is a signal,
	// not a flake.
	ExpectedCommit string `json:"expectedCommit,omitempty"`
	// Dir overrides the checkout directory.
	Dir string `json:"dir,omitempty"`
	// Depth shallows the fetch; 0 means full history.
	Depth int `json:"depth,omitempty"`
	// Clean resets and scrubs a reused directory (reset --hard +
	// clean -fdx) before checkout.
	Clean bool `json:"clean,omitempty"`
	// Submodules updates submodules recursively after checkout.
	Submodules bool `json:"submodules,omitempty"`

	Auth Auth `json:"auth,omitempty"`
}

// Report is the checkout's outcome.
type Report struct {
	// Dir is where the working tree is — feed it to whatever runs
	// next (a build context, a test command).
	Dir string `json:"dir"`
	// Commit is the resolved SHA actually checked out.
	Commit string `json:"commit"`
	Ref    string `json:"ref,omitempty"`
}

// Checkout brings the repository's working tree into the run's
// workspace — converging: a directory already holding this repository
// is fetched and reset, anything else is wiped and cloned. The
// machine's own git does the work.
func Checkout(spec Spec) activity.Call[Report] {
	return activity.Fn("git.checkout", checkoutActivity, spec)
}

func checkoutActivity(ctx context.Context, spec Spec) (Report, error) {
	if spec.Repository == "" {
		return Report{}, errors.New("checkout: repository is required")
	}
	dir := spec.Dir
	if dir == "" {
		if machine.Workspace() == "" {
			return Report{}, errors.New("checkout: no workspace (not agent-hosted) — set Dir explicitly")
		}
		dir = filepath.Join(machine.Workspace(), "src", repoName(spec.Repository))
	}
	extraArgs, extraEnv, cleanup, err := gitEnv(ctx, spec.Auth)
	if err != nil {
		return Report{}, err
	}
	defer cleanup()
	stop := heartbeat(ctx, "checkout "+spec.Repository)
	defer stop()

	// Reuse only a directory that IS this repository; anything else is
	// wiped — convergence, not archaeology.
	if origin, err := runGit(ctx, dir, nil, nil, "remote", "get-url", "origin"); err != nil || origin != spec.Repository {
		if err := os.RemoveAll(dir); err != nil {
			return Report{}, err
		}
		if err := os.MkdirAll(dir, 0o755); err != nil { //nolint:gosec // shared workspace dir
			return Report{}, err
		}
		if _, err := runGit(ctx, dir, nil, nil, "init", "--quiet"); err != nil {
			return Report{}, err
		}
		if _, err := runGit(ctx, dir, nil, nil, "remote", "add", "origin", spec.Repository); err != nil {
			return Report{}, err
		}
	} else if spec.Clean {
		if _, err := runGit(ctx, dir, nil, nil, "reset", "--hard", "--quiet"); err != nil {
			return Report{}, err
		}
		if _, err := runGit(ctx, dir, nil, nil, "clean", "-fdxq"); err != nil {
			return Report{}, err
		}
	}

	fetch := []string{"fetch", "--progress"}
	if spec.Depth > 0 {
		fetch = append(fetch, fmt.Sprintf("--depth=%d", spec.Depth))
	}
	target := spec.Ref
	if target == "" {
		target = "HEAD"
	}
	fetch = append(fetch, "origin", target)
	if err := streamGit(ctx, dir, extraArgs, extraEnv, fetch...); err != nil {
		return Report{}, err
	}
	if _, err := runGit(ctx, dir, nil, nil, "checkout", "--force", "--quiet", "FETCH_HEAD"); err != nil {
		return Report{}, err
	}
	if spec.Submodules {
		sub := []string{"submodule", "update", "--init", "--recursive"}
		if spec.Depth > 0 {
			sub = append(sub, fmt.Sprintf("--depth=%d", spec.Depth))
		}
		if err := streamGit(ctx, dir, extraArgs, extraEnv, sub...); err != nil {
			return Report{}, err
		}
	}
	commit, err := runGit(ctx, dir, nil, nil, "rev-parse", "HEAD")
	if err != nil {
		return Report{}, err
	}
	if spec.ExpectedCommit != "" && !strings.HasPrefix(commit, spec.ExpectedCommit) {
		return Report{}, temporal.NewNonRetryableApplicationError(
			fmt.Sprintf("checkout resolved %s, expected %s — the ref moved", commit, spec.ExpectedCommit),
			"git.commit-mismatch", nil)
	}
	return Report{Dir: dir, Commit: commit, Ref: spec.Ref}, nil
}

// repoName is the last path element without .git.
func repoName(repository string) string {
	name := path.Base(strings.TrimSuffix(strings.TrimSuffix(repository, "/"), ".git"))
	if i := strings.LastIndexAny(name, ":"); i >= 0 {
		name = name[i+1:]
	}
	if name == "" || name == "." || name == "/" {
		return "repo"
	}
	return name
}
