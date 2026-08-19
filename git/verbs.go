package gitlib

import (
	"context"
	"errors"
	"strings"

	"github.com/graphene-ci/pipeline/pkg/activity"
)

// LsRemoteSpec asks a remote for a ref's SHA — no clone, no workspace.
type LsRemoteSpec struct {
	Repository string `json:"repository"`
	// Ref is a branch or tag name; empty means the remote's HEAD.
	Ref  string `json:"ref,omitempty"`
	Auth Auth   `json:"auth,omitempty"`
}

// LsRemoteReport is the resolved SHA.
type LsRemoteReport struct {
	Commit string `json:"commit"`
	Ref    string `json:"ref,omitempty"`
}

// LsRemote resolves a ref to its SHA straight from the remote — the
// cheap way to decide BEFORE checking anything out (dedup, skip
// unchanged, pin a build).
func LsRemote(spec LsRemoteSpec) activity.Call[LsRemoteReport] {
	return activity.Fn("git.ls-remote", lsRemoteActivity, spec)
}

func lsRemoteActivity(ctx context.Context, spec LsRemoteSpec) (LsRemoteReport, error) {
	if spec.Repository == "" {
		return LsRemoteReport{}, errors.New("ls-remote: repository is required")
	}
	extraArgs, extraEnv, cleanup, err := gitEnv(ctx, spec.Auth)
	if err != nil {
		return LsRemoteReport{}, err
	}
	defer cleanup()
	args := []string{"ls-remote", spec.Repository}
	if spec.Ref != "" {
		args = append(args, spec.Ref)
	} else {
		args = append(args, "HEAD")
	}
	out, err := runGit(ctx, "", extraArgs, extraEnv, args...)
	if err != nil {
		return LsRemoteReport{}, err
	}
	line := strings.SplitN(strings.TrimSpace(out), "\n", 2)[0]
	fields := strings.Fields(line)
	if len(fields) == 0 {
		return LsRemoteReport{}, errors.New("ls-remote: ref not found on the remote")
	}
	return LsRemoteReport{Commit: fields[0], Ref: spec.Ref}, nil
}

// TagSpec puts a tag on a checked-out tree.
type TagSpec struct {
	// Dir is the working tree (a Checkout report's Dir).
	Dir string `json:"dir"`
	// Name is the tag ("v1.4.0").
	Name string `json:"name"`
	// Message makes the tag annotated; empty means lightweight.
	Message string `json:"message,omitempty"`
	// Push pushes the tag to origin.
	Push bool `json:"push,omitempty"`
	Auth Auth `json:"auth,omitempty"`
}

// TagReport is the tagged commit.
type TagReport struct {
	Name   string `json:"name"`
	Commit string `json:"commit"`
}

// Tag marks the current commit — the release move. Converging: the
// same tag on the same commit is a no-op, on a different commit an
// error.
func Tag(spec TagSpec) activity.Call[TagReport] {
	return activity.Fn("git.tag", tagActivity, spec)
}

func tagActivity(ctx context.Context, spec TagSpec) (TagReport, error) {
	if spec.Dir == "" || spec.Name == "" {
		return TagReport{}, errors.New("tag: dir and name are required")
	}
	head, err := runGit(ctx, spec.Dir, nil, nil, "rev-parse", "HEAD")
	if err != nil {
		return TagReport{}, err
	}
	if existing, err := runGit(ctx, spec.Dir, nil, nil, "rev-parse", "--verify", "--quiet", spec.Name+"^{commit}"); err == nil {
		if existing != head {
			return TagReport{}, errors.New("tag " + spec.Name + " already marks a different commit")
		}
	} else {
		args := []string{"tag"}
		if spec.Message != "" {
			args = append(args, "-a", "-m", spec.Message)
		}
		args = append(args, spec.Name)
		if _, err := runGit(ctx, spec.Dir, nil, nil, args...); err != nil {
			return TagReport{}, err
		}
	}
	if spec.Push {
		extraArgs, extraEnv, cleanup, err := gitEnv(ctx, spec.Auth)
		if err != nil {
			return TagReport{}, err
		}
		defer cleanup()
		if _, err := runGit(ctx, spec.Dir, extraArgs, extraEnv, "push", "origin", "refs/tags/"+spec.Name); err != nil {
			return TagReport{}, err
		}
	}
	return TagReport{Name: spec.Name, Commit: head}, nil
}

// CommitSpec records changes in a checked-out tree — the GitOps move:
// bump a manifest, commit, push.
type CommitSpec struct {
	Dir     string `json:"dir"`
	Message string `json:"message"`
	// Paths limits what is staged; empty stages everything changed.
	Paths []string `json:"paths,omitempty"`
	// AuthorName/AuthorEmail identify the committer; both required.
	AuthorName  string `json:"authorName"`
	AuthorEmail string `json:"authorEmail"`
	Push        bool   `json:"push,omitempty"`
	Auth        Auth   `json:"auth,omitempty"`
}

// CommitReport is the created commit; Committed is false when the tree
// was already clean.
type CommitReport struct {
	Commit    string `json:"commit"`
	Committed bool   `json:"committed"`
}

// Commit stages, commits, and optionally pushes. A clean tree is a
// successful no-op — convergence, not ceremony.
func Commit(spec CommitSpec) activity.Call[CommitReport] {
	return activity.Fn("git.commit", commitActivity, spec)
}

func commitActivity(ctx context.Context, spec CommitSpec) (CommitReport, error) {
	if spec.Dir == "" || spec.Message == "" {
		return CommitReport{}, errors.New("commit: dir and message are required")
	}
	if spec.AuthorName == "" || spec.AuthorEmail == "" {
		return CommitReport{}, errors.New("commit: author name and email are required")
	}
	add := append([]string{"add", "--"}, spec.Paths...)
	if len(spec.Paths) == 0 {
		add = []string{"add", "-A"}
	}
	if _, err := runGit(ctx, spec.Dir, nil, nil, add...); err != nil {
		return CommitReport{}, err
	}
	status, err := runGit(ctx, spec.Dir, nil, nil, "status", "--porcelain")
	if err != nil {
		return CommitReport{}, err
	}
	head, headErr := runGit(ctx, spec.Dir, nil, nil, "rev-parse", "HEAD")
	if status == "" && headErr == nil {
		return CommitReport{Commit: head, Committed: false}, nil
	}
	ident := []string{
		"-c", "user.name=" + spec.AuthorName,
		"-c", "user.email=" + spec.AuthorEmail,
	}
	if _, err := runGit(ctx, spec.Dir, ident, nil, "commit", "--quiet", "-m", spec.Message); err != nil {
		return CommitReport{}, err
	}
	commit, err := runGit(ctx, spec.Dir, nil, nil, "rev-parse", "HEAD")
	if err != nil {
		return CommitReport{}, err
	}
	if spec.Push {
		extraArgs, extraEnv, cleanup, err := gitEnv(ctx, spec.Auth)
		if err != nil {
			return CommitReport{}, err
		}
		defer cleanup()
		if _, err := runGit(ctx, spec.Dir, extraArgs, extraEnv, "push", "origin", "HEAD"); err != nil {
			return CommitReport{}, err
		}
	}
	return CommitReport{Commit: commit, Committed: true}, nil
}
