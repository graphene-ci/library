package gitlib

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// seedRepo builds a real repository to run the verbs against.
func seedRepo(t *testing.T) (repoDir, commit string) {
	t.Helper()
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("no git on this machine")
	}
	repoDir = t.TempDir()
	run := func(args ...string) string {
		t.Helper()
		cmd := exec.Command("git", args...)
		cmd.Dir = repoDir
		cmd.Env = append(os.Environ(),
			"GIT_AUTHOR_NAME=t", "GIT_AUTHOR_EMAIL=t@t",
			"GIT_COMMITTER_NAME=t", "GIT_COMMITTER_EMAIL=t@t")
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("git %v: %v: %s", args, err, out)
		}
		return strings.TrimSpace(string(out))
	}
	run("init", "--quiet", "--initial-branch=main")
	if err := os.WriteFile(filepath.Join(repoDir, "hello.txt"), []byte("hi\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	run("add", "-A")
	run("commit", "--quiet", "-m", "seed")
	return repoDir, run("rev-parse", "HEAD")
}

func TestCheckoutConverges(t *testing.T) {
	repo, want := seedRepo(t)
	dir := filepath.Join(t.TempDir(), "work")
	ctx := context.Background()

	out, err := checkoutActivity(ctx, Spec{Repository: repo, Ref: "main", Dir: dir})
	if err != nil {
		t.Fatalf("checkout: %v", err)
	}
	if out.Commit != want || out.Dir != dir {
		t.Fatalf("checkout got %+v, want commit %s", out, want)
	}
	// Second run over the same directory converges instead of failing.
	if _, err := os.Create(filepath.Join(dir, "trash.txt")); err != nil {
		t.Fatal(err)
	}
	out, err = checkoutActivity(ctx, Spec{Repository: repo, Ref: "main", Dir: dir, Clean: true})
	if err != nil {
		t.Fatalf("re-checkout: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "trash.txt")); err == nil {
		t.Fatal("Clean must scrub untracked files")
	}
	// A pinned commit that no longer matches fails without retries.
	if _, err := checkoutActivity(ctx, Spec{Repository: repo, Ref: "main", Dir: dir, ExpectedCommit: strings.Repeat("0", 40)}); err == nil {
		t.Fatal("expected the commit-mismatch error")
	}
}

func TestLsRemote(t *testing.T) {
	repo, want := seedRepo(t)
	out, err := lsRemoteActivity(context.Background(), LsRemoteSpec{Repository: repo, Ref: "main"})
	if err != nil {
		t.Fatalf("ls-remote: %v", err)
	}
	if out.Commit != want {
		t.Fatalf("ls-remote got %s, want %s", out.Commit, want)
	}
}

func TestTagAndCommit(t *testing.T) {
	repo, _ := seedRepo(t)
	dir := filepath.Join(t.TempDir(), "work")
	ctx := context.Background()
	if _, err := checkoutActivity(ctx, Spec{Repository: repo, Ref: "main", Dir: dir}); err != nil {
		t.Fatal(err)
	}

	tag, err := tagActivity(ctx, TagSpec{Dir: dir, Name: "v1.0.0", Message: "release"})
	if err != nil {
		t.Fatalf("tag: %v", err)
	}
	// Same tag, same commit: converged no-op.
	if _, err := tagActivity(ctx, TagSpec{Dir: dir, Name: "v1.0.0"}); err != nil {
		t.Fatalf("re-tag: %v", err)
	}

	if err := os.WriteFile(filepath.Join(dir, "values.yaml"), []byte("image: v2\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	out, err := commitActivity(ctx, CommitSpec{
		Dir: dir, Message: "bump", AuthorName: "bot", AuthorEmail: "bot@ci",
	})
	if err != nil {
		t.Fatalf("commit: %v", err)
	}
	if !out.Committed || out.Commit == tag.Commit {
		t.Fatalf("commit must move HEAD: %+v", out)
	}
	// Clean tree: successful no-op.
	again, err := commitActivity(ctx, CommitSpec{
		Dir: dir, Message: "bump", AuthorName: "bot", AuthorEmail: "bot@ci",
	})
	if err != nil || again.Committed {
		t.Fatalf("clean tree must be a no-op: %+v %v", again, err)
	}
}

func TestRepoName(t *testing.T) {
	for in, want := range map[string]string{
		"https://github.com/acme/service.git": "service",
		"git@github.com:acme/service.git":     "service",
		"https://host/group/repo/":            "repo",
	} {
		if got := repoName(in); got != want {
			t.Fatalf("repoName(%q) = %q, want %q", in, got, want)
		}
	}
}
