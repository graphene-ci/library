// Package gitlib is the activity library over git ON THE MACHINE: the
// machine's own git does the work through the machine contract (chroot
// from the hosted container, plain exec on the machine itself). One
// verb per CI move — Checkout, LsRemote, Tag, Commit — plus Install to
// bring git where it is missing. Everything else of git deliberately
// stays unwrapped: arbitrary operations are machine.Command in the
// user's own activity.
package gitlib

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"time"

	temporalactivity "go.temporal.io/sdk/activity"

	"github.com/graphene-ci/pipeline/pkg/activity"
	"github.com/graphene-ci/pipeline/pkg/capabilityapi"
	"github.com/graphene-ci/pipeline/pkg/machine"
	"github.com/graphene-ci/pipeline/pkg/obs"
	"github.com/graphene-ci/pipeline/pkg/pipeline"
	"github.com/graphene-ci/pipeline/pkg/ref"
	"github.com/graphene-ci/pipeline/pkg/secretsapi"
)

// errTailBytes bounds the output tail embedded in error messages: the
// failure must be diagnosable straight from the Temporal history event,
// but history is written on every retry — the FULL output lives in the
// log stream, only the tail travels with the error.
const errTailBytes = 2048

// InstallReport is what Install returns.
type InstallReport struct {
	Version string `json:"version"`
	// Installed is false when git was already there.
	Installed bool `json:"installed"`
}

// Install brings git onto the machine — converging: a git that is
// already there is reported, not reinstalled. The machine's own
// package manager does the work.
func Install() activity.Call[InstallReport] {
	return activity.Fn0("git.install", func(ctx context.Context) (InstallReport, error) {
		if version, err := gitVersion(ctx); err == nil {
			if err := publish(ctx, version); err != nil {
				return InstallReport{}, err
			}
			return InstallReport{Version: version}, nil
		}
		script := `set -e
if command -v apt-get >/dev/null 2>&1; then apt-get update -qq && apt-get install -y -qq git
elif command -v dnf >/dev/null 2>&1; then dnf install -y -q git
elif command -v yum >/dev/null 2>&1; then yum install -y -q git
elif command -v apk >/dev/null 2>&1; then apk add --no-cache git
elif command -v zypper >/dev/null 2>&1; then zypper --non-interactive install git
else echo "no known package manager" >&2; exit 1
fi`
		if out, err := obs.RunTail(ctx, machine.Shell(ctx, script), errTailBytes); err != nil {
			return InstallReport{}, fmt.Errorf("install git: %w: %s", err, out)
		}
		version, err := gitVersion(ctx)
		if err != nil {
			return InstallReport{}, err
		}
		if err := publish(ctx, version); err != nil {
			return InstallReport{}, err
		}
		return InstallReport{Version: version, Installed: true}, nil
	})
}

// publish records the capability on the machine's record.
func publish(ctx context.Context, version string) error {
	return capabilityapi.PublishSelf(ctx, pipeline.Capability{
		Name:      "git",
		Version:   version,
		BroughtBy: "gitlib.Install",
		Ready:     true,
	})
}

func gitVersion(ctx context.Context) (string, error) {
	bin, err := gitBinary()
	if err != nil {
		return "", err
	}
	out, err := machine.Command(ctx, bin, "--version").Output()
	if err != nil {
		return "", fmt.Errorf("git not available: %w", err)
	}
	return strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(string(out)), "git version ")), nil
}

// gitBinary locates the machine's git. On the machine itself normal
// PATH lookup applies; through the chroot the path must be absolute
// and is probed against the machine's filesystem.
func gitBinary() (string, error) {
	if machine.Root() == "" {
		return "git", nil
	}
	for _, p := range []string{"/usr/bin/git", "/usr/local/bin/git", "/bin/git"} {
		if _, err := os.Stat(machine.Path(p)); err == nil {
			return p, nil
		}
	}
	return "", errors.New("git not found on the machine (gitlib.Install brings it)")
}

// Auth carries credential REFERENCES — values resolve inside the
// activity body, never enter specs or history, and never persist in
// the repository's config.
type Auth struct {
	// TokenSecret names an HTTPS token.
	TokenSecret ref.SecretRef `json:"tokenSecret,omitempty"`
	// TokenUser is the basic-auth user the token pairs with; empty
	// means "x-access-token" (github; gitlab wants "oauth2").
	TokenUser string `json:"tokenUser,omitempty"`
	// SSHKeySecret names an SSH private key.
	SSHKeySecret ref.SecretRef `json:"sshKeySecret,omitempty"`
}

// gitEnv resolves Auth into per-invocation configuration: extra args
// (an ephemeral http.extraHeader — nothing lands in .git/config) and
// env (GIT_SSH_COMMAND with a key file). cleanup removes the key file.
func gitEnv(ctx context.Context, auth Auth) (args []string, env []string, cleanup func(), err error) {
	cleanup = func() {}
	if auth.TokenSecret.Name != "" {
		token, err := secretsapi.Resolve(ctx, auth.TokenSecret)
		if err != nil {
			return nil, nil, cleanup, err
		}
		user := auth.TokenUser
		if user == "" {
			user = "x-access-token"
		}
		basic := base64.StdEncoding.EncodeToString([]byte(user + ":" + token))
		args = append(args, "-c", "http.extraHeader=Authorization: Basic "+basic)
	}
	if auth.SSHKeySecret.Name != "" {
		key, err := secretsapi.Resolve(ctx, auth.SSHKeySecret)
		if err != nil {
			return nil, nil, cleanup, err
		}
		dir, err := os.MkdirTemp(machine.Workspace(), ".git-ssh-")
		if err != nil {
			return nil, nil, cleanup, err
		}
		keyFile := dir + "/key"
		if err := os.WriteFile(keyFile, []byte(key), 0o600); err != nil {
			_ = os.RemoveAll(dir)
			return nil, nil, cleanup, err
		}
		cleanup = func() { _ = os.RemoveAll(dir) }
		env = append(env,
			"GIT_SSH_COMMAND=ssh -i "+keyFile+" -o StrictHostKeyChecking=accept-new")
	}
	return args, env, cleanup, nil
}

// runGit executes one git invocation on the machine; its output is
// DATA (rev-parse, status) — captured, never streamed.
func runGit(ctx context.Context, dir string, extraArgs, extraEnv []string, args ...string) (string, error) {
	cmd, err := gitCommand(ctx, dir, extraArgs, extraEnv, args...)
	if err != nil {
		return "", err
	}
	out, err := cmd.CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("git %s: %w: %s", firstWord(args), err, tail(string(out), errTailBytes))
	}
	return strings.TrimSpace(string(out)), nil
}

// streamGit executes one long git invocation (fetch, push, submodule),
// streaming its progress as live log records on the activity.
func streamGit(ctx context.Context, dir string, extraArgs, extraEnv []string, args ...string) error {
	cmd, err := gitCommand(ctx, dir, extraArgs, extraEnv, args...)
	if err != nil {
		return err
	}
	if out, err := obs.RunTail(ctx, cmd, errTailBytes); err != nil {
		return fmt.Errorf("git %s: %w: %s", firstWord(args), err, out)
	}
	return nil
}

func gitCommand(ctx context.Context, dir string, extraArgs, extraEnv []string, args ...string) (*exec.Cmd, error) {
	bin, err := gitBinary()
	if err != nil {
		return nil, err
	}
	full := append(append([]string{}, extraArgs...), args...)
	cmd := machine.Command(ctx, bin, full...)
	if dir != "" {
		cmd.Dir = dir
	}
	if len(extraEnv) > 0 {
		if cmd.Env == nil {
			cmd.Env = os.Environ()
		}
		cmd.Env = append(cmd.Env, extraEnv...)
	}
	return cmd, nil
}

func firstWord(args []string) string {
	if len(args) == 0 {
		return ""
	}
	return args[0]
}

func tail(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return "…" + s[len(s)-n:]
}

// heartbeat keeps the activity alive while a long command runs.
func heartbeat(ctx context.Context, message string) func() {
	if !temporalactivity.IsActivity(ctx) {
		return func() {}
	}
	done := make(chan struct{})
	go func() {
		ticker := time.NewTicker(15 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-done:
				return
			case <-ticker.C:
				temporalactivity.RecordHeartbeat(ctx, message)
			}
		}
	}()
	return func() { close(done) }
}
