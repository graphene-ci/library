// Package file is the library resource for a FILE on a machine — the
// inverse of an artifact. artifact reads bytes FROM a machine into a
// blob; a File writes bytes ONTO a machine, and the file itself is a
// record in the tree (owned, cascaded, observed): Init writes it,
// Finalize removes it. A container mounts it (docker.WithFileMount).
//
// Content sources come from package file (pipeline/pkg/file): FromBytes,
// FromEmbed (the user ships config via //go:embed). Secret/Artifact
// sources are declared but resolved on the machine — not yet materialized
// here (a clear error until wired), so a value never sits in the spec.
package file

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"time"

	"github.com/graphene-ci/temporal-entity/pkg/entclient"
	"github.com/graphene-ci/temporal-entity/pkg/entdefine"
	entity "github.com/graphene-ci/temporal-entity/pkg/entity"
	temporalactivity "go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"

	"github.com/graphene-ci/pipeline/pkg/file"
	"github.com/graphene-ci/pipeline/pkg/flow/ownership"
	"github.com/graphene-ci/pipeline/pkg/machine"
	"github.com/graphene-ci/pipeline/pkg/pipeline"
	"github.com/graphene-ci/pipeline/pkg/ref"
	"github.com/graphene-ci/pipeline/pkg/wire"
)

// FileKind is the machine-file resource.
const FileKind = entity.KindName("file")

// Info is the output of a File resource.
type Info struct {
	Path string `json:"path"`
}

// fileSpec is what a File record IS.
type fileSpec struct {
	Path string `json:"path"`
	// Content is inline bytes (FromBytes/FromEmbed). Small configs only;
	// a secret's value never travels here.
	Content []byte `json:"content,omitempty"`
	// Secret/Artifact name a content source resolved on the machine.
	Secret   string          `json:"secret,omitempty"`
	Artifact string          `json:"artifact,omitempty"`
	Mode     uint32          `json:"mode,omitempty"`
	Owner    ref.OwnerRef    `json:"owner,omitempty"`
	Flows    []ownership.Flow `json:"flows,omitempty"`
}

type fileState struct {
	ownership.State
	Info Info `json:"info"`
}

// Write/remove activity names — the library's wire identities.
const (
	writeActivityName  = "file.write"
	removeActivityName = "file.remove"
)

func entityActivityCtx(ctx workflow.Context) workflow.Context {
	return workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 5 * time.Minute,
	})
}

func fileDef() *entdefine.Definition[fileSpec, fileState] {
	def := entdefine.New[fileSpec, fileState](FileKind,
		entdefine.WithSearchAttributes[fileSpec, fileState](true),
		entdefine.WithInit[fileSpec, fileState](func(ctx workflow.Context, spec fileSpec) (fileState, error) {
			var st fileState
			if spec.Owner != "" {
				ownership.Init(ctx, &st.State, spec.Owner)
			}
			st.State.Flows = spec.Flows
			if err := workflow.ExecuteActivity(entityActivityCtx(ctx), writeActivityName, spec).Get(ctx, &st.Info); err != nil {
				return st, err
			}
			return st, nil
		}),
		entdefine.WithFinalize[fileSpec, fileState](func(ctx workflow.Context, st *fileState) error {
			if st.Info.Path == "" {
				return nil
			}
			return workflow.ExecuteActivity(entityActivityCtx(ctx), removeActivityName, st.Info.Path).Get(ctx, nil)
		}),
	)
	ownership.Register(def, func(s *fileState) *ownership.State { return &s.State })
	return def
}

// writeActivity materializes the file on the machine. Runs inside the
// per-(agent × run) container, so the path is the machine's own.
func writeActivity(ctx context.Context, spec fileSpec) (Info, error) {
	switch {
	case spec.Secret != "":
		return Info{}, fmt.Errorf("file %q: secret source resolves on the machine — not wired yet", spec.Path)
	case spec.Artifact != "":
		return Info{}, fmt.Errorf("file %q: artifact source not wired yet", spec.Path)
	}
	host := machine.Path(spec.Path)
	if err := os.MkdirAll(filepath.Dir(host), 0o755); err != nil { //nolint:gosec // config dirs are world-readable
		return Info{}, fmt.Errorf("mkdir for %s: %w", spec.Path, err)
	}
	mode := os.FileMode(spec.Mode)
	if mode == 0 {
		mode = 0o644
	}
	if err := os.WriteFile(host, spec.Content, mode); err != nil {
		return Info{}, fmt.Errorf("write %s: %w", spec.Path, err)
	}
	return Info{Path: spec.Path}, nil
}

// removeActivity deletes the file; absence is success (retry-safe).
func removeActivity(ctx context.Context, path string) error {
	if err := os.Remove(machine.Path(path)); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("remove %s: %w", path, err)
	}
	return nil
}

// declareRequest asks the declare activity for one file.
type declareRequest struct {
	Name   string            `json:"name"`
	Labels map[string]string `json:"labels,omitempty"`
	RunId  string            `json:"runId,omitempty"`
	Spec   json.RawMessage   `json:"spec"`
}

const declareActivityName = "file.declare"

var declared *entdefine.Definition[fileSpec, fileState]

func recordEntities(ctx pipeline.Context) {
	allDims := []string{"state", "events", "logs", "metrics", "traces"}
	ctx.RecordKindInfo(string(FileKind),
		"a file on the agent's machine, written and removed with the record", reflect.TypeOf(fileSpec{}), allDims)
	ctx.RecordActivity(writeActivityName, writeActivity)
	ctx.RecordActivity(removeActivityName, removeActivity)
	ctx.RecordWorker(func(w worker.Worker, cl client.Client) error {
		if declared == nil {
			declared = fileDef()
		}
		if err := declared.Register(w); err != nil {
			return err
		}
		w.RegisterActivityWithOptions(makeDeclare(cl), temporalactivity.RegisterOptions{Name: declareActivityName})
		return nil
	})
}

func makeDeclare(cl client.Client) func(context.Context, declareRequest) (json.RawMessage, error) {
	return func(ctx context.Context, req declareRequest) (json.RawMessage, error) {
		if err := wire.ValidateUserLabels(req.Labels); err != nil {
			return nil, err
		}
		queue := temporalactivity.GetInfo(ctx).TaskQueue
		labels := map[string]string{}
		for k, v := range req.Labels {
			labels[k] = v
		}
		if req.RunId != "" {
			labels["graphene.io/run"] = req.RunId
		}
		var spec fileSpec
		if err := json.Unmarshal(req.Spec, &spec); err != nil {
			return nil, err
		}
		rid := entity.ResourceID(req.Name)
		bound := entclient.Bind(declared, cl, queue)
		if _, err := bound.CreateOrAttach(ctx, rid, spec, entclient.WithLabels(labels)); err != nil {
			return nil, err
		}
		for {
			desc, err := bound.Describe(ctx, rid)
			if err != nil {
				return nil, err
			}
			switch desc.Phase {
			case entity.PhaseReady:
				return json.Marshal(desc.State.Info)
			case entity.PhaseCreateFailed:
				return nil, fmt.Errorf("file %s failed to write", req.Name)
			case entity.PhaseDeleting, entity.PhaseDeleted, entity.PhaseDeleteFailed:
				return nil, fmt.Errorf("file %s is going away (phase %s)", req.Name, desc.Phase)
			case entity.PhaseCreating:
			}
			temporalactivity.RecordHeartbeat(ctx, string(desc.Phase))
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(time.Second):
			}
		}
	}
}

// File declares a file on the agent's machine as a resource. Its bytes
// come from the source (FromBytes/FromEmbed); the record is owned by the
// agent's handle when the agent is ours, so the file dies with it.
func File(ctx pipeline.Context, agent pipeline.Agent, path string, src file.Source, opts ...pipeline.ResourceOption) pipeline.Resource[Info] {
	self := ref.OwnerRef(string(FileKind) + "/" + fileId(agent, path))
	if ctx.Recording() {
		recordEntities(ctx)
		ctx.RecordDeclare(self, pipeline.BuildResourceOptions(ctx, planOpts(agent, opts)))
		return pipeline.NewResource[Info](ctx, self, nil)
	}
	if err := src.Validate(); err != nil {
		return pipeline.FailedResource[Info](ctx, self, fmt.Errorf("file %q: %w", path, err))
	}
	if h, ok := agent.(pipeline.Handle); ok {
		opts = append([]pipeline.ResourceOption{pipeline.Parent(h)}, opts...)
	}
	o := pipeline.BuildResourceOptions(ctx, opts)
	spec := fileSpec{Path: path, Content: src.Bytes, Secret: src.Secret, Artifact: src.Artifact, Owner: o.Parent, Flows: o.Flows}
	raw, _ := json.Marshal(spec)
	fut := pipeline.DispatchOnAgent(ctx, agent.AgentId(), dispatchOpts(), declareActivityName, declareRequest{
		Name: fileId(agent, path), Labels: o.Labels, RunId: string(ctx.RunId()), Spec: raw,
	})
	return pipeline.NewResource[Info](ctx, self, fut)
}

// fileId names the record: agent id + a path slug, so two files on the
// same machine do not collide.
func fileId(agent pipeline.Agent, path string) string {
	slug := ""
	for _, r := range path {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '-', r == '.':
			slug += string(r)
		default:
			slug += "-"
		}
	}
	return string(agent.AgentId()) + slug
}

func dispatchOpts() workflow.ActivityOptions {
	return workflow.ActivityOptions{StartToCloseTimeout: 10 * time.Minute, HeartbeatTimeout: time.Minute}
}

func planOpts(agent pipeline.Agent, opts []pipeline.ResourceOption) []pipeline.ResourceOption {
	if h, ok := agent.(pipeline.Handle); ok {
		return append([]pipeline.ResourceOption{pipeline.Parent(h)}, opts...)
	}
	return opts
}
