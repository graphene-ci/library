// Package k8stest supplies typed live objects to Graphene pipeline tests.
// It evaluates the production readiness predicate; it does not emulate a
// Kubernetes API server, Crossplane reconciliation, or cloud-provider behavior.
package k8stest

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/graphene-ci/library/k8s/internal/contract"
	"github.com/graphene-ci/pipeline/pkg/id"
	"github.com/graphene-ci/pipeline/pkg/pipelinetest"
	"github.com/graphene-ci/pipeline/pkg/ref"
	"github.com/graphene-ci/pipeline/pkg/wire"
	"go.temporal.io/sdk/workflow"
)

// Objects holds fixtures isolated to one test environment.
type Objects struct {
	world     *pipelinetest.World
	mu        sync.Mutex
	live      map[ref.OwnerRef]map[string]any
	fallback  func(kind, name string, manifest map[string]any) any
	knowledge map[string]contract.Knowledge
}

// Install attaches the adapter before pipelinetest.Workflow is prepared.
func Install(world *pipelinetest.World) *Objects {
	o := &Objects{world: world, live: map[ref.OwnerRef]map[string]any{}}
	world.OnPrepare(func(r pipelinetest.Registration) {
		o.knowledge, _ = r.Registration(contract.RegistrationKey).(map[string]contract.Knowledge)
	})
	pipelinetest.Handle1(world, contract.DeclareActivity, o.declare)
	return o
}

// Set supplies observed fields for kind/id. Omitted fields are taken from the
// declared manifest, so status-only fixtures and omitted TypeMeta work too.
// The object is copied immediately; changes later require another Set call.
func (o *Objects) Set(name ref.OwnerRef, live any) error {
	raw, err := json.Marshal(live)
	if err != nil {
		return err
	}
	var object map[string]any
	if err := json.Unmarshal(raw, &object); err != nil {
		return err
	}
	if object == nil {
		return fmt.Errorf("live object must not be nil")
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	o.live[name] = object
	return nil
}

// SetDefault answers for every object the test did not name with Set: the
// function receives the declared kind, name and manifest and returns the
// observed fields (nil — nothing observed yet, the object waits). It is how
// a test meets whatever a RunSpec declares without knowing the names in
// advance; a Set for a specific object still wins.
func (o *Objects) SetDefault(fn func(kind, name string, manifest map[string]any) any) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.fallback = fn
}

// observed answers the fields the test supplied for name: an explicit
// fixture first, the default otherwise.
func (o *Objects) observed(kind, name string, ref ref.OwnerRef, manifest map[string]any) (map[string]any, bool, error) {
	o.mu.Lock()
	object, exists := o.live[ref]
	fallback := o.fallback
	o.mu.Unlock()
	if exists || fallback == nil {
		return object, exists, nil
	}
	live := fallback(kind, name, overlay(manifest, nil))
	if live == nil {
		return nil, false, nil
	}
	raw, err := json.Marshal(live)
	if err != nil {
		return nil, false, err
	}
	var out map[string]any
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil, false, err
	}
	return out, out != nil, nil
}

func (o *Objects) declare(ctx workflow.Context, req contract.DeclareRequest) (any, error) {
	if queue := workflow.GetActivityOptions(ctx).TaskQueue; queue != req.TaskQueue || queue != wire.RunQueue(id.RunId(req.RunId)) {
		return nil, fmt.Errorf("kubernetes declaration must run on its run queue, got %q", queue)
	}
	name := ref.OwnerRef(req.Kind + "/" + req.Name)
	knowledge, ok := o.knowledge[req.Kind]
	if !ok {
		return nil, fmt.Errorf("kind %s was not discovered", req.Kind)
	}
	raw, _ := json.Marshal(req.Spec)
	if err := o.world.Declare(ctx, pipelinetest.Resource{Ref: name, Owner: req.Spec.Owner, Spec: raw, Labels: req.Labels}); err != nil {
		return nil, err
	}
	deadline := workflow.Now(ctx).Add(knowledge.Timeout)
	for {
		if err := o.world.Failure(name); err != nil {
			return nil, err
		}
		observed, exists, err := o.observed(req.Kind, req.Name, name, req.Spec.Manifest)
		if err != nil {
			return nil, err
		}
		if exists {
			live := overlay(req.Spec.Manifest, observed)
			ready, err := knowledge.Ready(live)
			if err != nil {
				return nil, err
			}
			if ready {
				state := contract.State{Live: live, Kubeconfig: req.Spec.Kubeconfig}
				return state, o.world.Ready(ctx, name, state)
			}
		}
		remaining := deadline.Sub(workflow.Now(ctx))
		if remaining <= 0 {
			return nil, fmt.Errorf("%s did not become ready within %s (supply a live fixture satisfying WithReady)", name, knowledge.Timeout)
		}
		if err := workflow.Sleep(ctx, min(knowledge.PollInterval, remaining)); err != nil {
			return nil, err
		}
	}
}

// overlay copies both sides through JSON before merging. Neither the recorded
// spec nor a fixture is mutable through the object passed to a readiness hook.
func overlay(desired, observed map[string]any) map[string]any {
	raw, _ := json.Marshal(desired)
	var live map[string]any
	_ = json.Unmarshal(raw, &live)
	if live == nil {
		live = map[string]any{}
	}
	for key, value := range observed {
		if nested, ok := value.(map[string]any); ok {
			base, _ := live[key].(map[string]any)
			live[key] = overlay(base, nested)
		} else {
			raw, _ := json.Marshal(value)
			var copy any
			_ = json.Unmarshal(raw, &copy)
			live[key] = copy
		}
	}
	return live
}

// After supplies a live object at virtual test time. Conversion is performed now,
// so invalid fixtures fail during test setup rather than inside a callback.
func (o *Objects) After(delay time.Duration, name ref.OwnerRef, live any) error {
	raw, err := json.Marshal(live)
	if err != nil {
		return err
	}
	var object map[string]any
	if err := json.Unmarshal(raw, &object); err != nil {
		return err
	}
	if object == nil {
		return fmt.Errorf("live object must not be nil")
	}
	o.world.Env.RegisterDelayedCallback(func() {
		o.mu.Lock()
		defer o.mu.Unlock()
		o.live[name] = object
	}, delay)
	return nil
}
