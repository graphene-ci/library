// Package contract shares resource wire types and registration metadata.
package contract

import "time"

// RegistrationKey identifies immutable per-preparation readiness metadata.
const RegistrationKey = "k8s.knowledge"

// Knowledge contains the same predicate and bounds the entity init uses.
type Knowledge struct {
	Ready        func(map[string]any) (bool, error)
	Timeout      time.Duration
	PollInterval time.Duration
}
