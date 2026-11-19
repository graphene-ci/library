// Package contract shares the host resource wire types with adapters.
package contract

import (
	"encoding/json"

	"github.com/graphene-ci/pipeline/pkg/flow/ownership"
	"github.com/graphene-ci/pipeline/pkg/ref"
)

// DeclareActivity is the host resource declaration activity: one name for
// every host kind, the request says which.
const DeclareActivity = "host.declare"

// Entity kinds.
const (
	SysctlKind    = "host-sysctl"
	SysfsKind     = "host-sysfs"
	DirKind       = "host-dir"
	UnitKind      = "host-unit"
	UnitStateKind = "host-unit-state"
	MountKind     = "host-mount"
)

// Tree carries the record's place in the ownership tree; every spec has it.
type Tree struct {
	Owner ref.OwnerRef     `json:"owner,omitempty"`
	Flows []ownership.Flow `json:"flows,omitempty"`
}

// SysctlSpec sets kernel parameters while the record lives.
type SysctlSpec struct {
	Tree
	// Values maps a sysctl key (vm.swappiness, net.core.somaxconn) to the
	// value written while the record lives.
	Values map[string]string `json:"values"`
}

// SysfsSpec sets one /sys attribute while the record lives.
type SysfsSpec struct {
	Tree
	Path  string `json:"path"`
	Value string `json:"value"`
}

// DirSpec is a directory the record creates and removes with its contents.
type DirSpec struct {
	Tree
	Path string `json:"path"`
	Mode uint32 `json:"mode,omitempty"`
	UID  int    `json:"uid"`
	GID  int    `json:"gid"`
}

// UnitSpec is a systemd unit the record installs, enables and starts.
type UnitSpec struct {
	Tree
	Name    string `json:"name"`
	Content []byte `json:"content"`
}

// UnitStateSpec changes the state of a unit the record does not own.
type UnitStateSpec struct {
	Tree
	Name string `json:"name"`
	Want string `json:"want"`
}

// MountSpec mounts a filesystem the record holds, or checks one it does
// not (Check).
type MountSpec struct {
	Tree
	Device     string   `json:"device"`
	Mountpoint string   `json:"mountpoint"`
	FSType     string   `json:"fsType"`
	Options    []string `json:"options,omitempty"`
	Format     bool     `json:"format,omitempty"`
	Check      bool     `json:"check,omitempty"`
}

// DeclareRequest starts or attaches to a host resource of Kind.
type DeclareRequest struct {
	Kind   string            `json:"kind"`
	Name   string            `json:"name"`
	Labels map[string]string `json:"labels,omitempty"`
	RunId  string            `json:"runId,omitempty"`
	Spec   json.RawMessage   `json:"spec"`
}
