package host

import (
	"context"
	"fmt"
	"path"
	"strings"

	"github.com/graphene-ci/library/host/internal/contract"
)

// SysfsInfo is the output of a Sysfs resource.
type SysfsInfo struct {
	Path  string `json:"path"`
	Prior string `json:"prior"`
}

type sysfsSpec = contract.SysfsSpec

func validateSysfs(spec sysfsSpec) error {
	if path.Clean(spec.Path) != spec.Path || !strings.HasPrefix(spec.Path, "/sys/") {
		return fmt.Errorf("sysfs path %q: want a clean path under /sys/", spec.Path)
	}
	if spec.Value == "" || strings.ContainsAny(spec.Value, "\n\r") {
		return fmt.Errorf("sysfs %s: value must be one non-empty line", spec.Path)
	}
	return nil
}

// selected reads a sysfs attribute's current value. A choice attribute
// lists every option and brackets the active one —
// "always [madvise] never" — and the bracketed word is what writing back
// restores.
func selected(raw string) string {
	open := strings.IndexByte(raw, '[')
	if open < 0 {
		return raw
	}
	end := strings.IndexByte(raw[open:], ']')
	if end < 0 {
		return raw
	}
	return raw[open+1 : open+end]
}

func (m Machine) applySysfs(_ context.Context, req applyRequest[sysfsSpec]) (SysfsInfo, error) {
	if err := validateSysfs(req.Spec); err != nil {
		return SysfsInfo{}, err
	}
	f, err := m.open()
	if err != nil {
		return SysfsInfo{}, err
	}
	defer f.Close()
	c, err := f.take(contract.SysfsKind, req.Spec.Path, req.Record, func() (string, error) {
		raw, err := f.readTrimmed(req.Spec.Path)
		return selected(raw), err
	})
	if err != nil {
		return SysfsInfo{}, err
	}
	if err := f.writeValue(req.Spec.Path, req.Spec.Value); err != nil {
		return SysfsInfo{}, err
	}
	return SysfsInfo{Path: req.Spec.Path, Prior: c.Prior}, nil
}

func (m Machine) removeSysfs(_ context.Context, req applyRequest[sysfsSpec]) error {
	f, err := m.open()
	if err != nil {
		return err
	}
	defer f.Close()
	c, ours, err := f.held(contract.SysfsKind, req.Spec.Path, req.Record)
	if err != nil || !ours {
		return err
	}
	if err := f.writeValue(req.Spec.Path, c.Prior); err != nil {
		return fmt.Errorf("restore %s: %w", req.Spec.Path, err)
	}
	return f.drop(contract.SysfsKind, req.Spec.Path, req.Record)
}
