package host

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"path"
	"regexp"
	"strings"

	"github.com/graphene-ci/library/host/internal/contract"
)

// UnitInfo is the output of a Unit resource.
type UnitInfo struct {
	Name string `json:"name"`
	Path string `json:"path"`
}

// UnitStateInfo is the output of a UnitState resource: the unit's state
// before the record changed it, restored on removal.
type UnitStateInfo struct {
	Name         string `json:"name"`
	PriorEnabled string `json:"priorEnabled"`
	PriorActive  string `json:"priorActive"`
}

// UnitWant is the state a UnitState record holds a unit in.
type UnitWant string

// The states a UnitState record can hold.
const (
	// UnitStopped stops the unit; it may start again on demand or at boot.
	UnitStopped UnitWant = "stopped"
	// UnitDisabled stops the unit and keeps it from starting at boot.
	UnitDisabled UnitWant = "disabled"
	// UnitMasked stops the unit and keeps anything from starting it.
	UnitMasked UnitWant = "masked"
)

type (
	unitSpec      = contract.UnitSpec
	unitStateSpec = contract.UnitStateSpec
)

const unitDir = "/etc/systemd/system"

var unitNamePattern = regexp.MustCompile(`^[A-Za-z0-9:_.@-]+\.(service|socket|timer|path|mount|automount|target|slice|scope)$`)

func validateUnitName(name string) error {
	if !unitNamePattern.MatchString(name) {
		return fmt.Errorf("unit name %q: want <name>.<type>", name)
	}
	return nil
}

func validateUnit(spec unitSpec) error {
	if err := validateUnitName(spec.Name); err != nil {
		return err
	}
	if len(spec.Content) == 0 {
		return fmt.Errorf("unit %s: empty unit file", spec.Name)
	}
	return nil
}

func validateUnitState(spec unitStateSpec) error {
	if err := validateUnitName(spec.Name); err != nil {
		return err
	}
	switch UnitWant(spec.Want) {
	case UnitStopped, UnitDisabled, UnitMasked:
		return nil
	default:
		return fmt.Errorf("unit %s: want %q, not one of stopped, disabled, masked", spec.Name, spec.Want)
	}
}

func (m Machine) systemctl(ctx context.Context, args ...string) error {
	_, err := m.runner.Run(ctx, "systemctl", args...)
	return err
}

// applyUnit installs the record's own unit file, enables and starts it. A
// unit file that exists and is not the record's is refused.
func (m Machine) applyUnit(ctx context.Context, req applyRequest[unitSpec]) (UnitInfo, error) {
	if err := validateUnit(req.Spec); err != nil {
		return UnitInfo{}, err
	}
	f, err := m.open()
	if err != nil {
		return UnitInfo{}, err
	}
	defer f.Close()
	file := path.Join(unitDir, req.Spec.Name)
	if _, ours, err := f.held(contract.UnitKind, req.Spec.Name, req.Record); err != nil {
		return UnitInfo{}, err
	} else if !ours {
		if _, err := f.r.Lstat(rel(file)); err == nil {
			return UnitInfo{}, conflict(contract.UnitKind, req.Spec.Name, "an existing unit file")
		} else if !errors.Is(err, fs.ErrNotExist) {
			return UnitInfo{}, err
		}
	}
	if _, err := f.take(contract.UnitKind, req.Spec.Name, req.Record, func() (string, error) { return "", nil }); err != nil {
		return UnitInfo{}, err
	}
	if err := f.r.MkdirAll(rel(unitDir), 0o750); err != nil {
		return UnitInfo{}, err
	}
	// systemd reads unit files as root: owner-only is enough.
	if err := f.writeFile(file, req.Spec.Content, 0o600); err != nil {
		return UnitInfo{}, err
	}
	if err := m.systemctl(ctx, "daemon-reload"); err != nil {
		return UnitInfo{}, err
	}
	if err := m.systemctl(ctx, "enable", "--now", req.Spec.Name); err != nil {
		return UnitInfo{}, err
	}
	return UnitInfo{Name: req.Spec.Name, Path: file}, nil
}

// removeUnit stops and disables the record's unit and deletes its file.
func (m Machine) removeUnit(ctx context.Context, req applyRequest[unitSpec]) error {
	f, err := m.open()
	if err != nil {
		return err
	}
	defer f.Close()
	_, ours, err := f.held(contract.UnitKind, req.Spec.Name, req.Record)
	if err != nil || !ours {
		return err
	}
	file := rel(path.Join(unitDir, req.Spec.Name))
	if _, err := f.r.Lstat(file); err == nil {
		if err := m.systemctl(ctx, "disable", "--now", req.Spec.Name); err != nil {
			return err
		}
		if err := f.r.Remove(file); err != nil && !errors.Is(err, fs.ErrNotExist) {
			return err
		}
	} else if !errors.Is(err, fs.ErrNotExist) {
		return err
	}
	if err := m.systemctl(ctx, "daemon-reload"); err != nil {
		return err
	}
	return f.drop(contract.UnitKind, req.Spec.Name, req.Record)
}

// unitPrior is what a UnitState record restores.
type unitPrior struct {
	Enabled string `json:"enabled"`
	Active  string `json:"active"`
}

// query reads a one-word systemctl answer. is-enabled and is-active report
// "disabled"/"inactive" with a non-zero exit — the word is the answer; an
// unknown unit answers nothing on stdout.
func (m Machine) query(ctx context.Context, verb, unit string) string {
	out, _ := m.runner.Run(ctx, "systemctl", verb, unit)
	if word := strings.TrimSpace(out); word != "" {
		return word
	}
	return "not-found"
}

// applyUnitState records the unit's state and holds it in the wanted one.
func (m Machine) applyUnitState(ctx context.Context, req applyRequest[unitStateSpec]) (UnitStateInfo, error) {
	if err := validateUnitState(req.Spec); err != nil {
		return UnitStateInfo{}, err
	}
	f, err := m.open()
	if err != nil {
		return UnitStateInfo{}, err
	}
	defer f.Close()
	c, err := f.take(contract.UnitStateKind, req.Spec.Name, req.Record, func() (string, error) {
		b, err := json.Marshal(unitPrior{
			Enabled: m.query(ctx, "is-enabled", req.Spec.Name),
			Active:  m.query(ctx, "is-active", req.Spec.Name),
		})
		return string(b), err
	})
	if err != nil {
		return UnitStateInfo{}, err
	}
	var prior unitPrior
	if err := json.Unmarshal([]byte(c.Prior), &prior); err != nil {
		return UnitStateInfo{}, err
	}
	info := UnitStateInfo{Name: req.Spec.Name, PriorEnabled: prior.Enabled, PriorActive: prior.Active}
	switch UnitWant(req.Spec.Want) {
	case UnitStopped:
		// An absent unit is already stopped.
		if prior.Enabled == "not-found" {
			return info, nil
		}
		err = m.systemctl(ctx, "stop", req.Spec.Name)
	case UnitDisabled:
		if prior.Enabled == "not-found" {
			return info, nil
		}
		err = m.systemctl(ctx, "disable", "--now", req.Spec.Name)
	case UnitMasked:
		// Masking an absent unit is meaningful: nothing installed later
		// can start it while the record lives.
		err = m.systemctl(ctx, "mask", "--now", req.Spec.Name)
	}
	return info, err
}

// removeUnitState puts the unit back the way the record found it.
func (m Machine) removeUnitState(ctx context.Context, req applyRequest[unitStateSpec]) error {
	f, err := m.open()
	if err != nil {
		return err
	}
	defer f.Close()
	c, ours, err := f.held(contract.UnitStateKind, req.Spec.Name, req.Record)
	if err != nil || !ours {
		return err
	}
	var prior unitPrior
	if err := json.Unmarshal([]byte(c.Prior), &prior); err != nil {
		return err
	}
	want := UnitWant(req.Spec.Want)
	if want == UnitMasked && prior.Enabled != "masked" {
		if err := m.systemctl(ctx, "unmask", req.Spec.Name); err != nil {
			return err
		}
	}
	if (want == UnitDisabled || want == UnitMasked) && prior.Enabled == "enabled" {
		if err := m.systemctl(ctx, "enable", req.Spec.Name); err != nil {
			return err
		}
	}
	if prior.Active == "active" {
		if err := m.systemctl(ctx, "start", req.Spec.Name); err != nil {
			return err
		}
	}
	return f.drop(contract.UnitStateKind, req.Spec.Name, req.Record)
}
