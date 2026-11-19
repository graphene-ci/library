package host

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestValidateUnitState(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		spec unitStateSpec
		ok   bool
	}{
		{name: "mask timer", spec: unitStateSpec{Name: "apt-daily.timer", Want: "masked"}, ok: true},
		{name: "template", spec: unitStateSpec{Name: "getty@tty1.service", Want: "stopped"}, ok: true},
		{name: "no type", spec: unitStateSpec{Name: "unattended-upgrades", Want: "masked"}},
		{name: "path", spec: unitStateSpec{Name: "../x.service", Want: "masked"}},
		{name: "want", spec: unitStateSpec{Name: "x.service", Want: "removed"}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, c.ok, validateUnitState(c.spec) == nil)
		})
	}
}

func TestUnitStateRoundTrip(t *testing.T) {
	t.Parallel()
	const unit = "unattended-upgrades.service"
	cases := []struct {
		name         string
		enabled      string
		active       string
		want         UnitWant
		applyCalls   []string
		restoreCalls []string
	}{
		{
			name: "mask an enabled running unit", enabled: "enabled", active: "active", want: UnitMasked,
			applyCalls:   []string{"systemctl mask --now " + unit},
			restoreCalls: []string{"systemctl unmask " + unit, "systemctl enable " + unit, "systemctl start " + unit},
		},
		{
			name: "mask an already masked unit", enabled: "masked", active: "inactive", want: UnitMasked,
			applyCalls:   []string{"systemctl mask --now " + unit},
			restoreCalls: nil,
		},
		{
			name: "mask an absent unit", enabled: "", active: "", want: UnitMasked,
			applyCalls:   []string{"systemctl mask --now " + unit},
			restoreCalls: []string{"systemctl unmask " + unit},
		},
		{
			name: "disable an enabled idle unit", enabled: "enabled", active: "inactive", want: UnitDisabled,
			applyCalls:   []string{"systemctl disable --now " + unit},
			restoreCalls: []string{"systemctl enable " + unit},
		},
		{
			name: "stop a static running unit", enabled: "static", active: "active", want: UnitStopped,
			applyCalls:   []string{"systemctl stop " + unit},
			restoreCalls: []string{"systemctl start " + unit},
		},
		{
			name: "stop an absent unit", enabled: "", active: "", want: UnitStopped,
			applyCalls:   nil,
			restoreCalls: nil,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			queries := []string{"systemctl is-enabled " + unit, "systemctl is-active " + unit}
			r := &scripted{answers: map[string]answer{
				queries[0]: {out: c.enabled + "\n", err: errors.New("exit status 1")},
				queries[1]: {out: c.active + "\n", err: errors.New("exit status 3")},
			}}
			m := NewMachine(t.TempDir(), r)
			req := applyRequest[unitStateSpec]{Record: "host-unit-state/m-u", Spec: unitStateSpec{Name: unit, Want: string(c.want)}}
			info, err := m.applyUnitState(context.Background(), req)
			require.NoError(t, err)
			wantEnabled := c.enabled
			if wantEnabled == "" {
				wantEnabled = "not-found"
			}
			require.Equal(t, wantEnabled, info.PriorEnabled)
			require.Equal(t, append(queries, c.applyCalls...), r.Calls())

			before := len(r.Calls())
			require.NoError(t, m.removeUnitState(context.Background(), req))
			require.Equal(t, c.restoreCalls, nilIfEmpty(r.Calls()[before:]))
			// Restoring twice changes nothing.
			require.NoError(t, m.removeUnitState(context.Background(), req))
			require.Equal(t, c.restoreCalls, nilIfEmpty(r.Calls()[before:]))
		})
	}
}

func nilIfEmpty(s []string) []string {
	if len(s) == 0 {
		return nil
	}
	return s
}

func TestUnitLifecycle(t *testing.T) {
	t.Parallel()
	r := &scripted{answers: map[string]answer{
		"systemctl show --property=ActiveState --value node-exporter.service": {out: "active\n"},
	}}
	m := NewMachine(t.TempDir(), r)
	content := []byte("[Service]\nExecStart=/usr/local/bin/node_exporter\n[Install]\nWantedBy=multi-user.target\n")
	req := applyRequest[unitSpec]{Record: "host-unit/m-node-exporter.service", Spec: unitSpec{Name: "node-exporter.service", Content: content}}
	info, err := m.applyUnit(context.Background(), req)
	require.NoError(t, err)
	require.Equal(t, UnitInfo{Name: "node-exporter.service", Path: "/etc/systemd/system/node-exporter.service"}, info)
	require.Equal(t, string(content), readFile(t, m.root, info.Path))
	require.Equal(t, []string{"systemctl daemon-reload", "systemctl enable --now node-exporter.service",
		"systemctl show --property=ActiveState --value node-exporter.service"}, r.Calls())

	require.NoError(t, m.removeUnit(context.Background(), req))
	_, err = os.Stat(filepath.Join(m.root, rel(info.Path)))
	require.True(t, os.IsNotExist(err))
	require.Equal(t, []string{
		"systemctl daemon-reload", "systemctl enable --now node-exporter.service",
		"systemctl show --property=ActiveState --value node-exporter.service",
		"systemctl disable --now node-exporter.service", "systemctl daemon-reload",
	}, r.Calls())
}

// `systemctl enable --now` exits 0 when the start job fails; the record
// is not ready until the unit IS active.
func TestUnitThatDoesNotStart(t *testing.T) {
	t.Parallel()
	cases := []string{"inactive", "failed", "activating"}
	for _, state := range cases {
		t.Run(state, func(t *testing.T) {
			t.Parallel()
			r := &scripted{answers: map[string]answer{
				"systemctl show --property=ActiveState --value x.service": {out: state + "\n"},
			}}
			m := NewMachine(t.TempDir(), r)
			_, err := m.applyUnit(context.Background(), applyRequest[unitSpec]{Record: "host-unit/m-x", Spec: unitSpec{Name: "x.service", Content: []byte("[Service]\n")}})
			require.ErrorContains(t, err, "is "+state+" after start")
		})
	}
}

// A unit file the record did not install is never taken over or removed.
func TestUnitRefusesExisting(t *testing.T) {
	t.Parallel()
	m, root := fixture(t, map[string]string{"/etc/systemd/system/postgresql.service": "theirs"})
	req := applyRequest[unitSpec]{Record: "host-unit/m-postgresql.service", Spec: unitSpec{Name: "postgresql.service", Content: []byte("ours")}}
	_, err := m.applyUnit(context.Background(), req)
	require.True(t, isConflict(err), "got %v", err)
	require.NoError(t, m.removeUnit(context.Background(), req))
	require.Equal(t, "theirs", readFile(t, root, "/etc/systemd/system/postgresql.service"))
}

func BenchmarkValidateUnitState(b *testing.B) {
	spec := unitStateSpec{Name: "apt-daily-upgrade.timer", Want: "masked"}
	for b.Loop() {
		if err := validateUnitState(spec); err != nil {
			b.Fatal(err)
		}
	}
}
