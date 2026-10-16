package contract

import (
	"fmt"
	"strings"
)

// Condition is one entry of an object's status.conditions — the
// Kubernetes convention every controller (Crossplane included) reports
// through: what is true or not, and why, in the controller's own words.
type Condition struct {
	Type    string `json:"type"`
	Status  string `json:"status"`
	Reason  string `json:"reason,omitempty"`
	Message string `json:"message,omitempty"`
}

// ConditionsOf reads status.conditions of a live object; an object
// without them has none.
func ConditionsOf(live map[string]any) []Condition {
	status, _ := live["status"].(map[string]any)
	raw, _ := status["conditions"].([]any)
	out := make([]Condition, 0, len(raw))
	for _, item := range raw {
		m, ok := item.(map[string]any)
		if !ok {
			continue
		}
		c := Condition{}
		c.Type, _ = m["type"].(string)
		c.Status, _ = m["status"].(string)
		c.Reason, _ = m["reason"].(string)
		c.Message, _ = m["message"].(string)
		if c.Type != "" {
			out = append(out, c)
		}
	}
	return out
}

// Summary renders conditions the way a person reads them in an error:
// "Synced=False (ReconcileError: Quota limit compute.ssdDisks.size
// exceeded); Ready=False (Creating)". Empty when there are none.
func Summary(cs []Condition) string {
	parts := make([]string, 0, len(cs))
	for _, c := range cs {
		s := c.Type + "=" + c.Status
		switch {
		case c.Reason != "" && c.Message != "":
			s += fmt.Sprintf(" (%s: %s)", c.Reason, c.Message)
		case c.Reason != "":
			s += " (" + c.Reason + ")"
		case c.Message != "":
			s += " (" + c.Message + ")"
		}
		parts = append(parts, s)
	}
	return strings.Join(parts, "; ")
}

// Explain appends the last observed conditions to an error's text — the
// cloud's own reason belongs in the failure, not only in its logs.
func Explain(msg string, cs []Condition) string {
	if s := Summary(cs); s != "" {
		return msg + "; last observed: " + s
	}
	return msg
}
