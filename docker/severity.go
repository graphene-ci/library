package dockerlib

// A container's output has no severity of its own: the stream is not one
// (PostgreSQL writes everything to stderr, pytest prints its failures to
// stdout). The level a line names in its own text is, so it is read from
// there — a structured line by its field, a text line by the marker its
// format puts near the start — and a line that names none stays info.

import (
	"context"
	"encoding/json"
	"strconv"
	"strings"

	"github.com/graphene-ci/pipeline/pkg/obs"
)

// level is the severity a line names.
type level int

const (
	levelInfo level = iota // the default: a line that names no level
	levelDebug
	levelWarn
	levelError
)

// logAt emits one line at its level. Fatal, panic and critical are
// errors: the record keeps the line, and an error filter finds it.
func logAt(ctx context.Context, lvl level, body string, attrs ...obs.KV) {
	switch lvl {
	case levelDebug:
		obs.Debug(ctx, body, attrs...)
	case levelWarn:
		obs.Warn(ctx, body, attrs...)
	case levelError:
		obs.Error(ctx, body, attrs...)
	case levelInfo:
		obs.Info(ctx, body, attrs...)
	}
}

// lineLevel reads the level a log line names:
//
//   - a JSON object — its level, severity, lvl or L field (zap, logrus,
//     slog, pino's numbers);
//   - logfmt — level=, lvl= or severity=;
//   - text — glog/klog's leading I/W/E/F stamp (CockroachDB), or, among
//     the first words, a level in brackets ([ERROR], [Warning], MySQL's
//     [Note]) or in capitals, alone or before a colon (PostgreSQL's
//     "ERROR:", "LOG:").
//
// A lowercase word in a sentence ("query error") is prose, not a level.
func lineLevel(line string) level {
	s := strings.TrimSpace(line)
	if strings.HasPrefix(s, "{") {
		if lvl, ok := jsonLevel(s); ok {
			return lvl
		}
	}
	if lvl, ok := logfmtLevel(s); ok {
		return lvl
	}
	return textLevel(s)
}

// jsonLevel reads the level field of a JSON line.
func jsonLevel(s string) (level, bool) {
	var fields map[string]json.RawMessage
	if json.Unmarshal([]byte(s), &fields) != nil {
		return 0, false
	}
	for _, key := range []string{"level", "severity", "lvl", "L", "log.level", "levelname"} {
		raw, ok := fields[key]
		if !ok {
			continue
		}
		var word string
		if json.Unmarshal(raw, &word) == nil {
			if lvl, ok := levelWord(word); ok {
				return lvl, true
			}
			continue
		}
		var n float64
		if json.Unmarshal(raw, &n) == nil {
			return numericLevel(n), true
		}
	}
	return 0, false
}

// numericLevel maps pino/bunyan numbers: 10 trace, 20 debug, 30 info,
// 40 warn, 50 error, 60 fatal.
func numericLevel(n float64) level {
	switch {
	case n >= 50:
		return levelError
	case n >= 40:
		return levelWarn
	case n >= 30:
		return levelInfo
	default:
		return levelDebug
	}
}

// logfmtLevel reads level=, lvl= or severity= of a logfmt line.
func logfmtLevel(s string) (level, bool) {
	for _, field := range strings.Fields(s) {
		key, value, ok := strings.Cut(field, "=")
		if !ok {
			continue
		}
		switch strings.ToLower(key) {
		case "level", "lvl", "severity":
			if lvl, ok := levelWord(strings.Trim(value, `"'`)); ok {
				return lvl, true
			}
		}
	}
	return 0, false
}

// textMarkerWords is how far into a text line a marker is looked for: a
// timestamp, a zone, a pid and a thread id come before it, the message
// after.
const textMarkerWords = 6

// textLevel reads the marker of a text line.
func textLevel(s string) level {
	words := strings.Fields(s)
	if len(words) > 0 {
		if lvl, ok := glogLevel(words[0]); ok {
			return lvl
		}
	}
	for i, w := range words {
		if i == textMarkerWords {
			break
		}
		if inner, ok := strings.CutPrefix(w, "["); ok {
			if inner, ok := strings.CutSuffix(strings.TrimSuffix(inner, ":"), "]"); ok && len(inner) >= 3 {
				if lvl, ok := levelWord(inner); ok {
					return lvl
				}
			}
			continue
		}
		// A single capital ("I", "A") is a word, not a level.
		bare := strings.TrimSuffix(w, ":")
		if len(bare) < 3 || bare != strings.ToUpper(bare) {
			continue
		}
		if lvl, ok := levelWord(bare); ok {
			return lvl
		}
	}
	return levelInfo
}

// glogLevel reads glog/klog's "E1027" (level letter, then MMDD) and
// CockroachDB's "E261027" (then YYMMDD).
func glogLevel(w string) (level, bool) {
	if len(w) != 5 && len(w) != 7 {
		return 0, false
	}
	if _, err := strconv.Atoi(w[1:]); err != nil {
		return 0, false
	}
	switch w[0] {
	case 'I':
		return levelInfo, true
	case 'W':
		return levelWarn, true
	case 'E', 'F':
		return levelError, true
	}
	return 0, false
}

// levelWord maps a level's name, any case.
func levelWord(w string) (level, bool) {
	switch strings.ToLower(w) {
	case "trace", "debug", "debug1", "debug2", "debug3", "debug4", "debug5", "dbug", "d", "t":
		return levelDebug, true
	case "info", "information", "notice", "note", "log", "system", "i", "n":
		return levelInfo, true
	case "warn", "warning", "w":
		return levelWarn, true
	case "error", "err", "fatal", "panic", "dpanic", "crit", "critical", "alert", "emerg", "emergency", "e", "f":
		return levelError, true
	}
	return 0, false
}
