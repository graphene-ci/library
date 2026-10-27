package dockerlib

import "testing"

// The level is the one a line names in its own text; a line that names
// none is info, whatever stream it came on.
func TestLineLevel(t *testing.T) {
	for line, want := range map[string]level{
		// PostgreSQL: everything on stderr, the level after the stamp.
		`2026-10-27 12:27:03.670 GMT [102] ERROR:  could not serialize access due to concurrent update`: levelError,
		`2026-10-27 12:27:03.670 GMT [102] STATEMENT:  UPDATE t SET x = 1`:                              levelInfo,
		`2026-10-27 12:27:03.670 GMT [1] LOG:  database system is ready to accept connections`:          levelInfo,
		`2026-10-27 12:27:03.670 GMT [1] WARNING:  could not open statistics file`:                      levelWarn,
		`2026-10-27 12:27:03.670 GMT [1] FATAL:  the database system is starting up`:                    levelError,
		`2026-10-27 12:27:03.670 GMT [1] PANIC:  could not write to file`:                               levelError,
		// MySQL / MariaDB.
		`2026-10-27T12:27:03.670Z 0 [Warning] [MY-010068] [Server] CA certificate is self signed.`: levelWarn,
		`2026-10-27T12:27:03.670Z 8 [ERROR] [MY-012592] [InnoDB] Operating system error number 2`:  levelError,
		`2026-10-27T12:27:03.670Z 0 [Note] [MY-010311] [Server] Server hostname`:                   levelInfo,
		`2026-10-27T12:27:03.670Z 0 [System] [MY-010116] [Server] starting as process 1`:           levelInfo,
		// Structured: zap, logrus, slog, pino.
		`{"level":"error","ts":1761568023.67,"msg":"Query","err":"ERROR: could not serialize"}`: levelError,
		`{"level":"warn","msg":"slow"}`:                         levelWarn,
		`{"severity":"DEBUG","message":"x"}`:                    levelDebug,
		`{"L":"FATAL","M":"bye"}`:                               levelError,
		`{"level":50,"msg":"pino"}`:                             levelError,
		`{"level":"info","msg":"the error count is zero"}`:      levelInfo,
		`time=2026-10-27T12:27:03Z level=WARN msg="slow query"`: levelWarn,
		`ts=1 lvl=error caller=main.go:10 msg="dial failed"`:    levelError,
		// glog / CockroachDB.
		`E261027 12:27:03.670612 1234 sql/conn_executor.go:123 ⋮ [n1] error`: levelError,
		`W1027 12:27:03.670612 1 reflector.go:1] watch ended`:                levelWarn,
		`I261027 12:27:03.670612 1 server/node.go:1 ⋮ started`:               levelInfo,
		// Generic markers.
		`ERROR something broke`:                               levelError,
		`[WARN] disk almost full`:                             levelWarn,
		`2026/10/27 12:27:03 [error] 7#7: upstream timed out`: levelError,
		// Prose is not a level.
		`query error: retrying`:        levelInfo,
		`I think it works`:             levelInfo,
		`collected 12 items; 3 failed`: levelInfo,
		`{not json at all`:             levelInfo,
		``:                             levelInfo,
		`progress 10/100 ... done, no ERRORS were found in the remaining long tail of this line`: levelInfo,
	} {
		if got := lineLevel(line); got != want {
			t.Errorf("lineLevel(%q) = %d, want %d", line, got, want)
		}
	}
}
