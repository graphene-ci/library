package dockerlib

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestAPTLockWaitConfig(t *testing.T) {
	for _, fail := range []bool{false, true} {
		t.Run(map[bool]string{false: "success", true: "failure"}[fail], func(t *testing.T) {
			dir := t.TempDir()
			original := filepath.Join(dir, "original.conf")
			contents := "Acquire::Retries \"3\";\n"
			if err := os.WriteFile(original, []byte(contents), 0o600); err != nil {
				t.Fatal(err)
			}
			status := "0"
			if fail {
				status = "7"
			}
			command := exec.Command("sh", "-ec", `apt_config="$TEST_TEMP"; trap 'rm -f "$apt_config"' EXIT; `+aptLockWaitConfig+`sh -ec 'cat "$APT_CONFIG"'; exit `+status)
			command.Env = append(os.Environ(), "APT_CONFIG="+original, "TEST_TEMP="+filepath.Join(dir, "temp.conf"))
			out, err := command.CombinedOutput()
			if (err != nil) != fail {
				t.Fatalf("failure=%v: %v: %s", fail, err, out)
			}
			if !strings.Contains(string(out), contents) || !strings.Contains(string(out), `DPkg::Lock::Timeout "120";`) {
				t.Fatalf("nested command missed config: %s", out)
			}
			if _, err := os.Stat(filepath.Join(dir, "temp.conf")); !os.IsNotExist(err) {
				t.Fatalf("temporary config survived: %v", err)
			}
			data, err := os.ReadFile(filepath.Clean(original))
			if err != nil || string(data) != contents {
				t.Fatalf("original config changed: %v", err)
			}
		})
	}
}
