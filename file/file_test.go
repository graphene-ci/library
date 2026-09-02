package file

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

func TestWriteAndRemove(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "sub", "scrape.yml")
	content := []byte("global:\n  scrape_interval: 15s\n")

	info, err := writeActivity(context.Background(), fileSpec{Path: path, Content: content, Mode: 0o600})
	if err != nil {
		t.Fatalf("write: %v", err)
	}
	if info.Path != path {
		t.Fatalf("info.Path = %q, want %q", info.Path, path)
	}
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read back: %v", err)
	}
	if string(got) != string(content) {
		t.Fatalf("content = %q, want %q", got, content)
	}
	if st, _ := os.Stat(path); st.Mode().Perm() != 0o600 {
		t.Fatalf("mode = %v, want 0600", st.Mode().Perm())
	}

	if err := removeActivity(context.Background(), path); err != nil {
		t.Fatalf("remove: %v", err)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatal("file must be gone")
	}
	// Remove is retry-safe: absence is success.
	if err := removeActivity(context.Background(), path); err != nil {
		t.Fatalf("second remove must be nil: %v", err)
	}
}

func TestWriteRejectsUnwiredSources(t *testing.T) {
	if _, err := writeActivity(context.Background(), fileSpec{Path: "/tmp/x", Secret: "s"}); err == nil {
		t.Fatal("secret source must error until wired")
	}
	if _, err := writeActivity(context.Background(), fileSpec{Path: "/tmp/x", Artifact: "a"}); err == nil {
		t.Fatal("artifact source must error until wired")
	}
}
