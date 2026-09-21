package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestResolveStorePath(t *testing.T) {
	exeDir := t.TempDir()
	workDir := t.TempDir()
	t.Chdir(workDir)

	abs := filepath.Join(workDir, "abs.db")
	if got, legacy := resolveStorePath(abs, exeDir); got != abs || legacy {
		t.Fatalf("absolute path: got %q legacy=%v", got, legacy)
	}

	// Nothing exists yet: anchor next to the config.
	want := filepath.Join(exeDir, "whatsmeow.db")
	if got, legacy := resolveStorePath("whatsmeow.db", exeDir); got != want || legacy {
		t.Fatalf("fresh install: got %q legacy=%v, want %q", got, legacy, want)
	}

	// Only the old working-directory database exists: keep using it.
	if err := os.WriteFile(filepath.Join(workDir, "whatsmeow.db"), nil, 0o644); err != nil {
		t.Fatal(err)
	}
	wantLegacy, _ := filepath.Abs("whatsmeow.db")
	if got, legacy := resolveStorePath("whatsmeow.db", exeDir); got != wantLegacy || !legacy {
		t.Fatalf("legacy only: got %q legacy=%v, want %q", got, legacy, wantLegacy)
	}

	// Both exist: the one next to the config wins.
	if err := os.WriteFile(want, nil, 0o644); err != nil {
		t.Fatal(err)
	}
	if got, legacy := resolveStorePath("whatsmeow.db", exeDir); got != want || legacy {
		t.Fatalf("both: got %q legacy=%v, want %q", got, legacy, want)
	}
}
