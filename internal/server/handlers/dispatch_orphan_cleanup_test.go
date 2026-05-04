package handlers

import (
	"os"
	"path/filepath"
	"testing"
)

// TestCleanupOrphanMissionFiles_RemovesAllArtifacts pins the orphan-cleanup
// contract: the dispatch insertErr branch and the orphan-from-crash branch must
// leave an identical filesystem state. Both paths route through
// cleanupOrphanMissionFiles — this test pins what that helper deletes.
func TestCleanupOrphanMissionFiles_RemovesAllArtifacts(t *testing.T) {
	dataDir := t.TempDir()
	missionID := "01900000-0000-7000-8000-deadbeefcafe"
	outDir := filepath.Join(dataDir, "output", "01", "90")
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		t.Fatal(err)
	}
	workDir := filepath.Join(dataDir, "work", missionID)
	if err := os.MkdirAll(workDir, 0o755); err != nil {
		t.Fatal(err)
	}

	// Drop every sentinel dispatch could have left behind.
	artifacts := []string{
		filepath.Join(outDir, missionID+"-events"),
		filepath.Join(outDir, missionID+"-stdout"),
		filepath.Join(outDir, missionID+"-stderr"),
		filepath.Join(outDir, missionID+"-combined"),
	}
	for _, p := range artifacts {
		if err := os.WriteFile(p, []byte("orphan"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	// Leave a non-empty workdir to confirm RemoveAll handles it.
	if err := os.WriteFile(filepath.Join(workDir, "stale.txt"), []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}

	cleanupOrphanMissionFiles(dataDir, outDir, missionID)

	for _, p := range artifacts {
		if _, err := os.Stat(p); !os.IsNotExist(err) {
			t.Errorf("not removed: %s (err=%v)", p, err)
		}
	}
	if _, err := os.Stat(workDir); !os.IsNotExist(err) {
		t.Errorf("workdir not removed: %v", err)
	}
}

// TestCleanupOrphanMissionFiles_BestEffort: missing files do not cause
// an error/panic — the helper is a best-effort sweep on either error
// path. This mirrors the real call sites which discard the return.
func TestCleanupOrphanMissionFiles_BestEffort(t *testing.T) {
	dataDir := t.TempDir()
	outDir := filepath.Join(dataDir, "output", "ab", "cd")
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		t.Fatal(err)
	}
	// Nothing present — must still succeed silently.
	cleanupOrphanMissionFiles(dataDir, outDir, "01900000-0000-7000-8000-000000000001")
}
