package mission

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"letts/internal/ids"
	"letts/internal/storage"
)

// openTestDB opens an in-memory SQLite DB with migrations applied.
func openTestDB(t *testing.T) *sql.DB {
	t.Helper()
	dir := t.TempDir()
	db, err := storage.Open(filepath.Join(dir, "state.db"), storage.Options{})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if err := storage.Migrate(context.Background(), db); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	return db
}

func TestPrepareWorkdirCreatesSubdirs(t *testing.T) {
	dataDir := t.TempDir()
	missionID := ids.NewUUIDv7()

	workdir, err := PrepareWorkdir(dataDir, missionID, nil)
	if err != nil {
		t.Fatalf("PrepareWorkdir: %v", err)
	}

	expected := filepath.Join(dataDir, "work", missionID)
	if workdir != expected {
		t.Errorf("workdir path: got %q, want %q", workdir, expected)
	}

	for _, sub := range []string{"", "in", "out", "tmp"} {
		dir := filepath.Join(workdir, sub)
		info, err := os.Stat(dir)
		if err != nil {
			t.Errorf("subdir %q missing: %v", sub, err)
			continue
		}
		if !info.IsDir() {
			t.Errorf("%q is not a directory", dir)
		}
		// Check 0755 permissions (ignore sticky/setuid).
		if info.Mode().Perm() != 0o755 {
			t.Errorf("%q mode: got %o, want %o", dir, info.Mode().Perm(), 0o755)
		}
	}
}

func TestPrepareWorkdirCopiesInputs(t *testing.T) {
	dataDir := t.TempDir()
	missionID := ids.NewUUIDv7()

	// Create a source file.
	srcContent := []byte("hello input content")
	srcFile := filepath.Join(dataDir, "src_input")
	if err := os.WriteFile(srcFile, srcContent, 0o644); err != nil {
		t.Fatalf("write src: %v", err)
	}

	inputs := []ResolvedInput{
		{Role: "data", SourcePath: srcFile, Sha256: "abc", Size: int64(len(srcContent))},
	}

	workdir, err := PrepareWorkdir(dataDir, missionID, inputs)
	if err != nil {
		t.Fatalf("PrepareWorkdir: %v", err)
	}

	dst := filepath.Join(workdir, "in", "data")
	got, err := os.ReadFile(dst)
	if err != nil {
		t.Fatalf("read dst: %v", err)
	}
	if !bytes.Equal(got, srcContent) {
		t.Errorf("dst content: got %q, want %q", got, srcContent)
	}

	// Verify 0444 permissions on input file.
	info, err := os.Stat(dst)
	if err != nil {
		t.Fatalf("stat dst: %v", err)
	}
	if info.Mode().Perm() != 0o444 {
		t.Errorf("dst mode: got %o, want 0444", info.Mode().Perm())
	}
}

func TestPrepareWorkdirIdempotent(t *testing.T) {
	dataDir := t.TempDir()
	missionID := ids.NewUUIDv7()

	// First call.
	if _, err := PrepareWorkdir(dataDir, missionID, nil); err != nil {
		t.Fatalf("first PrepareWorkdir: %v", err)
	}

	// Second call should not error (replaces existing).
	srcContent := []byte("second input")
	srcFile := filepath.Join(dataDir, "src2")
	_ = os.WriteFile(srcFile, srcContent, 0o644)

	if _, err := PrepareWorkdir(dataDir, missionID, []ResolvedInput{
		{Role: "file2", SourcePath: srcFile, Sha256: "x", Size: int64(len(srcContent))},
	}); err != nil {
		t.Fatalf("second PrepareWorkdir: %v", err)
	}
}

func TestCleanupWorkdir(t *testing.T) {
	dataDir := t.TempDir()
	missionID := ids.NewUUIDv7()

	workdir, err := PrepareWorkdir(dataDir, missionID, nil)
	if err != nil {
		t.Fatalf("PrepareWorkdir: %v", err)
	}

	if err := CleanupWorkdir(dataDir, missionID); err != nil {
		t.Fatalf("CleanupWorkdir: %v", err)
	}

	if _, err := os.Stat(workdir); !os.IsNotExist(err) {
		t.Errorf("workdir should not exist after cleanup, got stat err=%v", err)
	}
}

func TestLoadInputsEmpty(t *testing.T) {
	db := openTestDB(t)
	dataDir := t.TempDir()
	missionID := ids.NewUUIDv7()

	inputs, err := LoadInputs(context.Background(), db, dataDir, missionID)
	if err != nil {
		t.Fatalf("LoadInputs: %v", err)
	}
	if len(inputs) != 0 {
		t.Errorf("expected 0 inputs, got %d", len(inputs))
	}
}

func TestLoadInputsOne(t *testing.T) {
	db := openTestDB(t)
	dataDir := t.TempDir()
	missionID := ids.NewUUIDv7()
	stagingID := ids.NewUUIDv7()

	ctx := context.Background()

	// Must insert mission row first (FK constraint).
	if err := storage.InsertMission(ctx, db, &storage.Mission{
		ID:            missionID,
		Kind:          storage.KindMission,
		Lane:          "normal",
		MissionName:   "test_mission",
		Status:        storage.StatusQueued,
		Input:         []byte("null"),
		TimeCreatedMs: 1000,
	}); err != nil {
		t.Fatalf("InsertMission: %v", err)
	}

	// Path is relative to dataDir, includes "staging" prefix.
	stagingPath := filepath.Join("staging", "ab", stagingID)

	if err := storage.InsertStaging(ctx, db, &storage.StagingFile{
		StagingID:     stagingID,
		State:         storage.StagingComplete,
		Sha256:        "deadbeef",
		Size:          512,
		Path:          stagingPath,
		TimeCreatedMs: 1000,
		TimeUpdatedMs: 1000,
		TimeExpiresMs: 9999999,
	}); err != nil {
		t.Fatalf("InsertStaging: %v", err)
	}

	if err := storage.InsertRef(ctx, db, storage.StagingRef{
		MissionID: missionID,
		StagingID: stagingID,
		RefKind:   storage.RefInput,
		Role:      "my_input",
	}); err != nil {
		t.Fatalf("InsertRef: %v", err)
	}

	inputs, err := LoadInputs(ctx, db, dataDir, missionID)
	if err != nil {
		t.Fatalf("LoadInputs: %v", err)
	}
	if len(inputs) != 1 {
		t.Fatalf("expected 1 input, got %d", len(inputs))
	}

	in := inputs[0]
	if in.Role != "my_input" {
		t.Errorf("role: got %q, want %q", in.Role, "my_input")
	}
	if in.Sha256 != "deadbeef" {
		t.Errorf("sha256: got %q", in.Sha256)
	}
	if in.Size != 512 {
		t.Errorf("size: got %d", in.Size)
	}
	wantPath := filepath.Join(dataDir, stagingPath)
	if in.SourcePath != wantPath {
		t.Errorf("source path: got %q, want %q", in.SourcePath, wantPath)
	}
}

// TestWriteStdinEnvelopeIsUserInputOnly enforces that the stdin payload
// must be the user input JSON verbatim (or "null" when empty) — the JSON
// input never carries `files`. An envelope shape like
// {"input":..., "files":{...}} would be wire-protocol drift that PHP
// clients would have to manually strip.
func TestWriteStdinEnvelopeIsUserInputOnly(t *testing.T) {
	workdir := t.TempDir()
	path, err := writeStdinEnvelope(workdir, []byte(`{"x":1}`))
	if err != nil {
		t.Fatalf("writeStdinEnvelope: %v", err)
	}
	wantPath := filepath.Join(workdir, "input.json")
	if path != wantPath {
		t.Errorf("path: got %q, want %q", path, wantPath)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if string(raw) != `{"x":1}` {
		t.Errorf("got %q, want %q", string(raw), `{"x":1}`)
	}
	// `files` must NOT be present in the wire payload — verify by
	// decoding into a generic map and checking absence.
	var generic map[string]any
	if err := json.Unmarshal(raw, &generic); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if _, ok := generic["files"]; ok {
		t.Errorf("stdin should not include `files`")
	}
}

func TestWriteStdinEnvelopeEmptyInputBecomesNull(t *testing.T) {
	workdir := t.TempDir()

	path, err := writeStdinEnvelope(workdir, nil)
	if err != nil {
		t.Fatalf("writeStdinEnvelope: %v", err)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if string(raw) != "null" {
		t.Errorf("got %q, want %q", string(raw), "null")
	}
}

func TestLoadInputsSkipsNonInput(t *testing.T) {
	db := openTestDB(t)
	dataDir := t.TempDir()
	missionID := ids.NewUUIDv7()
	stagingID := ids.NewUUIDv7()

	ctx := context.Background()

	// Must insert mission row first (FK constraint).
	if err := storage.InsertMission(ctx, db, &storage.Mission{
		ID:            missionID,
		Kind:          storage.KindMission,
		Lane:          "normal",
		MissionName:   "test_mission",
		Status:        storage.StatusQueued,
		Input:         []byte("null"),
		TimeCreatedMs: 1000,
	}); err != nil {
		t.Fatalf("InsertMission: %v", err)
	}

	if err := storage.InsertStaging(ctx, db, &storage.StagingFile{
		StagingID:     stagingID,
		State:         storage.StagingComplete,
		Sha256:        "aabbcc",
		Size:          256,
		Path:          "staging/00/output_file",
		TimeCreatedMs: 1000,
		TimeUpdatedMs: 1000,
		TimeExpiresMs: 9999999,
	}); err != nil {
		t.Fatalf("InsertStaging: %v", err)
	}

	// Insert as output ref (not input).
	if err := storage.InsertRef(ctx, db, storage.StagingRef{
		MissionID: missionID,
		StagingID: stagingID,
		RefKind:   storage.RefOutput,
		Role:      "out_file",
	}); err != nil {
		t.Fatalf("InsertRef: %v", err)
	}

	inputs, err := LoadInputs(ctx, db, dataDir, missionID)
	if err != nil {
		t.Fatalf("LoadInputs: %v", err)
	}
	if len(inputs) != 0 {
		t.Errorf("expected 0 inputs (output refs should be filtered), got %d", len(inputs))
	}
}
