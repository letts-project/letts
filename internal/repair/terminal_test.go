package repair_test

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"letts/internal/ids"
	"letts/internal/repair"
	"letts/internal/storage"
)

func TestEnsureTerminalEventsAppendsMissingDone(t *testing.T) {
	db := setupRepairDB(t)
	dataDir := t.TempDir()
	cfg := repairCfg(dataDir)
	id, parentDir := repairFixture(t, db, dataDir)

	started := time.Now().Add(-3 * time.Second).UnixMilli()
	finished := time.Now().UnixMilli()
	if _, err := db.Exec(`UPDATE missions SET status='done', outcome='success', exit_code=0,
		time_started=?, time_finished=? WHERE mission_id=?`, started, finished, id); err != nil {
		t.Fatal(err)
	}

	if err := repair.EnsureTerminalEvents(context.Background(), cfg, db, slog.Default()); err != nil {
		t.Fatalf("ensure: %v", err)
	}

	events := loadEventsRepair(t, parentDir, id)
	last := events[len(events)-1]
	if last["event"] != "done" {
		t.Fatalf("last event=%v, want done", last)
	}
	if last["outcome"] != "success" {
		t.Errorf("outcome=%v", last["outcome"])
	}
	if last["seq"].(float64) != 2 {
		t.Errorf("seq=%v, want 2 (after the running event)", last["seq"])
	}
	if int64(last["time_finished"].(float64)) != finished {
		t.Errorf("time_finished=%v, want %d", last["time_finished"], finished)
	}
	if int64(last["duration_ms"].(float64)) != finished-started {
		t.Errorf("duration_ms=%v, want %d", last["duration_ms"], finished-started)
	}
	if _, ok := last["exit_code"]; !ok {
		t.Errorf("exit_code missing: %v", last)
	}
}

func TestEnsureTerminalEventsRecreatesMissingFile(t *testing.T) {
	db := setupRepairDB(t)
	dataDir := t.TempDir()
	cfg := repairCfg(dataDir)
	id, parentDir := repairFixture(t, db, dataDir)
	if _, err := db.Exec(`UPDATE missions SET status='done', outcome='lost', exit_code=0,
		time_finished=? WHERE mission_id=?`, time.Now().UnixMilli(), id); err != nil {
		t.Fatal(err)
	}
	// The events file vanished entirely (manual cleanup, disk repair).
	if err := os.Remove(filepath.Join(parentDir, id+"-events")); err != nil {
		t.Fatal(err)
	}

	if err := repair.EnsureTerminalEvents(context.Background(), cfg, db, slog.Default()); err != nil {
		t.Fatalf("ensure: %v", err)
	}

	events := loadEventsRepair(t, parentDir, id)
	if len(events) == 0 {
		t.Fatal("events file not recreated")
	}
	last := events[len(events)-1]
	if last["event"] != "done" || last["outcome"] != "lost" {
		t.Errorf("last event=%v", last)
	}
}

func TestEnsureTerminalEventsLeavesHealthyFileUntouched(t *testing.T) {
	db := setupRepairDB(t)
	dataDir := t.TempDir()
	cfg := repairCfg(dataDir)
	id, parentDir := repairFixture(t, db, dataDir)
	finished := time.Now().UnixMilli()
	if _, err := db.Exec(`UPDATE missions SET status='done', outcome='success', exit_code=0,
		time_finished=? WHERE mission_id=?`, finished, id); err != nil {
		t.Fatal(err)
	}
	// A complete file: the done line is already there.
	appendDoneLine(t, parentDir, id, `{"seq":2,"event":"done","outcome":"success","exit_code":0,"time_finished":`+strconv.FormatInt(finished, 10)+`}`)
	path := filepath.Join(parentDir, id+"-events")
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}

	if err := repair.EnsureTerminalEvents(context.Background(), cfg, db, slog.Default()); err != nil {
		t.Fatalf("ensure: %v", err)
	}

	after, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(before) != string(after) {
		t.Errorf("healthy events file modified:\nbefore=%q\nafter=%q", before, after)
	}
}

// A done event glued onto a torn progress line is one newline-terminated junk
// line — unparseable as JSON, so the stream carries no terminal event even
// though the bytes contain a done. The tail probe must skip the junk instead
// of choking on it, and the pass must append a fresh parseable done.
func TestEnsureTerminalEventsRepairsGluedJunkLine(t *testing.T) {
	db := setupRepairDB(t)
	dataDir := t.TempDir()
	cfg := repairCfg(dataDir)
	id, parentDir := repairFixture(t, db, dataDir)
	finished := time.Now().UnixMilli()
	if _, err := db.Exec(`UPDATE missions SET status='done', outcome='success', exit_code=0,
		time_finished=? WHERE mission_id=?`, finished, id); err != nil {
		t.Fatal(err)
	}
	appendRawEvents(t, parentDir, id,
		`{"seq":2,"event":"progress","msg":"to{"seq":3,"event":"done","outcome":"success","exit_code":0}`+"\n")

	if err := repair.EnsureTerminalEvents(context.Background(), cfg, db, slog.Default()); err != nil {
		t.Fatalf("ensure: %v", err)
	}

	last := lastEventLine(t, parentDir, id)
	if last["event"] != "done" || last["outcome"] != "success" {
		t.Errorf("last line=%v, want a parseable done", last)
	}
}

// A file whose tail is an unterminated partial line — here a torn done write
// that lost its trailing bytes — holds no complete done line, and the probe
// must not credit the fragment as a terminal event. The pass must terminate
// the tail and append a parseable done after it.
func TestEnsureTerminalEventsRepairsUnterminatedTail(t *testing.T) {
	db := setupRepairDB(t)
	dataDir := t.TempDir()
	cfg := repairCfg(dataDir)
	id, parentDir := repairFixture(t, db, dataDir)
	finished := time.Now().UnixMilli()
	if _, err := db.Exec(`UPDATE missions SET status='done', outcome='success', exit_code=0,
		time_finished=? WHERE mission_id=?`, finished, id); err != nil {
		t.Fatal(err)
	}
	appendRawEvents(t, parentDir, id, `{"seq":2,"event":"done","outcome":"succ`)

	if err := repair.EnsureTerminalEvents(context.Background(), cfg, db, slog.Default()); err != nil {
		t.Fatalf("ensure: %v", err)
	}

	last := lastEventLine(t, parentDir, id)
	if last["event"] != "done" || last["outcome"] != "success" {
		t.Errorf("last line=%v, want a parseable done", last)
	}
}

func TestEnsureTerminalEventsIncludesOutputsAndReturn(t *testing.T) {
	db := setupRepairDB(t)
	dataDir := t.TempDir()
	cfg := repairCfg(dataDir)
	id, parentDir := repairFixture(t, db, dataDir)

	// A committed output: complete staging row and ref_kind='output' ref.
	stagingID := ids.NewUUIDv7()
	now := time.Now().UnixMilli()
	if err := storage.InsertStaging(context.Background(), db, &storage.StagingFile{
		StagingID: stagingID, State: storage.StagingComplete,
		Sha256: "abc123", Size: 7, BytesReceived: 7,
		Path: "p", TimeCreatedMs: now, TimeUpdatedMs: now, TimeExpiresMs: now + 60_000,
	}); err != nil {
		t.Fatal(err)
	}
	if err := storage.InsertRef(context.Background(), db, storage.StagingRef{
		MissionID: id, StagingID: stagingID, RefKind: storage.RefOutput, Role: "result",
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`UPDATE missions SET status='done', outcome='success', exit_code=0,
		return_value=?, time_finished=? WHERE mission_id=?`, []byte(`{"ok":true}`), now, id); err != nil {
		t.Fatal(err)
	}

	if err := repair.EnsureTerminalEvents(context.Background(), cfg, db, slog.Default()); err != nil {
		t.Fatalf("ensure: %v", err)
	}

	events := loadEventsRepair(t, parentDir, id)
	last := events[len(events)-1]
	if last["event"] != "done" {
		t.Fatalf("last event=%v", last)
	}
	ret, ok := last["return"].(map[string]any)
	if !ok || ret["ok"] != true {
		t.Errorf("return=%v", last["return"])
	}
	outputs, ok := last["outputs"].(map[string]any)
	if !ok {
		t.Fatalf("outputs missing: %v", last)
	}
	result, ok := outputs["result"].(map[string]any)
	if !ok {
		t.Fatalf("outputs.result missing: %v", outputs)
	}
	if result["staging_id"] != stagingID || result["sha256"] != "abc123" || result["size"].(float64) != 7 {
		t.Errorf("outputs.result=%v", result)
	}
}

func TestEnsureTerminalEventsReconstructsFailureFields(t *testing.T) {
	db := setupRepairDB(t)
	dataDir := t.TempDir()
	cfg := repairCfg(dataDir)
	id, parentDir := repairFixture(t, db, dataDir)
	if _, err := db.Exec(`UPDATE missions SET status='done', outcome='killed', exit_code=137,
		signal='SIGKILL', fail_reason='external_kill', fail_message='killed by operator',
		fail_details='{"who":"admin"}', time_finished=? WHERE mission_id=?`,
		time.Now().UnixMilli(), id); err != nil {
		t.Fatal(err)
	}

	if err := repair.EnsureTerminalEvents(context.Background(), cfg, db, slog.Default()); err != nil {
		t.Fatalf("ensure: %v", err)
	}

	events := loadEventsRepair(t, parentDir, id)
	last := events[len(events)-1]
	if last["event"] != "done" || last["outcome"] != "killed" {
		t.Fatalf("last event=%v", last)
	}
	if last["exit_code"].(float64) != 137 || last["signal"] != "SIGKILL" {
		t.Errorf("exit_code=%v signal=%v", last["exit_code"], last["signal"])
	}
	if last["fail_reason"] != "external_kill" || last["fail_message"] != "killed by operator" {
		t.Errorf("fail fields: %v", last)
	}
	details, ok := last["fail_details"].(map[string]any)
	if !ok || details["who"] != "admin" {
		t.Errorf("fail_details=%v", last["fail_details"])
	}
	if _, present := last["duration_ms"]; present {
		t.Errorf("duration_ms present despite unknown time_started: %v", last)
	}
}

func TestEnsureTerminalEventsSkipsNonDoneMissions(t *testing.T) {
	db := setupRepairDB(t)
	dataDir := t.TempDir()
	cfg := repairCfg(dataDir)
	id, parentDir := repairFixture(t, db, dataDir) // stays status='running'

	if err := repair.EnsureTerminalEvents(context.Background(), cfg, db, slog.Default()); err != nil {
		t.Fatalf("ensure: %v", err)
	}

	events := loadEventsRepair(t, parentDir, id)
	for _, ev := range events {
		if ev["event"] == "done" {
			t.Errorf("done appended to a running mission: %v", ev)
		}
	}
}

// appendDoneLine writes a raw done line and newline to the events file.
func appendDoneLine(t *testing.T, parentDir, id, line string) {
	t.Helper()
	appendRawEvents(t, parentDir, id, line+"\n")
}

// appendRawEvents appends raw bytes to the events file exactly as given —
// no newline added — the way a torn or glued write would leave them.
func appendRawEvents(t *testing.T, parentDir, id, raw string) {
	t.Helper()
	f, err := os.OpenFile(filepath.Join(parentDir, id+"-events"), os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteString(raw); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
}

// lastEventLine asserts the events file ends with a newline-terminated,
// JSON-parseable final line and returns that line decoded.
func lastEventLine(t *testing.T, parentDir, id string) map[string]any {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(parentDir, id+"-events"))
	if err != nil {
		t.Fatalf("read events: %v", err)
	}
	if len(raw) == 0 || raw[len(raw)-1] != '\n' {
		t.Fatalf("events file does not end with a newline: %q", raw)
	}
	trimmed := raw[:len(raw)-1]
	line := trimmed[bytes.LastIndexByte(trimmed, '\n')+1:]
	var ev map[string]any
	if err := json.Unmarshal(line, &ev); err != nil {
		t.Fatalf("last line is not parseable JSON: %q: %v", line, err)
	}
	return ev
}
