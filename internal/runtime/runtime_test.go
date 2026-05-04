package runtime

import (
	"context"
	"database/sql"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	"letts/internal/config"
	"letts/internal/ids"
	"letts/internal/lane"
	"letts/internal/mission"
	"letts/internal/storage"
)

func openDB(t *testing.T) *sql.DB {
	t.Helper()
	dir := t.TempDir()
	db, err := storage.Open(filepath.Join(dir, "state.db"), storage.Options{})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if err := storage.Migrate(context.Background(), db); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	return db
}

func writeScript(t *testing.T, dir, name, body string) string {
	t.Helper()
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, []byte("#!/bin/sh\n"+body), 0o755); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
	return path
}

func runtimeCfg(dataDir string) *config.DugdaleConfig {
	return &config.DugdaleConfig{
		DataDir: dataDir,
		Limits: config.LimitsConfig{
			MaxOutputBuffer:      1024 * 1024,
			MaxEventsBuffer:      256 * 1024,
			MaxEventLineSize:     32 * 1024,
			MaxReturnValueSize:   64 * 1024,
			MaxFailMessageSize:   8 * 1024,
			MaxFailDetailsSize:   16 * 1024,
			MaxProgressRate:      100,
			ProgressBufferSize:   16 * 1024,
			MaxOutputFilesPerMsn: 8,
			DefaultKillGrace:     150 * time.Millisecond,
			ReaderPostExitGrace:  300 * time.Millisecond,
		},
	}
}

func insertQueuedMission(t *testing.T, db *sql.DB, scriptPath, lane string) string {
	t.Helper()
	id := ids.NewUUIDv7()
	m := storage.Mission{
		ID:               id,
		Kind:             storage.KindMission,
		Lane:             lane,
		MissionName:      filepath.Base(scriptPath),
		Status:           storage.StatusQueued,
		Input:            []byte(`{}`),
		InputFingerprint: "fp",
		TimeCreatedMs:    time.Now().UnixMilli(),
	}
	if err := storage.InsertMission(context.Background(), db, &m); err != nil {
		t.Fatalf("insert mission: %v", err)
	}
	rt := storage.MissionRuntime{
		MissionID:           id,
		MissionDir:          filepath.Dir(scriptPath),
		CommandTemplate:     `["sh", "{mission_path}"]`,
		ValidateMissionFile: true,
	}
	if err := storage.InsertRuntime(context.Background(), db, &rt); err != nil {
		t.Fatalf("insert runtime: %v", err)
	}
	return id
}

func waitForDone(t *testing.T, db *sql.DB, id string, deadline time.Duration) *storage.Mission {
	t.Helper()
	until := time.Now().Add(deadline)
	for time.Now().Before(until) {
		m, err := storage.GetMission(context.Background(), db, id)
		if err == nil && m.Status == storage.StatusDone {
			return m
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("mission %s did not reach done within %v", id, deadline)
	return nil
}

func TestRuntimeRunsMissionEndToEnd(t *testing.T) {
	db := openDB(t)
	cfg := runtimeCfg(t.TempDir())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	r := NewRuntime(ctx, cfg, db, slog.Default())

	r.Manager.Apply([]lane.LaneSpec{{Name: "normal", Concurrency: 1}})
	defer r.Manager.StopAll()

	scriptDir := t.TempDir()
	script := writeScript(t, scriptDir, "ok.sh", `echo '{"event":"success","return":{"v":42}}' >&3
exit 0`)
	id := insertQueuedMission(t, db, script, "normal")

	r.Manager.Notify("normal")
	got := waitForDone(t, db, id, 5*time.Second)
	if got.Outcome.String != "success" {
		t.Errorf("outcome=%q", got.Outcome.String)
	}
	if string(got.ReturnValue) != `{"v":42}` {
		t.Errorf("ReturnValue=%q", string(got.ReturnValue))
	}
}

func TestRuntimeSignalKillStopsRunningMission(t *testing.T) {
	db := openDB(t)
	cfg := runtimeCfg(t.TempDir())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	r := NewRuntime(ctx, cfg, db, slog.Default())

	r.Manager.Apply([]lane.LaneSpec{{Name: "slow", Concurrency: 1}})
	defer r.Manager.StopAll()

	scriptDir := t.TempDir()
	script := writeScript(t, scriptDir, "sleep.sh", `sleep 30`)
	id := insertQueuedMission(t, db, script, "slow")

	r.Manager.Notify("slow")

	// Wait until the mission is registered (running).
	until := time.Now().Add(2 * time.Second)
	for time.Now().Before(until) {
		if r.IsRunning(id) {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !r.IsRunning(id) {
		t.Fatal("mission never registered as running")
	}

	if !r.SignalKill(id, mission.KillByAPI) {
		t.Fatal("SignalKill returned false on running mission")
	}

	got := waitForDone(t, db, id, 5*time.Second)
	if got.Outcome.String != "killed" || got.FailReason.String != "killed_by_api" {
		t.Errorf("outcome=%q reason=%q", got.Outcome.String, got.FailReason.String)
	}
}

func TestRuntimeSignalKillUnknownMissionReturnsFalse(t *testing.T) {
	db := openDB(t)
	cfg := runtimeCfg(t.TempDir())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	r := NewRuntime(ctx, cfg, db, slog.Default())
	if r.SignalKill("nonexistent", mission.KillByAPI) {
		t.Error("SignalKill should return false for unknown mission")
	}
	if r.IsRunning("nonexistent") {
		t.Error("IsRunning should be false for unknown mission")
	}
}

func TestRuntimeKillChRegistrationCleansUp(t *testing.T) {
	db := openDB(t)
	cfg := runtimeCfg(t.TempDir())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	r := NewRuntime(ctx, cfg, db, slog.Default())
	r.Manager.Apply([]lane.LaneSpec{{Name: "fast", Concurrency: 1}})
	defer r.Manager.StopAll()

	scriptDir := t.TempDir()
	script := writeScript(t, scriptDir, "ok.sh", `echo '{"event":"success"}' >&3
exit 0`)
	id := insertQueuedMission(t, db, script, "fast")
	r.Manager.Notify("fast")
	waitForDone(t, db, id, 5*time.Second)

	if r.IsRunning(id) {
		t.Error("kill channel still registered after mission completed")
	}
}
