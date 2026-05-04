package repair_test

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	"letts/internal/ids"
	"letts/internal/repair"
	"letts/internal/storage"
)

func TestSweepRunningToLostFinalizesAll(t *testing.T) {
	db := setupRepairDB(t)
	dataDir := t.TempDir()
	cfg := repairCfg(dataDir)

	id1, _ := repairFixture(t, db, dataDir)
	id2, _ := repairFixture(t, db, dataDir)

	if err := repair.SweepRunningToLost(context.Background(), cfg, db, slog.Default()); err != nil {
		t.Fatalf("sweep: %v", err)
	}
	for _, id := range []string{id1, id2} {
		m, err := storage.GetMission(context.Background(), db, id)
		if err != nil {
			t.Fatalf("get %s: %v", id, err)
		}
		if m.Status != storage.StatusDone || m.Outcome.String != "lost" {
			t.Errorf("mission %s: status=%q outcome=%q", id, m.Status, m.Outcome.String)
		}
	}
}

func TestSweepRunningToLostAppendsDoneEvent(t *testing.T) {
	db := setupRepairDB(t)
	dataDir := t.TempDir()
	cfg := repairCfg(dataDir)

	id, parentDir := repairFixture(t, db, dataDir)

	if err := repair.SweepRunningToLost(context.Background(), cfg, db, slog.Default()); err != nil {
		t.Fatalf("sweep: %v", err)
	}
	events := loadEventsRepair(t, parentDir, id)
	if len(events) < 2 {
		t.Fatalf("events=%v, want at least running+done", events)
	}
	last := events[len(events)-1]
	if last["event"] != "done" || last["outcome"] != "lost" {
		t.Errorf("last event=%v", last)
	}
}

func TestSweepRunningToLostMissingEventsFileRecovers(t *testing.T) {
	db := setupRepairDB(t)
	dataDir := t.TempDir()
	cfg := repairCfg(dataDir)
	id, parentDir := repairFixture(t, db, dataDir)
	// Pretend the events file was lost in a crash.
	_ = os.Remove(filepath.Join(parentDir, id+"-events"))

	if err := repair.SweepRunningToLost(context.Background(), cfg, db, slog.Default()); err != nil {
		t.Fatalf("sweep: %v", err)
	}
	m, _ := storage.GetMission(context.Background(), db, id)
	if m.Outcome.String != "lost" {
		t.Errorf("outcome=%q", m.Outcome.String)
	}
	if _, err := os.Stat(filepath.Join(parentDir, id+"-events")); err != nil {
		t.Errorf("events file not recreated: %v", err)
	}
}

func TestSweepRunningToLostNoRunningRowsIsNoop(t *testing.T) {
	db := setupRepairDB(t)
	dataDir := t.TempDir()
	cfg := repairCfg(dataDir)
	if err := repair.SweepRunningToLost(context.Background(), cfg, db, slog.Default()); err != nil {
		t.Fatalf("sweep: %v", err)
	}
}

func TestSweepRunningToLostLeavesNonRunningAlone(t *testing.T) {
	db := setupRepairDB(t)
	dataDir := t.TempDir()
	cfg := repairCfg(dataDir)

	queuedID := ids.NewUUIDv7()
	_ = storage.InsertMission(context.Background(), db, &storage.Mission{
		ID: queuedID, Kind: storage.KindMission, Lane: "n",
		MissionName: "x", Status: storage.StatusQueued,
		Input: []byte(`{}`), InputFingerprint: "fp",
		TimeCreatedMs: time.Now().UnixMilli(),
	})

	if err := repair.SweepRunningToLost(context.Background(), cfg, db, slog.Default()); err != nil {
		t.Fatalf("sweep: %v", err)
	}
	m, _ := storage.GetMission(context.Background(), db, queuedID)
	if m.Status != storage.StatusQueued {
		t.Errorf("queued mission disturbed: status=%q", m.Status)
	}
}

func TestSweepOrphansRemovesUnknownOutputFiles(t *testing.T) {
	db := setupRepairDB(t)
	dataDir := t.TempDir()
	cfg := repairCfg(dataDir)

	bogusID := ids.NewUUIDv7()
	shard, _ := ids.ShardPath(bogusID)
	dir := filepath.Join(dataDir, "output", shard)
	_ = os.MkdirAll(dir, 0o755)
	for _, sfx := range []string{"-stdout", "-stderr", "-combined", "-events"} {
		path := filepath.Join(dir, bogusID+sfx)
		_ = os.WriteFile(path, []byte("data"), 0o644)
	}

	if err := repair.SweepOrphans(context.Background(), cfg, db, slog.Default()); err != nil {
		t.Fatalf("sweep: %v", err)
	}
	for _, sfx := range []string{"-stdout", "-stderr", "-combined", "-events"} {
		if _, err := os.Stat(filepath.Join(dir, bogusID+sfx)); !errors.Is(err, os.ErrNotExist) {
			t.Errorf("orphan %s not removed", sfx)
		}
	}
}

func TestSweepOrphansKeepsKnownOutputFiles(t *testing.T) {
	db := setupRepairDB(t)
	dataDir := t.TempDir()
	cfg := repairCfg(dataDir)

	id, parentDir := repairFixture(t, db, dataDir)

	if err := repair.SweepOrphans(context.Background(), cfg, db, slog.Default()); err != nil {
		t.Fatalf("sweep: %v", err)
	}
	if _, err := os.Stat(filepath.Join(parentDir, id+"-events")); err != nil {
		t.Errorf("known events file removed: %v", err)
	}
}

func TestSweepOrphansRemovesOrphanWorkdirs(t *testing.T) {
	db := setupRepairDB(t)
	dataDir := t.TempDir()
	cfg := repairCfg(dataDir)

	bogus := ids.NewUUIDv7()
	dir := filepath.Join(dataDir, "work", bogus)
	_ = os.MkdirAll(dir, 0o755)

	if err := repair.SweepOrphans(context.Background(), cfg, db, slog.Default()); err != nil {
		t.Fatalf("sweep: %v", err)
	}
	if _, err := os.Stat(dir); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("orphan workdir not removed")
	}
}

func TestSweepOrphansRemovesOrphanStagingFiles(t *testing.T) {
	db := setupRepairDB(t)
	dataDir := t.TempDir()
	cfg := repairCfg(dataDir)

	bogus := ids.NewUUIDv7()
	shard, _ := ids.ShardPath(bogus)
	dir := filepath.Join(dataDir, "staging", shard)
	_ = os.MkdirAll(dir, 0o755)
	path := filepath.Join(dir, bogus)
	_ = os.WriteFile(path, []byte("data"), 0o644)
	tmpPath := filepath.Join(dir, bogus+".tmp")
	_ = os.WriteFile(tmpPath, []byte("data"), 0o644)

	if err := repair.SweepOrphans(context.Background(), cfg, db, slog.Default()); err != nil {
		t.Fatalf("sweep: %v", err)
	}
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("orphan staging not removed")
	}
	if _, err := os.Stat(tmpPath); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("orphan staging .tmp not removed")
	}
}

func TestSweepOrphansRecalcsZeroedTTL(t *testing.T) {
	db := setupRepairDB(t)
	dataDir := t.TempDir()
	cfg := repairCfg(dataDir)

	// Insert a complete staging row with time_expires=0 to simulate the
	// trigger having marked it for recalc (post-CASCADE delete of last ref).
	id := ids.NewUUIDv7()
	now := time.Now().UnixMilli()
	if err := storage.InsertStaging(context.Background(), db, &storage.StagingFile{
		StagingID: id, State: storage.StagingComplete, Sha256: "x", Size: 1, BytesReceived: 1,
		Path: "p", TimeCreatedMs: now, TimeUpdatedMs: now, TimeExpiresMs: 0,
	}); err != nil {
		t.Fatal(err)
	}

	if err := repair.SweepOrphans(context.Background(), cfg, db, slog.Default()); err != nil {
		t.Fatalf("sweep: %v", err)
	}
	sf, _ := storage.GetStaging(context.Background(), db, id)
	if sf.TimeExpiresMs == 0 {
		t.Errorf("time_expires still 0 after recalc")
	}
}
