package apply_test

import (
	"context"
	"encoding/json"
	"log/slog"
	"path/filepath"
	"testing"
	"time"

	"letts/internal/apply"
	"letts/internal/lane"
	"letts/internal/storage"
)

func TestReplayFromDBNoConfigIsNoop(t *testing.T) {
	dir := t.TempDir()
	db, err := storage.Open(filepath.Join(dir, "state.db"), storage.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	if err := storage.Migrate(context.Background(), db); err != nil {
		t.Fatal(err)
	}
	mgr := &lane.Manager{
		DB: db, Logger: slog.Default(), Ctx: context.Background(),
		Spawner: func(_ context.Context, _ *storage.Mission, release func()) error {
			release()
			return nil
		},
	}
	defer mgr.StopAll()

	if err := apply.ReplayFromDB(context.Background(), db, mgr); err != nil {
		t.Fatalf("Replay: %v", err)
	}
	if specs := mgr.CurrentLanes(); len(specs) != 0 {
		t.Errorf("CurrentLanes=%v, want empty", specs)
	}
}

func TestReplayFromDBRestoresLanes(t *testing.T) {
	dir := t.TempDir()
	db, err := storage.Open(filepath.Join(dir, "state.db"), storage.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	if err := storage.Migrate(context.Background(), db); err != nil {
		t.Fatal(err)
	}

	state := apply.AppliedState{
		MissionDir: "/tmp/m",
		Lanes: map[string]apply.LaneCfg{
			"alpha": {Concurrency: 2},
			"beta":  {Concurrency: 5, Paused: true},
		},
	}
	data, _ := json.Marshal(state)
	if err := storage.SetAppliedConfig(context.Background(), db, storage.AppliedConfig{
		Data: data, AppliedAt: time.Now().UnixMilli(),
	}); err != nil {
		t.Fatal(err)
	}

	mgr := &lane.Manager{
		DB: db, Logger: slog.Default(), Ctx: context.Background(),
		Spawner: func(_ context.Context, _ *storage.Mission, release func()) error {
			release()
			return nil
		},
	}
	defer mgr.StopAll()

	if err := apply.ReplayFromDB(context.Background(), db, mgr); err != nil {
		t.Fatalf("Replay: %v", err)
	}
	specs := mgr.CurrentLanes()
	if len(specs) != 2 {
		t.Fatalf("len=%d", len(specs))
	}
	byName := map[string]lane.LaneSpec{}
	for _, s := range specs {
		byName[s.Name] = s
	}
	if byName["alpha"].Concurrency != 2 {
		t.Errorf("alpha concurrency=%v", byName["alpha"].Concurrency)
	}
	if !byName["beta"].Paused {
		t.Errorf("beta should be paused")
	}
}
