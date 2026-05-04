package lane

import (
	"context"
	"sort"
	"testing"
	"time"

	"letts/internal/storage"
)

// noopSpawner is a Spawner that does nothing and immediately releases the slot.
func noopSpawner(_ context.Context, _ *storage.Mission, release func()) error {
	release()
	return nil
}

func containsAll(t *testing.T, label string, got, want []string) {
	t.Helper()
	wantSet := make(map[string]struct{}, len(want))
	for _, w := range want {
		wantSet[w] = struct{}{}
	}
	gotSet := make(map[string]struct{}, len(got))
	for _, g := range got {
		gotSet[g] = struct{}{}
	}
	for _, w := range want {
		if _, ok := gotSet[w]; !ok {
			t.Errorf("%s: expected %q in %v", label, w, got)
		}
	}
	for _, g := range got {
		if _, ok := wantSet[g]; !ok {
			t.Errorf("%s: unexpected %q in %v", label, g, got)
		}
	}
}

// TestManagerApplyStartStop verifies that Apply starts and stops lanes correctly.
func TestManagerApplyStartStop(t *testing.T) {
	db := setupTestDB(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	m := &Manager{
		DB:      db,
		Spawner: noopSpawner,
		Logger:  newLogger(),
		Ctx:     ctx,
	}
	defer m.StopAll()

	// First apply: start a and b.
	started, stopped, resized := m.Apply([]LaneSpec{
		{Name: "a", Concurrency: 5},
		{Name: "b", Concurrency: 10},
	})

	containsAll(t, "started", started, []string{"a", "b"})
	if len(stopped) != 0 {
		t.Errorf("expected no stopped, got %v", stopped)
	}
	if len(resized) != 0 {
		t.Errorf("expected no resized, got %v", resized)
	}

	// Second apply: resize a to 7, stop b, start c.
	started2, stopped2, resized2 := m.Apply([]LaneSpec{
		{Name: "a", Concurrency: 7},
		{Name: "c", Concurrency: 3},
	})

	containsAll(t, "started2", started2, []string{"c"})
	containsAll(t, "stopped2", stopped2, []string{"b"})
	containsAll(t, "resized2", resized2, []string{"a"})

	// Verify current state via CurrentLanes.
	current := m.CurrentLanes()
	names := make([]string, len(current))
	conc := make(map[string]int)
	for i, s := range current {
		names[i] = s.Name
		conc[s.Name] = s.Concurrency
	}
	sort.Strings(names)
	if len(names) != 2 || names[0] != "a" || names[1] != "c" {
		t.Errorf("expected lanes [a c], got %v", names)
	}
	if conc["a"] != 7 {
		t.Errorf("expected a concurrency=7, got %d", conc["a"])
	}
	if conc["c"] != 3 {
		t.Errorf("expected c concurrency=3, got %d", conc["c"])
	}

	// b must not reappear.
	for _, s := range current {
		if s.Name == "b" {
			t.Error("lane b should have been stopped")
		}
	}
}

// TestManagerApplyNoChangeNotResized verifies that applying with same concurrency
// does not report resized.
func TestManagerApplyNoChangeNotResized(t *testing.T) {
	db := setupTestDB(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	m := &Manager{
		DB:      db,
		Spawner: noopSpawner,
		Logger:  newLogger(),
		Ctx:     ctx,
	}
	defer m.StopAll()

	m.Apply([]LaneSpec{{Name: "x", Concurrency: 4}})
	_, _, resized := m.Apply([]LaneSpec{{Name: "x", Concurrency: 4}})
	if len(resized) != 0 {
		t.Errorf("expected no resized for same concurrency, got %v", resized)
	}
}

// TestManagerApplyPausedLane verifies pause/resume propagation.
func TestManagerApplyPausedLane(t *testing.T) {
	db := setupTestDB(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	m := &Manager{
		DB:      db,
		Spawner: noopSpawner,
		Logger:  newLogger(),
		Ctx:     ctx,
	}
	defer m.StopAll()

	// Start paused.
	m.Apply([]LaneSpec{{Name: "p", Concurrency: 2, Paused: true}})
	current := m.CurrentLanes()
	if len(current) != 1 || !current[0].Paused {
		t.Errorf("expected paused lane, got %+v", current)
	}

	// Resume via apply.
	m.Apply([]LaneSpec{{Name: "p", Concurrency: 2, Paused: false}})
	current2 := m.CurrentLanes()
	if len(current2) != 1 || current2[0].Paused {
		t.Errorf("expected resumed lane, got %+v", current2)
	}
}

// TestManagerNotify verifies that Notify passes through without panic.
func TestManagerNotify(t *testing.T) {
	db := setupTestDB(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	m := &Manager{
		DB:      db,
		Spawner: noopSpawner,
		Logger:  newLogger(),
		Ctx:     ctx,
	}
	defer m.StopAll()

	m.Apply([]LaneSpec{{Name: "q", Concurrency: 1}})

	// Should not panic.
	m.Notify("q")
	m.Notify("nonexistent")
}

// TestManagerStopAll verifies that StopAll clears all runners.
func TestManagerStopAll(t *testing.T) {
	db := setupTestDB(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	m := &Manager{
		DB:      db,
		Spawner: noopSpawner,
		Logger:  newLogger(),
		Ctx:     ctx,
	}

	m.Apply([]LaneSpec{
		{Name: "x", Concurrency: 1},
		{Name: "y", Concurrency: 1},
	})

	m.StopAll()

	current := m.CurrentLanes()
	if len(current) != 0 {
		t.Errorf("expected 0 lanes after StopAll, got %d: %v", len(current), current)
	}

	cancel()
}
