package lane

import (
	"context"
	"database/sql"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"letts/internal/ids"
	"letts/internal/storage"
)

func setupTestDB(t *testing.T) *sql.DB {
	t.Helper()
	dir := t.TempDir()
	db, err := storage.Open(filepath.Join(dir, "state.db"), storage.Options{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if err := storage.Migrate(context.Background(), db); err != nil {
		t.Fatal(err)
	}
	return db
}

func insertQueued(t *testing.T, db *sql.DB, lane string, n int) []string {
	t.Helper()
	ids_ := make([]string, n)
	for i := range ids_ {
		id := ids.NewUUIDv7()
		ids_[i] = id
		err := storage.InsertMission(context.Background(), db, &storage.Mission{
			ID:               id,
			Kind:             storage.KindMission,
			Lane:             lane,
			MissionName:      "test",
			Status:           storage.StatusQueued,
			Input:            []byte("{}"),
			InputFingerprint: id,
			TimeCreatedMs:    time.Now().UnixMilli(),
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	return ids_
}

func newLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug}))
}

// TestRunnerConcurrencyLimit verifies that at most `concurrency` spawns are
// in-flight at once and all N missions are eventually picked up exactly once.
func TestRunnerConcurrencyLimit(t *testing.T) {
	db := setupTestDB(t)
	const N = 10
	const concurrency = 2
	const lane = "testlane"

	insertedIDs := insertQueued(t, db, lane, N)

	var mu sync.Mutex
	var inFlight int
	var maxInFlight int
	var pickedIDs []string

	// done is closed when all N missions have been spawned and released.
	done := make(chan struct{})
	var doneOnce sync.Once

	spawner := Spawner(func(ctx context.Context, m *storage.Mission, releaseSlot func()) error {
		mu.Lock()
		inFlight++
		if inFlight > maxInFlight {
			maxInFlight = inFlight
		}
		if inFlight > concurrency {
			mu.Unlock()
			t.Errorf("in-flight %d exceeds concurrency %d", inFlight, concurrency)
			releaseSlot()
			return nil
		}
		mu.Unlock()

		// Simulate work.
		time.Sleep(5 * time.Millisecond)

		mu.Lock()
		inFlight--
		pickedIDs = append(pickedIDs, m.ID)
		allDone := len(pickedIDs) == N
		mu.Unlock()

		releaseSlot()
		if allDone {
			doneOnce.Do(func() { close(done) })
		}
		return nil
	})

	r := &Runner{
		Lane:        lane,
		DB:          db,
		Spawner:     spawner,
		Concurrency: concurrency,
		Logger:      newLogger(),
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	r.Start(ctx)
	defer r.Stop()

	select {
	case <-done:
	case <-ctx.Done():
		t.Fatal("timed out waiting for all missions to be picked up")
	}

	mu.Lock()
	defer mu.Unlock()

	if len(pickedIDs) != N {
		t.Errorf("expected %d picked, got %d", N, len(pickedIDs))
	}
	// No duplicates.
	seen := make(map[string]int)
	for _, id := range pickedIDs {
		seen[id]++
	}
	for id, count := range seen {
		if count > 1 {
			t.Errorf("mission %s picked %d times", id, count)
		}
	}
	// All inserted IDs were picked.
	sort.Strings(insertedIDs)
	sort.Strings(pickedIDs)
	for i, id := range insertedIDs {
		if id != pickedIDs[i] {
			t.Errorf("inserted[%d]=%s != picked[%d]=%s", i, id, i, pickedIDs[i])
		}
	}
	if maxInFlight > concurrency {
		t.Errorf("max in-flight was %d, exceeds concurrency %d", maxInFlight, concurrency)
	}
	t.Logf("max in-flight: %d (limit: %d)", maxInFlight, concurrency)
}

// TestRunnerPauseResume verifies that Pause stops new pickups and Resume resumes them.
//
// Strategy: start runner with concurrency=1. Insert 3 missions. The spawner
// holds a gateChannel so we can control when each spawn finishes.
// Sequence:
//  1. Mission 1 gets picked and held.
//  2. Pause the runner before releasing mission 1.
//  3. Release mission 1 → slot is freed, but runner is paused so no new pickup.
//  4. Wait briefly; assert only 1 picked.
//  5. Resume → missions 2 and 3 get picked.
func TestRunnerPauseResume(t *testing.T) {
	db := setupTestDB(t)
	const laneName = "pauselane"

	insertQueued(t, db, laneName, 3)

	var mu sync.Mutex
	var pickedCount int

	// gate controls spawner: send on it to let one spawner proceed.
	gate := make(chan struct{}, 10)

	spawner := Spawner(func(ctx context.Context, m *storage.Mission, releaseSlot func()) error {
		select {
		case <-gate:
		case <-ctx.Done():
			releaseSlot()
			return nil
		}
		mu.Lock()
		pickedCount++
		mu.Unlock()
		releaseSlot()
		return nil
	})

	r := &Runner{
		Lane:        laneName,
		DB:          db,
		Spawner:     spawner,
		Concurrency: 1,
		Logger:      newLogger(),
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	r.Start(ctx)
	defer r.Stop()

	// Wait until runner has picked mission 1 — the spawner is blocked
	// on gate, so inflight goes 0 → 1 and stays.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if r.inflight.Load() == 1 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}

	// Pause before releasing the slot.
	r.Pause()

	// Release the spawner for mission 1.
	gate <- struct{}{}

	// Slot is now free, but runner is paused.
	// Allow a generous window for a (wrong) pickup to happen.
	time.Sleep(80 * time.Millisecond)

	mu.Lock()
	afterPause := pickedCount
	mu.Unlock()
	if afterPause != 1 {
		t.Errorf("expected exactly 1 picked while paused, got %d", afterPause)
	}

	// Resume — allow remaining 2 to proceed.
	r.Resume()
	gate <- struct{}{}
	gate <- struct{}{}

	// Wait for all 3 to be picked.
	deadline2 := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline2) {
		mu.Lock()
		c := pickedCount
		mu.Unlock()
		if c >= 3 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	mu.Lock()
	final := pickedCount
	mu.Unlock()
	if final != 3 {
		t.Errorf("expected 3 picked after resume, got %d", final)
	}
}

// TestRunnerMarkRemovingBlocksFurtherPickups enforces: once apply
// marks a lane "removing", the runner
// must not pick any more missions, even if the queue has rows and
// the runner is at idle (under concurrency limit). MarkRemoving
// blocks until the runner acks — apply can then safely terminate
// queued missions without racing the pickup loop.
func TestRunnerMarkRemovingBlocksFurtherPickups(t *testing.T) {
	db := setupTestDB(t)
	const lane = "removinglane"

	var pickedCount atomic.Int64
	spawner := Spawner(func(_ context.Context, _ *storage.Mission, releaseSlot func()) error {
		pickedCount.Add(1)
		releaseSlot()
		return nil
	})

	r := &Runner{
		Lane: lane, DB: db, Spawner: spawner,
		Concurrency: 4, Logger: newLogger(),
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	r.Start(ctx)
	defer r.Stop()

	// Wait until the loop has done a no-mission pickup pass (so the
	// flag-check at top of iteration is reached).
	time.Sleep(20 * time.Millisecond)

	// Mark removing. Blocks until loop acknowledges.
	r.MarkRemoving()

	// Now queue 3 missions AFTER the runner has acked removing. The
	// loop is parked on ctx.Done in the removing branch; PickQueuedForLane
	// must not run for these rows.
	insertQueued(t, db, lane, 3)
	// Wake the runner just in case (Notify is what dispatch handlers do).
	r.Notify()

	// Give a generous window for an erroneous pickup.
	time.Sleep(100 * time.Millisecond)

	if got := pickedCount.Load(); got != 0 {
		t.Errorf("runner picked %d after MarkRemoving; want 0", got)
	}
}

// TestRunnerSetConcurrencyShrinkBlocksUntilDrain verifies the
// fix: when SetConcurrency reduces the limit BELOW current in-flight,
// the runner does NOT pick more missions until releases bring in-flight
// under the new limit. The previous channel-semaphore swap let new
// pickups draw from a fresh channel while running spawners still held
// tickets in the old one, exceeding the new limit until drain.
func TestRunnerSetConcurrencyShrinkBlocksUntilDrain(t *testing.T) {
	db := setupTestDB(t)
	const lane = "shrinklane"
	insertQueued(t, db, lane, 5)

	gate := make(chan struct{}, 5)
	var pickedCount atomic.Int64
	spawner := Spawner(func(_ context.Context, _ *storage.Mission, releaseSlot func()) error {
		pickedCount.Add(1)
		<-gate // block until test releases
		releaseSlot()
		return nil
	})

	r := &Runner{
		Lane: lane, DB: db, Spawner: spawner,
		Concurrency: 4, Logger: newLogger(),
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	r.Start(ctx)
	defer r.Stop()

	// Wait until all 4 are in-flight (blocked on gate).
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if r.inflight.Load() == 4 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if got := r.inflight.Load(); got != 4 {
		t.Fatalf("expected 4 in-flight, got %d", got)
	}

	// Shrink to 1. Currently 4 are running; new pickups must be gated
	// until 3 of them finish.
	r.SetConcurrency(1)

	// Give the loop a moment — it should NOT pick the 5th mission while
	// inflight (4) >= limit (1).
	time.Sleep(50 * time.Millisecond)
	if got := pickedCount.Load(); got != 4 {
		t.Errorf("runner picked %d while shrinking from 4 to 1; want still 4", got)
	}

	// Release 3 of the in-flight missions. inflight goes 4→1. Still
	// >= limit (1), so runner still won't pick #5.
	gate <- struct{}{}
	gate <- struct{}{}
	gate <- struct{}{}
	deadline2 := time.Now().Add(1 * time.Second)
	for time.Now().Before(deadline2) {
		if r.inflight.Load() == 1 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	time.Sleep(50 * time.Millisecond)
	if got := pickedCount.Load(); got != 4 {
		t.Errorf("runner picked %d with inflight=limit=1; want still 4", got)
	}

	// Release one more — inflight 1→0, runner now picks #5.
	gate <- struct{}{}
	deadline3 := time.Now().Add(1 * time.Second)
	for time.Now().Before(deadline3) {
		if pickedCount.Load() == 5 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if got := pickedCount.Load(); got != 5 {
		t.Errorf("after release runner picked %d; want 5", got)
	}
	// Drain remaining.
	gate <- struct{}{}
}

// TestRunnerSetConcurrency verifies that reducing concurrency takes effect
// for new pickups.
func TestRunnerSetConcurrency(t *testing.T) {
	db := setupTestDB(t)
	const lane = "resizelane"

	insertQueued(t, db, lane, 6)

	var mu sync.Mutex
	var inFlight int
	var maxInFlight int
	var totalPicked int
	done := make(chan struct{})
	var doneOnce sync.Once

	spawner := Spawner(func(ctx context.Context, m *storage.Mission, releaseSlot func()) error {
		mu.Lock()
		inFlight++
		if inFlight > maxInFlight {
			maxInFlight = inFlight
		}
		mu.Unlock()

		time.Sleep(10 * time.Millisecond)

		mu.Lock()
		inFlight--
		totalPicked++
		all := totalPicked == 6
		mu.Unlock()

		releaseSlot()
		if all {
			doneOnce.Do(func() { close(done) })
		}
		return nil
	})

	r := &Runner{
		Lane:        lane,
		DB:          db,
		Spawner:     spawner,
		Concurrency: 4,
		Logger:      newLogger(),
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	r.Start(ctx)
	defer r.Stop()

	// Resize down to 1 while running.
	time.Sleep(5 * time.Millisecond)
	r.SetConcurrency(1)

	select {
	case <-done:
	case <-ctx.Done():
		t.Fatal("timed out after SetConcurrency")
	}

	mu.Lock()
	defer mu.Unlock()
	if totalPicked != 6 {
		t.Errorf("expected 6 picked, got %d", totalPicked)
	}
	// maxInFlight may be up to initial concurrency=4 before resize.
	t.Logf("max in-flight: %d", maxInFlight)
}
