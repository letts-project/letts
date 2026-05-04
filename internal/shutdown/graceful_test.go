package shutdown_test

import (
	"bytes"
	"context"
	"database/sql"
	"log/slog"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"letts/internal/config"
	"letts/internal/ids"
	"letts/internal/lane"
	"letts/internal/mission"
	"letts/internal/shutdown"
	"letts/internal/storage"
)

func setupShutdownDB(t *testing.T) *sql.DB {
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

func insertRunningMission(t *testing.T, db *sql.DB, lane string) string {
	t.Helper()
	id := ids.NewUUIDv7()
	m := storage.Mission{
		ID: id, Kind: storage.KindMission, Lane: lane,
		MissionName: "ShutdownFixture", Status: storage.StatusRunning,
		Input: []byte(`{}`), InputFingerprint: "fp",
		TimeCreatedMs: time.Now().UnixMilli(),
		TimeStartedMs: sql.NullInt64{Int64: time.Now().UnixMilli(), Valid: true},
	}
	if err := storage.InsertMission(context.Background(), db, &m); err != nil {
		t.Fatal(err)
	}
	return id
}

func markMissionDone(t *testing.T, db *sql.DB, id string) {
	t.Helper()
	if _, err := db.Exec(`UPDATE missions SET status='done', outcome='success' WHERE mission_id=?`, id); err != nil {
		t.Fatal(err)
	}
}

type fakeKiller struct {
	mu     sync.Mutex
	calls  []killCall
	reject bool
	onSig  func(id string)
}

type killCall struct {
	id     string
	reason mission.ExternalKillReason
}

func (f *fakeKiller) SignalKill(id string, reason mission.ExternalKillReason) bool {
	f.mu.Lock()
	f.calls = append(f.calls, killCall{id: id, reason: reason})
	cb := f.onSig
	rej := f.reject
	f.mu.Unlock()
	if cb != nil {
		cb(id)
	}
	return !rej
}

func (f *fakeKiller) signalCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.calls)
}

// syncBuf is a goroutine-safe wrapper around bytes.Buffer for use as
// StatusOut in tests where the drain goroutine writes concurrently with the
// test reading.
type syncBuf struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (s *syncBuf) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.Write(p)
}

func (s *syncBuf) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.String()
}

func (s *syncBuf) Len() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.Len()
}

// newTestCoord builds a coordinator wired to a test DB and a fake killer.
// t.Cleanup cancels its context so the drain goroutine always exits before
// the test returns (otherwise it'd race against db.Close in t.Cleanup).
func newTestCoord(t *testing.T, db *sql.DB) (*shutdown.Coordinator, *lane.Manager, *fakeKiller, context.Context) {
	t.Helper()
	cfg := &config.DugdaleConfig{Limits: config.LimitsConfig{DefaultKillGrace: 50 * time.Millisecond}}
	mgr := &lane.Manager{
		DB: db, Logger: slog.Default(), Ctx: context.Background(),
		Spawner: func(_ context.Context, _ *storage.Mission, release func()) error {
			release()
			return nil
		},
	}
	killer := &fakeKiller{}
	c := shutdown.New(db, cfg, mgr, killer, slog.Default())
	c.StatusInterval = 30 * time.Millisecond
	c.AggressiveInterval = 10 * time.Millisecond
	c.StatusOut = &syncBuf{}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(func() {
		cancel()
		// Drain loop exits on ctx.Done; bound the wait so a buggy test fails fast.
		select {
		case <-time.After(2 * time.Second):
			t.Error("coordinator drain loop didn't exit after ctx cancel")
		case <-waitChan(c):
		}
	})
	return c, mgr, killer, ctx
}

func TestCoordinatorBlockDispatchPhases(t *testing.T) {
	db := setupShutdownDB(t)
	c, _, _, ctx := newTestCoord(t, db)

	if c.BlockNewDispatches() {
		t.Error("Running phase should not block")
	}
	if c.Phase() != shutdown.PhaseRunning {
		t.Errorf("Phase=%s", c.Phase())
	}

	c.Stop(ctx)
	if !c.BlockNewDispatches() {
		t.Error("Draining should block")
	}
	if c.Phase() != shutdown.PhaseDraining {
		t.Errorf("Phase=%s", c.Phase())
	}

	c.Stop(ctx)
	if c.Phase() != shutdown.PhaseAggressive {
		t.Errorf("Phase=%s after 2nd Stop", c.Phase())
	}
	if !c.BlockNewDispatches() {
		t.Error("Aggressive should block")
	}

	c.Stop(ctx)
	c.Stop(ctx)
	if c.Phase() != shutdown.PhaseAggressive && c.Phase() != shutdown.PhaseDone {
		t.Errorf("subsequent Stops should be no-op, phase=%s", c.Phase())
	}
}

// BlockNewDispatches stays true at PhaseDone. The doc comment
// already says "True for any non-Running phase" — this test locks in
// that contract so a refactor that "fixes" Done back to false would
// have to be deliberate. Reasoning: PhaseDone means drain finished but
// the daemon hasn't called http.Server.Shutdown yet; a dispatch landing
// in that brief window deserves the same 503 draining as Draining/
// Aggressive (we're still shutting down, not re-accepting work).
func TestCoordinatorBlockDispatchAtPhaseDone(t *testing.T) {
	db := setupShutdownDB(t)
	c, _, _, ctx := newTestCoord(t, db)

	// Stop with no running missions → drainLoop reaches PhaseDone fast.
	c.Stop(ctx)
	select {
	case <-waitChan(c):
	case <-time.After(time.Second):
		t.Fatal("drainLoop didn't reach PhaseDone")
	}
	if c.Phase() != shutdown.PhaseDone {
		t.Fatalf("Phase=%s want done", c.Phase())
	}
	if !c.BlockNewDispatches() {
		t.Error("Done must still block — daemon is exiting")
	}
}

func TestCoordinatorFinishesWhenNoRunning(t *testing.T) {
	db := setupShutdownDB(t)
	c, _, _, ctx := newTestCoord(t, db)

	c.Stop(ctx)
	select {
	case <-waitChan(c):
	case <-time.After(time.Second):
		t.Fatal("Wait didn't return when no running missions")
	}
	if c.Phase() != shutdown.PhaseDone {
		t.Errorf("Phase=%s, want done", c.Phase())
	}
}

func TestCoordinatorWaitsForRunningToDrain(t *testing.T) {
	db := setupShutdownDB(t)
	c, _, _, ctx := newTestCoord(t, db)
	id1 := insertRunningMission(t, db, "normal")
	id2 := insertRunningMission(t, db, "normal")

	c.Stop(ctx)

	// While running, Wait should not return.
	done := make(chan struct{})
	go func() { c.Wait(); close(done) }()
	select {
	case <-done:
		t.Fatal("Wait returned with running missions")
	case <-time.After(100 * time.Millisecond):
	}

	markMissionDone(t, db, id1)
	markMissionDone(t, db, id2)

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Wait didn't return after missions marked done")
	}
}

// TestCoordinatorDrainErrorDoesNotDeclareDone verifies the previous
// listRunning(ctx) returned nil on query error, and drainLoop treated
// nil as "no running missions" and transitioned to PhaseDone. A
// transient DB busy/closed during shutdown then made the daemon exit
// while mission processes were still alive; their outcomes landed as
// `lost` on the next start instead of killed/dugdale_shutdown.
func TestCoordinatorDrainErrorDoesNotDeclareDone(t *testing.T) {
	db := setupShutdownDB(t)
	c, _, _, ctx := newTestCoord(t, db)
	_ = insertRunningMission(t, db, "normal")

	c.Stop(ctx)

	// Wait one drain tick so listRunning observes the running row first.
	time.Sleep(60 * time.Millisecond)
	if c.Phase() != shutdown.PhaseDraining {
		t.Fatalf("expected PhaseDraining before close, got %s", c.Phase())
	}

	// Close the DB to force subsequent QueryContext calls to fail.
	if err := db.Close(); err != nil {
		t.Fatalf("close db: %v", err)
	}

	// Give drainLoop several ticks to see the error.
	time.Sleep(200 * time.Millisecond)

	// Phase must NOT be Done — a DB error is not drain completion.
	if c.Phase() == shutdown.PhaseDone {
		t.Errorf("Phase=%s; drain declared complete despite DB error", c.Phase())
	}
}

func TestCoordinatorAggressiveSignalsKill(t *testing.T) {
	db := setupShutdownDB(t)
	c, _, killer, ctx := newTestCoord(t, db)

	id := insertRunningMission(t, db, "normal")
	killer.onSig = func(missionID string) {
		// Simulate the mission's kill watcher → finalize → done.
		markMissionDone(t, db, missionID)
	}

	c.Stop(ctx) // → Draining
	c.Stop(ctx) // → Aggressive

	select {
	case <-waitChan(c):
	case <-time.After(time.Second):
		t.Fatal("Wait didn't return after aggressive kill")
	}
	if killer.signalCount() != 1 {
		t.Errorf("kill calls=%d, want 1", killer.signalCount())
	}
	if killer.calls[0].id != id || killer.calls[0].reason != mission.KillDugdaleShutdown {
		t.Errorf("call=%+v", killer.calls[0])
	}
}

func TestCoordinatorPausesLanes(t *testing.T) {
	db := setupShutdownDB(t)
	c, mgr, _, ctx := newTestCoord(t, db)

	mgr.Apply([]lane.LaneSpec{{Name: "alpha", Concurrency: 1}, {Name: "beta", Concurrency: 1}})
	defer mgr.StopAll()

	// Put a queued mission in alpha; if the lane is paused, it should remain queued.
	id := ids.NewUUIDv7()
	_ = storage.InsertMission(context.Background(), db, &storage.Mission{
		ID: id, Kind: storage.KindMission, Lane: "alpha",
		MissionName: "x", Status: storage.StatusQueued,
		Input: []byte(`{}`), InputFingerprint: "fp",
		TimeCreatedMs: time.Now().UnixMilli(),
	})

	c.Stop(ctx)
	mgr.Notify("alpha")

	time.Sleep(100 * time.Millisecond)

	m, _ := storage.GetMission(context.Background(), db, id)
	if m.Status != storage.StatusQueued {
		t.Errorf("mission picked up after pause: status=%q", m.Status)
	}

	// finish drain so coordinator exits cleanly
	select {
	case <-waitChan(c):
	case <-time.After(time.Second):
		t.Fatal("Wait timeout")
	}
}

func TestCoordinatorPrintStatusFormat(t *testing.T) {
	db := setupShutdownDB(t)
	c, _, _, ctx := newTestCoord(t, db)
	buf := &syncBuf{}
	c.StatusOut = buf

	insertRunningMission(t, db, "normal")
	c.Stop(ctx)

	// Wait for the drain loop to print at least once.
	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		if buf.Len() > 0 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	got := buf.String()
	if !strings.Contains(got, "graceful-shutdown") {
		t.Errorf("missing prefix: %q", got)
	}
	if !strings.Contains(got, "running") {
		t.Errorf("missing 'running' label: %q", got)
	}
	if !strings.Contains(got, "ShutdownFixture") {
		t.Errorf("missing mission name: %q", got)
	}
}

func TestCoordinatorAggressiveWithoutKillerLogsWarn(t *testing.T) {
	db := setupShutdownDB(t)
	c := shutdown.New(db, &config.DugdaleConfig{}, nil, nil, slog.Default())
	c.StatusInterval = 10 * time.Millisecond
	c.AggressiveInterval = 5 * time.Millisecond
	c.StatusOut = &syncBuf{}

	id := insertRunningMission(t, db, "normal")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c.Stop(ctx)
	c.Stop(ctx)
	// Without killer, missions don't get signalled — drain wouldn't converge
	// unless we mark them done externally.
	time.Sleep(50 * time.Millisecond)
	if c.Phase() != shutdown.PhaseAggressive {
		t.Errorf("Phase=%s, want aggressive", c.Phase())
	}
	// Mark done so drain converges before test ends.
	markMissionDone(t, db, id)
	select {
	case <-waitChan(c):
	case <-time.After(time.Second):
		t.Fatal("Wait timeout after marking done")
	}
}

func TestCoordinatorCtxCancelExitsDrain(t *testing.T) {
	db := setupShutdownDB(t)
	c, _, _, _ := newTestCoord(t, db)
	insertRunningMission(t, db, "normal")
	ctx, cancel := context.WithCancel(context.Background())
	c.Stop(ctx)
	cancel()
	select {
	case <-waitChan(c):
	case <-time.After(time.Second):
		t.Fatal("Wait didn't return after ctx cancel")
	}
}

// waitChan returns a channel that closes when c.Wait returns.
func waitChan(c *shutdown.Coordinator) <-chan struct{} {
	done := make(chan struct{})
	go func() { c.Wait(); close(done) }()
	return done
}
