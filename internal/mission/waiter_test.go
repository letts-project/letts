package mission

import (
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"letts/internal/config"
	"letts/internal/ids"
	"letts/internal/storage"
)

// mapCollectErrorToReason converts the CollectOutputs error
// (which prefixes its messages with a discriminator like "missing_output:")
// to a stable string the wire format quotes verbatim. A regression that
// loses one of these classifications would silently fold distinct failure
// modes onto "output_collect_failed" — observable only via the audit log,
// not via tests. Lock every documented branch.
func TestMapCollectErrorToReasonClassifications(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want string
	}{
		{"missing_output", fmt.Errorf("missing_output: result"), "missing_output"},
		{"path_escape", fmt.Errorf("output_path_escape: foo (symlink)"), "output_path_escape"},
		{"not_regular_file", fmt.Errorf("output_not_regular_file: dir/"), "output_not_regular_file"},
		{"too_large", fmt.Errorf("output_too_large: big (size > 1)"), "output_too_large"},
		// Soft-cap during CollectOutputs maps to the same fail_reason
		// bucket as a kernel ENOSPC so operators only see one "quota"
		// category.
		{"data_dir_quota", fmt.Errorf("data_dir_quota_exceeded: used=1 cap=0"), "disk_quota_exceeded"},
		{"unknown_falls_back", fmt.Errorf("openat foo: bar baz"), "output_collect_failed"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := mapCollectErrorToReason(c.err)
			if got != c.want {
				t.Errorf("got=%q want=%q (err=%v)", got, c.want, c.err)
			}
		})
	}
}

// decodeBase64Trim accepts either plain base64 or base64 with embedded
// whitespace (the BSD `base64` utility wraps every 76 chars).
func decodeBase64Trim(s string) ([]byte, error) {
	clean := strings.Map(func(r rune) rune {
		if r == '\n' || r == '\r' || r == ' ' || r == '\t' {
			return -1
		}
		return r
	}, s)
	return base64.StdEncoding.DecodeString(clean)
}

func writeScript(t *testing.T, dir, name, body string) string {
	t.Helper()
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, []byte("#!/bin/sh\n"+body), 0o755); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
	return path
}

func runFixtureCfg(dataDir string) *config.DugdaleConfig {
	return &config.DugdaleConfig{
		DataDir: dataDir,
		Limits: config.LimitsConfig{
			MaxOutputBuffer:      8 * 1024 * 1024,
			MaxEventsBuffer:      1024 * 1024,
			MaxEventLineSize:     1024 * 1024,
			MaxReturnValueSize:   768 * 1024,
			MaxFailMessageSize:   64 * 1024,
			MaxFailDetailsSize:   256 * 1024,
			MaxProgressRate:      100,
			ProgressBufferSize:   256 * 1024,
			MaxOutputFilesPerMsn: 32,
			MaxOutputFileSize:    0,
			DefaultKillGrace:     150 * time.Millisecond,
			ReaderPostExitGrace:  300 * time.Millisecond,
		},
	}
}

// runFixture inserts a mission and runtime row pointing at scriptPath, returns
// the mission id. command_template is `["sh", "{mission_path}"]` so the script
// is executed via /bin/sh.
func runFixture(t *testing.T, db *sql.DB, scriptPath string, lane string, timeoutMs int64) string {
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
	if timeoutMs > 0 {
		m.TimeoutMs = sql.NullInt64{Int64: timeoutMs, Valid: true}
	}
	if err := storage.InsertMission(context.Background(), db, &m); err != nil {
		t.Fatalf("insert mission: %v", err)
	}
	rt := storage.MissionRuntime{
		MissionID:           id,
		MissionDir:          filepath.Dir(scriptPath),
		CommandTemplate:     `["sh", "{mission_path}"]`,
		MissionPathTemplate: "",
		ValidateMissionFile: true,
	}
	if err := storage.InsertRuntime(context.Background(), db, &rt); err != nil {
		t.Fatalf("insert runtime: %v", err)
	}
	return id
}

func TestRunSuccessNoOutput(t *testing.T) {
	db := openTestDB(t)
	cfg := runFixtureCfg(t.TempDir())
	scriptDir := t.TempDir()
	script := writeScript(t, scriptDir, "ok.sh", `echo '{"event":"success","return":{"k":"v"}}' >&3
exit 0`)
	id := runFixture(t, db, script, "normal", 0)

	m, _ := storage.GetMission(context.Background(), db, id)
	killCh := make(chan ExternalKillReason, 1)
	if err := Run(context.Background(), cfg, db, m, killCh, func() {}); err != nil {
		t.Fatalf("Run: %v", err)
	}

	got := loadMission(t, db, id)
	if got.Status != storage.StatusDone || got.Outcome.String != "success" {
		t.Errorf("status=%q outcome=%q, want done/success", got.Status, got.Outcome.String)
	}
	if string(got.ReturnValue) != `{"k":"v"}` {
		t.Errorf("ReturnValue=%q", string(got.ReturnValue))
	}
}

func TestRunSuccessWithOutputFile(t *testing.T) {
	db := openTestDB(t)
	cfg := runFixtureCfg(t.TempDir())
	scriptDir := t.TempDir()
	script := writeScript(t, scriptDir, "out.sh", `echo "payload" > "$LETTS_WORKDIR/out/result"
echo '{"event":"output_file","key":"result"}' >&3
echo '{"event":"success"}' >&3
exit 0`)
	id := runFixture(t, db, script, "normal", 0)

	m, _ := storage.GetMission(context.Background(), db, id)
	if err := Run(context.Background(), cfg, db, m, make(chan ExternalKillReason, 1), func() {}); err != nil {
		t.Fatalf("Run: %v", err)
	}

	got := loadMission(t, db, id)
	if got.Outcome.String != "success" {
		t.Errorf("outcome=%q, want success", got.Outcome.String)
	}
	refs, err := storage.RefsByMission(context.Background(), db, id)
	if err != nil {
		t.Fatalf("refs: %v", err)
	}
	if len(refs) != 1 || refs[0].Role != "result" {
		t.Errorf("refs=%v", refs)
	}
	sf, err := storage.GetStaging(context.Background(), db, refs[0].StagingID)
	if err != nil {
		t.Fatalf("staging: %v", err)
	}
	if sf.State != storage.StagingComplete {
		t.Errorf("staging state=%q, want complete", sf.State)
	}
	if sf.Size != 8 { // "payload\n"
		t.Errorf("staging size=%d, want 8", sf.Size)
	}
}

func TestRunFailExplicit(t *testing.T) {
	db := openTestDB(t)
	cfg := runFixtureCfg(t.TempDir())
	scriptDir := t.TempDir()
	script := writeScript(t, scriptDir, "fail.sh", `echo '{"event":"fail","message":"oops","reason":"explicit","exit_code":2}' >&3
exit 2`)
	id := runFixture(t, db, script, "normal", 0)
	m, _ := storage.GetMission(context.Background(), db, id)
	if err := Run(context.Background(), cfg, db, m, make(chan ExternalKillReason, 1), func() {}); err != nil {
		t.Fatalf("Run: %v", err)
	}
	got := loadMission(t, db, id)
	if got.Outcome.String != "failed" || got.FailReason.String != "explicit" {
		t.Errorf("outcome=%q reason=%q, want failed/explicit", got.Outcome.String, got.FailReason.String)
	}
	if got.FailMessage.String != "oops" {
		t.Errorf("FailMessage=%q", got.FailMessage.String)
	}
	if got.ExitCode.Int64 != 2 {
		t.Errorf("ExitCode=%d", got.ExitCode.Int64)
	}
}

func TestRunImplicitSuccess(t *testing.T) {
	db := openTestDB(t)
	cfg := runFixtureCfg(t.TempDir())
	scriptDir := t.TempDir()
	script := writeScript(t, scriptDir, "implicit.sh", `echo "stdout"
exit 0`)
	id := runFixture(t, db, script, "normal", 0)
	m, _ := storage.GetMission(context.Background(), db, id)
	if err := Run(context.Background(), cfg, db, m, make(chan ExternalKillReason, 1), func() {}); err != nil {
		t.Fatalf("Run: %v", err)
	}
	got := loadMission(t, db, id)
	if got.Outcome.String != "success" {
		t.Errorf("outcome=%q", got.Outcome.String)
	}
}

func TestRunImplicitFailNonzeroExit(t *testing.T) {
	db := openTestDB(t)
	cfg := runFixtureCfg(t.TempDir())
	scriptDir := t.TempDir()
	script := writeScript(t, scriptDir, "die.sh", `exit 7`)
	id := runFixture(t, db, script, "normal", 0)
	m, _ := storage.GetMission(context.Background(), db, id)
	if err := Run(context.Background(), cfg, db, m, make(chan ExternalKillReason, 1), func() {}); err != nil {
		t.Fatalf("Run: %v", err)
	}
	got := loadMission(t, db, id)
	if got.Outcome.String != "failed" || got.FailReason.String != "no_event_nonzero_exit" {
		t.Errorf("outcome=%q reason=%q", got.Outcome.String, got.FailReason.String)
	}
	if got.ExitCode.Int64 != 7 {
		t.Errorf("ExitCode=%d", got.ExitCode.Int64)
	}
}

func TestRunTimeout(t *testing.T) {
	db := openTestDB(t)
	cfg := runFixtureCfg(t.TempDir())
	scriptDir := t.TempDir()
	script := writeScript(t, scriptDir, "sleep.sh", `sleep 30`)
	id := runFixture(t, db, script, "normal", 200) // 200ms timeout
	m, _ := storage.GetMission(context.Background(), db, id)

	start := time.Now()
	if err := Run(context.Background(), cfg, db, m, make(chan ExternalKillReason, 1), func() {}); err != nil {
		t.Fatalf("Run: %v", err)
	}
	elapsed := time.Since(start)
	if elapsed > 5*time.Second {
		t.Errorf("Run took %v, expected ≤ 5s", elapsed)
	}

	got := loadMission(t, db, id)
	if got.Outcome.String != "timeout" {
		t.Errorf("outcome=%q, want timeout", got.Outcome.String)
	}
}

// TestRunReturnsPromptlyWhenDescendantHoldsStdio: a mission that backgrounds
// a child and exits leaves the stdout/stderr pipe write-ends (and fd 3) open
// in the survivor. cmd.Wait must not block on those pipes until the
// descendant exits — that would pin the lane slot and keep the row 'running'
// for as long as the daemonized child lives. WaitDelay bounds the post-exit
// pipe wait to reader_post_exit_grace, so Run returns promptly and the
// mission still finalizes success/exit 0 (exec.ErrWaitDelay leaves
// ProcessState intact).
func TestRunReturnsPromptlyWhenDescendantHoldsStdio(t *testing.T) {
	db := openTestDB(t)
	cfg := runFixtureCfg(t.TempDir())
	scriptDir := t.TempDir()
	// Deliberately NO redirections: the point is that the backgrounded sleep
	// INHERITS the pipes. The 15s sleep is far beyond the assertion bound,
	// so a regression (Wait blocking until the descendant exits) fails the
	// elapsed check decisively.
	script := writeScript(t, scriptDir, "daemonize.sh", `(sleep 15 &)
exit 0`)
	id := runFixture(t, db, script, "normal", 0)
	m, _ := storage.GetMission(context.Background(), db, id)

	start := time.Now()
	if err := Run(context.Background(), cfg, db, m, make(chan ExternalKillReason, 1), func() {}); err != nil {
		t.Fatalf("Run: %v", err)
	}
	elapsed := time.Since(start)
	// Expected ≈ 300ms WaitDelay + ≤300ms fd3 grace; 10s is a generous CI
	// bound that still sits well under the descendant's 15s lifetime.
	if elapsed > 10*time.Second {
		t.Errorf("Run took %v; pipes held by a surviving descendant must not block reaping", elapsed)
	}

	got := loadMission(t, db, id)
	if got.Status != storage.StatusDone || got.Outcome.String != "success" {
		t.Errorf("status=%q outcome=%q, want done/success", got.Status, got.Outcome.String)
	}
	if got.ExitCode.Int64 != 0 {
		t.Errorf("ExitCode=%d, want 0", got.ExitCode.Int64)
	}
}

func TestRunKillByAPI(t *testing.T) {
	db := openTestDB(t)
	cfg := runFixtureCfg(t.TempDir())
	scriptDir := t.TempDir()
	script := writeScript(t, scriptDir, "sleep.sh", `sleep 30`)
	id := runFixture(t, db, script, "normal", 0)
	m, _ := storage.GetMission(context.Background(), db, id)

	killCh := make(chan ExternalKillReason, 1)
	done := make(chan error, 1)
	go func() {
		done <- Run(context.Background(), cfg, db, m, killCh, func() {})
	}()

	// Give the script a moment to start, then send the kill reason.
	time.Sleep(50 * time.Millisecond)
	killCh <- KillByAPI

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run didn't return after kill")
	}

	got := loadMission(t, db, id)
	if got.Outcome.String != "killed" || got.FailReason.String != "killed_by_api" {
		t.Errorf("outcome=%q reason=%q", got.Outcome.String, got.FailReason.String)
	}
}

func TestRunCtxCancelTreatedAsShutdown(t *testing.T) {
	db := openTestDB(t)
	cfg := runFixtureCfg(t.TempDir())
	scriptDir := t.TempDir()
	script := writeScript(t, scriptDir, "sleep.sh", `sleep 30`)
	id := runFixture(t, db, script, "normal", 0)
	m, _ := storage.GetMission(context.Background(), db, id)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- Run(ctx, cfg, db, m, make(chan ExternalKillReason, 1), func() {})
	}()
	time.Sleep(50 * time.Millisecond)
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run didn't return after ctx cancel")
	}
	got := loadMission(t, db, id)
	if got.Outcome.String != "killed" || got.FailReason.String != "dugdale_shutdown" {
		t.Errorf("outcome=%q reason=%q", got.Outcome.String, got.FailReason.String)
	}
}

func TestRunSpawnFailureCrashed(t *testing.T) {
	db := openTestDB(t)
	cfg := runFixtureCfg(t.TempDir())
	scriptDir := t.TempDir()
	id := runFixture(t, db, filepath.Join(scriptDir, "missing.sh"), "normal", 0)
	// validate_mission_file=true with a missing script makes ResolveCommand
	// return ErrMissionNotFound; finalizeCrashed maps that to fail_reason=
	// "mission_not_found" (not generic "spawn_failed").
	m, _ := storage.GetMission(context.Background(), db, id)
	if err := Run(context.Background(), cfg, db, m, make(chan ExternalKillReason, 1), func() {}); err != nil {
		t.Fatalf("Run: %v", err)
	}
	got := loadMission(t, db, id)
	if got.Outcome.String != "crashed" || got.FailReason.String != "mission_not_found" {
		t.Errorf("outcome=%q reason=%q, want crashed/mission_not_found", got.Outcome.String, got.FailReason.String)
	}
}

func TestRunMissingOutputDeclaredButNotWritten(t *testing.T) {
	db := openTestDB(t)
	cfg := runFixtureCfg(t.TempDir())
	scriptDir := t.TempDir()
	script := writeScript(t, scriptDir, "missing_out.sh", `echo '{"event":"output_file","key":"ghost"}' >&3
echo '{"event":"success"}' >&3
exit 0`)
	id := runFixture(t, db, script, "normal", 0)
	m, _ := storage.GetMission(context.Background(), db, id)
	if err := Run(context.Background(), cfg, db, m, make(chan ExternalKillReason, 1), func() {}); err != nil {
		t.Fatalf("Run: %v", err)
	}
	got := loadMission(t, db, id)
	if got.Outcome.String != "failed" || got.FailReason.String != "missing_output" {
		t.Errorf("outcome=%q reason=%q, want failed/missing_output", got.Outcome.String, got.FailReason.String)
	}
}

func TestRunReleaseCallbackFires(t *testing.T) {
	db := openTestDB(t)
	cfg := runFixtureCfg(t.TempDir())
	scriptDir := t.TempDir()
	script := writeScript(t, scriptDir, "ok.sh", `echo '{"event":"success"}' >&3
exit 0`)
	id := runFixture(t, db, script, "normal", 0)
	m, _ := storage.GetMission(context.Background(), db, id)
	released := false
	if err := Run(context.Background(), cfg, db, m, make(chan ExternalKillReason, 1), func() { released = true }); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if !released {
		t.Error("release callback not invoked")
	}
}

func TestRunProgressEventsAppendedToFile(t *testing.T) {
	db := openTestDB(t)
	cfg := runFixtureCfg(t.TempDir())
	scriptDir := t.TempDir()
	script := writeScript(t, scriptDir, "progress.sh", `for i in 1 2 3; do
  echo '{"event":"progress","value":0.'$i',"message":"step '$i'"}' >&3
done
echo '{"event":"success"}' >&3
exit 0`)
	id := runFixture(t, db, script, "normal", 0)
	m, _ := storage.GetMission(context.Background(), db, id)
	if err := Run(context.Background(), cfg, db, m, make(chan ExternalKillReason, 1), func() {}); err != nil {
		t.Fatalf("Run: %v", err)
	}
	events := loadEvents(t, cfg.DataDir, id)
	progress := 0
	for _, ev := range events {
		if ev["event"] == "progress" {
			progress++
		}
	}
	if progress != 3 {
		t.Errorf("progress events=%d, want 3 (events=%v)", progress, events)
	}
}

// TestRunProgressDroppedInDoneEvent verifies that a mission emitting more
// progress events than max_progress_rate allows surfaces the drop count
// in the done event as `progress_dropped: N`. Without the drop count
// consumers can't tell that the rate limiter hid signals.
func TestRunProgressDroppedInDoneEvent(t *testing.T) {
	db := openTestDB(t)
	cfg := runFixtureCfg(t.TempDir())
	// Force a tight rate-limit so any burst trips drops deterministically.
	cfg.Limits.MaxProgressRate = 1
	scriptDir := t.TempDir()
	// Emit 5 progress events back-to-back; the 1/sec budget keeps one and
	// drops the rest (mission still exits success).
	script := writeScript(t, scriptDir, "burst.sh", `for i in 1 2 3 4 5; do
  echo '{"event":"progress","value":0.'$i',"message":"step '$i'"}' >&3
done
echo '{"event":"success"}' >&3
exit 0`)
	id := runFixture(t, db, script, "normal", 0)
	m, _ := storage.GetMission(context.Background(), db, id)
	if err := Run(context.Background(), cfg, db, m, make(chan ExternalKillReason, 1), func() {}); err != nil {
		t.Fatalf("Run: %v", err)
	}
	events := loadEvents(t, cfg.DataDir, id)
	var done map[string]any
	for _, ev := range events {
		if ev["event"] == "done" {
			done = ev
			break
		}
	}
	if done == nil {
		t.Fatalf("no done event in %v", events)
	}
	dropped, ok := done["progress_dropped"]
	if !ok {
		t.Fatalf("done event missing progress_dropped: %v", done)
	}
	if n, ok := dropped.(float64); !ok || n < 1 {
		t.Errorf("progress_dropped=%v, want >=1", dropped)
	}
}

func TestRunCapturesStdoutAndStderr(t *testing.T) {
	db := openTestDB(t)
	cfg := runFixtureCfg(t.TempDir())
	scriptDir := t.TempDir()
	script := writeScript(t, scriptDir, "io.sh", `echo "out line"
echo "err line" >&2
echo '{"event":"success"}' >&3
exit 0`)
	id := runFixture(t, db, script, "normal", 0)
	m, _ := storage.GetMission(context.Background(), db, id)
	if err := Run(context.Background(), cfg, db, m, make(chan ExternalKillReason, 1), func() {}); err != nil {
		t.Fatalf("Run: %v", err)
	}
	shard, _ := ids.ShardPath(id)
	stdoutPath := filepath.Join(cfg.DataDir, "output", shard, id+"-stdout")
	stderrPath := filepath.Join(cfg.DataDir, "output", shard, id+"-stderr")
	if b, err := os.ReadFile(stdoutPath); err != nil || string(b) != "out line\n" {
		t.Errorf("stdout=%q err=%v", string(b), err)
	}
	if b, err := os.ReadFile(stderrPath); err != nil || string(b) != "err line\n" {
		t.Errorf("stderr=%q err=%v", string(b), err)
	}
}

func TestRunOOMDetectorTripsOnPHPMarker(t *testing.T) {
	db := openTestDB(t)
	cfg := runFixtureCfg(t.TempDir())
	scriptDir := t.TempDir()
	script := writeScript(t, scriptDir, "oom.sh", `echo "PHP Fatal error:  Allowed memory size of 16777216 bytes exhausted" >&2
exit 255`)
	id := runFixture(t, db, script, "normal", 0)
	m, _ := storage.GetMission(context.Background(), db, id)
	if err := Run(context.Background(), cfg, db, m, make(chan ExternalKillReason, 1), func() {}); err != nil {
		t.Fatalf("Run: %v", err)
	}
	got := loadMission(t, db, id)
	if got.Outcome.String != "oom" || got.FailReason.String != "php_memory_limit" {
		t.Errorf("outcome=%q reason=%q, want oom/php_memory_limit", got.Outcome.String, got.FailReason.String)
	}
}

// TestRunDeliversInputViaStdin verifies the mission process receives the
// user input JSON on fd 0. The wire payload is the user JSON verbatim —
// file metadata goes via env vars only, not the stdin payload.
func TestRunDeliversInputViaStdin(t *testing.T) {
	db := openTestDB(t)
	cfg := runFixtureCfg(t.TempDir())
	scriptDir := t.TempDir()
	// Read stdin verbatim, base64-encode, ship in a success return so we can
	// inspect the bytes the child actually received.
	script := writeScript(t, scriptDir, "echo_stdin.sh",
		`PAYLOAD=$(base64 -i - 2>/dev/null || base64)
printf '{"event":"success","return":{"b64":"%s"}}\n' "$PAYLOAD" >&3
exit 0`)

	id := ids.NewUUIDv7()
	m := storage.Mission{
		ID:               id,
		Kind:             storage.KindMission,
		Lane:             "normal",
		MissionName:      filepath.Base(script),
		Status:           storage.StatusQueued,
		Input:            []byte(`{"k":"v"}`),
		InputFingerprint: "fp",
		TimeCreatedMs:    time.Now().UnixMilli(),
	}
	if err := storage.InsertMission(context.Background(), db, &m); err != nil {
		t.Fatalf("insert mission: %v", err)
	}
	rt := storage.MissionRuntime{
		MissionID:           id,
		MissionDir:          filepath.Dir(script),
		CommandTemplate:     `["sh", "{mission_path}"]`,
		ValidateMissionFile: true,
	}
	if err := storage.InsertRuntime(context.Background(), db, &rt); err != nil {
		t.Fatalf("insert runtime: %v", err)
	}

	mLoaded, _ := storage.GetMission(context.Background(), db, id)
	if err := Run(context.Background(), cfg, db, mLoaded, make(chan ExternalKillReason, 1), func() {}); err != nil {
		t.Fatalf("Run: %v", err)
	}

	got := loadMission(t, db, id)
	if got.Outcome.String != "success" {
		t.Fatalf("outcome=%q, want success (return=%s)", got.Outcome.String, string(got.ReturnValue))
	}

	var ret struct {
		B64 string `json:"b64"`
	}
	if err := json.Unmarshal(got.ReturnValue, &ret); err != nil {
		t.Fatalf("unmarshal return: %v (raw=%s)", err, string(got.ReturnValue))
	}
	decoded, err := decodeBase64Trim(ret.B64)
	if err != nil {
		t.Fatalf("base64 decode: %v (b64=%q)", err, ret.B64)
	}

	var payload map[string]any
	if err := json.Unmarshal(decoded, &payload); err != nil {
		t.Fatalf("unmarshal stdin payload: %v (raw=%s)", err, string(decoded))
	}
	if payload["k"] != "v" {
		t.Errorf("k: got %v, want v (full payload=%s)", payload["k"], string(decoded))
	}
	// Stdin must NOT carry `files` — that metadata lives in env vars
	// (LETTS_IN_<role>{,__SHA256,__SIZE}).
	if _, ok := payload["files"]; ok {
		t.Errorf("stdin includes `files`: %s", string(decoded))
	}
}

func TestRunExitCodeZeroWithoutFd3(t *testing.T) {
	// Sanity: implicit-success path doesn't accidentally produce return values.
	db := openTestDB(t)
	cfg := runFixtureCfg(t.TempDir())
	scriptDir := t.TempDir()
	script := writeScript(t, scriptDir, "silent.sh", `exit 0`)
	id := runFixture(t, db, script, "normal", 0)
	m, _ := storage.GetMission(context.Background(), db, id)
	if err := Run(context.Background(), cfg, db, m, make(chan ExternalKillReason, 1), func() {}); err != nil {
		t.Fatalf("Run: %v", err)
	}
	got := loadMission(t, db, id)
	if got.ReturnValue != nil && string(got.ReturnValue) != "" {
		var v any
		if err := json.Unmarshal(got.ReturnValue, &v); err == nil && v != nil {
			t.Errorf("ReturnValue=%q, want null/empty", string(got.ReturnValue))
		}
	}
}
