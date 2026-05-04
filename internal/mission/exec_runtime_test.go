package mission

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"letts/internal/config"
	"letts/internal/ids"
	"letts/internal/storage"
)

// mkStaging writes a fake staging file under <dataDir>/staging/<shard>/<sid>
// and returns the corresponding *storage.StagingFile suitable for stagingMeta.
// Path is stored relative to dataDir to mirror real production rows.
func mkStaging(t *testing.T, dataDir, sid, content string) *storage.StagingFile {
	t.Helper()
	shard, err := ids.ShardPath(sid)
	if err != nil {
		t.Fatalf("shard %s: %v", sid, err)
	}
	dir := filepath.Join(dataDir, "staging", shard)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("mkdir staging: %v", err)
	}
	relPath := filepath.Join("staging", shard, sid)
	absPath := filepath.Join(dataDir, relPath)
	if err := os.WriteFile(absPath, []byte(content), 0o644); err != nil {
		t.Fatalf("write staging: %v", err)
	}
	return &storage.StagingFile{
		StagingID: sid,
		State:     storage.StagingComplete,
		Sha256:    fmt.Sprintf("sha-%s", sid[:8]),
		Size:      int64(len(content)),
		Path:      relPath,
	}
}

// insertExecMission inserts a kind=exec missions row with the given payload
// embedded as input. Inserts refs for script/in/__stdin__ as appropriate.
// Returns the mission id.
func insertExecMission(t *testing.T, db *sql.DB, lane string, payload execPayload, timeoutMs int64) string {
	t.Helper()
	id := ids.NewUUIDv7()
	raw, err := json.Marshal(&payload)
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}
	m := storage.Mission{
		ID:               id,
		Kind:             storage.KindExec,
		Lane:             lane,
		MissionName:      "exec",
		Status:           storage.StatusQueued,
		Input:            raw,
		InputFingerprint: "fp-" + id[:8],
		TimeCreatedMs:    time.Now().UnixMilli(),
	}
	if payload.GroupID != "" {
		m.GroupID = sql.NullString{String: payload.GroupID, Valid: true}
	}
	if timeoutMs > 0 {
		m.TimeoutMs = sql.NullInt64{Int64: timeoutMs, Valid: true}
	}
	if err := storage.InsertMission(context.Background(), db, &m); err != nil {
		t.Fatalf("insert mission: %v", err)
	}
	if payload.Script != nil {
		if err := storage.InsertRef(context.Background(), db, storage.StagingRef{
			MissionID: id, StagingID: payload.Script.StagingID, RefKind: storage.RefScript,
		}); err != nil {
			t.Fatalf("insert script ref: %v", err)
		}
	}
	for _, in := range payload.In {
		if err := storage.InsertRef(context.Background(), db, storage.StagingRef{
			MissionID: id, StagingID: in.StagingID, RefKind: storage.RefInput, Role: in.Key,
		}); err != nil {
			t.Fatalf("insert input ref %s: %v", in.Key, err)
		}
	}
	if payload.StdinStagingID != "" {
		if err := storage.InsertRef(context.Background(), db, storage.StagingRef{
			MissionID: id, StagingID: payload.StdinStagingID, RefKind: storage.RefInput, Role: "__stdin__",
		}); err != nil {
			t.Fatalf("insert stdin ref: %v", err)
		}
	}
	return id
}

func execRuntimeCfg(t *testing.T) *config.DugdaleConfig {
	t.Helper()
	return &config.DugdaleConfig{
		DataDir: t.TempDir(),
		Limits: config.LimitsConfig{
			MaxOutputBuffer:     1 << 20,
			MaxEventsBuffer:     1 << 20,
			MaxEventLineSize:    1 << 20,
			MaxReturnValueSize:  768 * 1024,
			MaxFailMessageSize:  64 * 1024,
			MaxFailDetailsSize:  256 * 1024,
			MaxOutputFileSize:   0,
			DefaultKillGrace:    150 * time.Millisecond,
			ReaderPostExitGrace: 300 * time.Millisecond,
		},
	}
}

// --- PrepareExecWorkdir ---

func TestPrepareExecWorkdir(t *testing.T) {
	cfg := execRuntimeCfg(t)
	scriptSid := ids.NewUUIDv7()
	in1Sid := ids.NewUUIDv7()
	in2Sid := ids.NewUUIDv7()
	stdinSid := ids.NewUUIDv7()

	stagingMeta := map[string]*storage.StagingFile{
		scriptSid: mkStaging(t, cfg.DataDir, scriptSid, "#!/bin/sh\necho hi\n"),
		in1Sid:    mkStaging(t, cfg.DataDir, in1Sid, "input one"),
		in2Sid:    mkStaging(t, cfg.DataDir, in2Sid, "input two contents"),
		stdinSid:  mkStaging(t, cfg.DataDir, stdinSid, "stdin payload"),
	}

	payload := execPayload{
		Lane:    "normal",
		Command: []string{"sh", "script/script"},
		Script:  &execPayloadScript{StagingID: scriptSid},
		In: []execPayloadIn{
			{Key: "alpha", StagingID: in1Sid},
			{Key: "beta", StagingID: in2Sid},
		},
		Out: []execPayloadOut{
			{Key: "result"},
			{Key: "log"},
		},
		Stdin:          "single",
		StdinStagingID: stdinSid,
		GroupID:        "group-xyz",
	}

	id := ids.NewUUIDv7()
	m := &storage.Mission{ID: id, Kind: storage.KindExec, Lane: payload.Lane, Input: mustJSON(t, payload)}
	refs := []storage.StagingRef{
		{MissionID: id, StagingID: scriptSid, RefKind: storage.RefScript},
		{MissionID: id, StagingID: in1Sid, RefKind: storage.RefInput, Role: "alpha"},
		{MissionID: id, StagingID: in2Sid, RefKind: storage.RefInput, Role: "beta"},
		{MissionID: id, StagingID: stdinSid, RefKind: storage.RefInput, Role: "__stdin__"},
	}

	workdir, env, stdinPath, err := PrepareExecWorkdir(cfg, m, stagingMeta, refs)
	if err != nil {
		t.Fatalf("PrepareExecWorkdir: %v", err)
	}

	// Subdirs exist.
	for _, sub := range []string{"in", "out", "tmp", "script"} {
		info, err := os.Stat(filepath.Join(workdir, sub))
		if err != nil || !info.IsDir() {
			t.Errorf("subdir %s missing: %v", sub, err)
		}
	}

	// Script file present and 0555.
	scriptDst := filepath.Join(workdir, "script", "script")
	info, err := os.Stat(scriptDst)
	if err != nil {
		t.Fatalf("stat script: %v", err)
	}
	if info.Mode().Perm() != 0o555 {
		t.Errorf("script mode=%o, want 0555", info.Mode().Perm())
	}

	// Input files present at workdir/in/<key>, mode 0444.
	for _, key := range []string{"alpha", "beta"} {
		info, err := os.Stat(filepath.Join(workdir, "in", key))
		if err != nil {
			t.Errorf("input %s missing: %v", key, err)
			continue
		}
		if info.Mode().Perm() != 0o444 {
			t.Errorf("input %s mode=%o, want 0444", key, info.Mode().Perm())
		}
	}

	// Stdin copy at tmp/.stdin, stdinPath matches, mode 0444.
	expectedStdin := filepath.Join(workdir, "tmp", ".stdin")
	if stdinPath != expectedStdin {
		t.Errorf("stdinPath=%q want %q", stdinPath, expectedStdin)
	}
	info, err = os.Stat(stdinPath)
	if err != nil {
		t.Fatalf("stat stdin copy: %v", err)
	}
	if info.Mode().Perm() != 0o444 {
		t.Errorf("stdin copy mode=%o, want 0444", info.Mode().Perm())
	}

	// Out files do NOT exist; LETTS_OUT_<key> still set.
	for _, key := range []string{"result", "log"} {
		if _, err := os.Stat(filepath.Join(workdir, "out", key)); !os.IsNotExist(err) {
			t.Errorf("out/%s should not exist yet, err=%v", key, err)
		}
	}

	envMap := envSliceToMap(env)
	wantKeys := []string{
		"LETTS_EXEC_ID", "LETTS_GROUP_ID", "LETTS_KIND", "LETTS_LANE",
		"LETTS_WORKDIR", "LETTS_TMPDIR", "LETTS_SCRIPT",
		"LETTS_IN_alpha", "LETTS_IN_alpha__SHA256", "LETTS_IN_alpha__SIZE",
		"LETTS_IN_beta", "LETTS_IN_beta__SHA256", "LETTS_IN_beta__SIZE",
		"LETTS_OUT_result", "LETTS_OUT_log",
		"PATH",
	}
	for _, k := range wantKeys {
		if _, ok := envMap[k]; !ok {
			t.Errorf("env missing %s; have %v", k, envMap)
		}
	}
	if envMap["LETTS_KIND"] != "exec" {
		t.Errorf("LETTS_KIND=%q, want exec", envMap["LETTS_KIND"])
	}
	if envMap["LETTS_EXEC_ID"] != id {
		t.Errorf("LETTS_EXEC_ID=%q want %q", envMap["LETTS_EXEC_ID"], id)
	}
	if envMap["LETTS_GROUP_ID"] != "group-xyz" {
		t.Errorf("LETTS_GROUP_ID=%q want group-xyz", envMap["LETTS_GROUP_ID"])
	}
	if envMap["LETTS_LANE"] != "normal" {
		t.Errorf("LETTS_LANE=%q want normal", envMap["LETTS_LANE"])
	}
	if envMap["LETTS_WORKDIR"] != workdir {
		t.Errorf("LETTS_WORKDIR=%q want %q", envMap["LETTS_WORKDIR"], workdir)
	}
	if envMap["LETTS_TMPDIR"] != filepath.Join(workdir, "tmp") {
		t.Errorf("LETTS_TMPDIR=%q", envMap["LETTS_TMPDIR"])
	}
	if envMap["LETTS_SCRIPT"] != scriptDst {
		t.Errorf("LETTS_SCRIPT=%q want %q", envMap["LETTS_SCRIPT"], scriptDst)
	}
	if envMap["PATH"] != "/usr/local/bin:/usr/bin:/bin" {
		t.Errorf("PATH=%q", envMap["PATH"])
	}
	// LETTS_OUT_<key> paths.
	if got, want := envMap["LETTS_OUT_result"], filepath.Join(workdir, "out", "result"); got != want {
		t.Errorf("LETTS_OUT_result=%q want %q", got, want)
	}
	if got, want := envMap["LETTS_OUT_log"], filepath.Join(workdir, "out", "log"); got != want {
		t.Errorf("LETTS_OUT_log=%q want %q", got, want)
	}
	// __SHA256 / __SIZE for each input.
	if envMap["LETTS_IN_alpha__SHA256"] != stagingMeta[in1Sid].Sha256 {
		t.Errorf("alpha sha256 mismatch")
	}
	if envMap["LETTS_IN_alpha__SIZE"] != fmt.Sprintf("%d", stagingMeta[in1Sid].Size) {
		t.Errorf("alpha size mismatch")
	}
	if envMap["LETTS_IN_beta__SIZE"] != fmt.Sprintf("%d", stagingMeta[in2Sid].Size) {
		t.Errorf("beta size mismatch")
	}
}

func TestPrepareExecWorkdirOmitsGroupIDWhenEmpty(t *testing.T) {
	cfg := execRuntimeCfg(t)
	payload := execPayload{
		Lane:    "normal",
		Command: []string{"true"},
	}
	id := ids.NewUUIDv7()
	m := &storage.Mission{ID: id, Kind: storage.KindExec, Lane: "normal", Input: mustJSON(t, payload)}

	_, env, _, err := PrepareExecWorkdir(cfg, m, map[string]*storage.StagingFile{}, nil)
	if err != nil {
		t.Fatalf("PrepareExecWorkdir: %v", err)
	}
	envMap := envSliceToMap(env)
	if _, ok := envMap["LETTS_GROUP_ID"]; ok {
		t.Errorf("LETTS_GROUP_ID should be absent when payload.GroupID empty")
	}
}

func TestPrepareExecWorkdirOmitsScriptEnvWhenAbsent(t *testing.T) {
	cfg := execRuntimeCfg(t)
	payload := execPayload{
		Lane:    "normal",
		Command: []string{"true"},
	}
	id := ids.NewUUIDv7()
	m := &storage.Mission{ID: id, Kind: storage.KindExec, Lane: "normal", Input: mustJSON(t, payload)}

	workdir, env, _, err := PrepareExecWorkdir(cfg, m, map[string]*storage.StagingFile{}, nil)
	if err != nil {
		t.Fatalf("PrepareExecWorkdir: %v", err)
	}
	envMap := envSliceToMap(env)
	if _, ok := envMap["LETTS_SCRIPT"]; ok {
		t.Errorf("LETTS_SCRIPT should be absent when payload has no script")
	}
	// script subdir is always created (mirror PrepareWorkdir's
	// "always-mkdir" stance so the kind-switch doesn't surprise callers).
	if info, err := os.Stat(filepath.Join(workdir, "script")); err != nil || !info.IsDir() {
		t.Errorf("script subdir not created: %v", err)
	}
}

// --- DeriveExecOutcome ---

func TestDeriveExecOutcome(t *testing.T) {
	cases := []struct {
		name     string
		exitCode int
		signal   syscall.Signal
		timedOut bool
		want     OutcomeResult
	}{
		{
			"timeout wins",
			-1, syscall.SIGKILL, true,
			OutcomeResult{Outcome: "timeout", FailReason: "timeout", ExitCode: -1},
		},
		{
			"signal without timeout",
			-1, syscall.SIGTERM, false,
			OutcomeResult{Outcome: "killed", FailReason: "signal", FailMessage: "terminated", ExitCode: -1, Signal: "terminated"},
		},
		{
			"zero exit is success",
			0, 0, false,
			OutcomeResult{Outcome: "success", ExitCode: 0},
		},
		{
			"nonzero exit fails",
			7, 0, false,
			OutcomeResult{Outcome: "failed", FailReason: "exit_nonzero", FailMessage: "exit code 7", ExitCode: 7},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := DeriveExecOutcome(tc.exitCode, tc.signal, tc.timedOut)
			// Signal.String() is platform-shaped; for the SIGTERM case verify
			// the field is non-empty rather than the exact text on this
			// platform (darwin/linux both produce a meaningful string).
			if tc.signal != 0 && !tc.timedOut {
				if got.Outcome != "killed" || got.FailReason != "signal" || got.Signal == "" || got.FailMessage == "" {
					t.Errorf("signal case mismatch: %+v", got)
				}
				return
			}
			if got.Outcome != tc.want.Outcome || got.FailReason != tc.want.FailReason ||
				got.ExitCode != tc.want.ExitCode {
				t.Errorf("got=%+v want=%+v", got, tc.want)
			}
			if tc.want.FailMessage != "" && got.FailMessage != tc.want.FailMessage {
				t.Errorf("FailMessage=%q want %q", got.FailMessage, tc.want.FailMessage)
			}
		})
	}
}

// When cmd.ProcessState is nil after Wait() (very rare —
// typically happens only on a misconfigured Cmd or a Go-runtime hiccup),
// the previous code leaked exit_code=-1 to clients without any signal
// that "we don't actually know the result". Map this defensively to a
// "crashed" outcome with a specific FailReason so dashboards and CLI
// retries can distinguish it from a real signaled `exit_code=-1`.
func TestExtractExecResultHandlesNilProcessState(t *testing.T) {
	exit, sig, ok := extractExecResult(nil)
	if ok {
		t.Error("ok must be false when ProcessState is nil")
	}
	if exit != -1 {
		t.Errorf("exit=%d want -1", exit)
	}
	if sig != 0 {
		t.Errorf("sig=%v want 0", sig)
	}
}

func TestExtractExecResultReadsNormalExit(t *testing.T) {
	// Spawn a quick process that exits 7 so we get a real ProcessState.
	cmd := exec.Command("sh", "-c", "exit 7")
	if err := cmd.Run(); err == nil {
		t.Fatal("expected non-nil err for exit-7 process")
	}
	exit, sig, ok := extractExecResult(cmd.ProcessState)
	if !ok {
		t.Fatal("ok must be true with real ProcessState")
	}
	if exit != 7 {
		t.Errorf("exit=%d want 7", exit)
	}
	if sig != 0 {
		t.Errorf("sig=%v want 0", sig)
	}
}

// --- spawnExec end-to-end ---

func TestSpawnExecSimpleEcho(t *testing.T) {
	db := openTestDB(t)
	cfg := execRuntimeCfg(t)
	id := insertExecMission(t, db, "normal", execPayload{
		Lane:    "normal",
		Command: []string{"/bin/echo", "hi"},
	}, 0)
	m, _ := storage.GetMission(context.Background(), db, id)
	if err := Run(context.Background(), cfg, db, m, make(chan ExternalKillReason, 1), func() {}); err != nil {
		t.Fatalf("Run: %v", err)
	}
	got := loadMission(t, db, id)
	if got.Status != storage.StatusDone || got.Outcome.String != "success" {
		t.Errorf("status=%q outcome=%q want done/success", got.Status, got.Outcome.String)
	}
	if got.ExitCode.Int64 != 0 {
		t.Errorf("ExitCode=%d want 0", got.ExitCode.Int64)
	}
	shard, _ := ids.ShardPath(id)
	stdout, err := os.ReadFile(filepath.Join(cfg.DataDir, "output", shard, id+"-stdout"))
	if err != nil {
		t.Fatalf("read stdout: %v", err)
	}
	if string(stdout) != "hi\n" {
		t.Errorf("stdout=%q want %q", string(stdout), "hi\n")
	}
}

func TestSpawnExecCollectsDeclaredOutput(t *testing.T) {
	db := openTestDB(t)
	cfg := execRuntimeCfg(t)
	id := insertExecMission(t, db, "normal", execPayload{
		Lane:    "normal",
		Command: []string{"sh", "-c", `echo data > "$LETTS_OUT_result"`},
		Out:     []execPayloadOut{{Key: "result"}},
	}, 0)
	m, _ := storage.GetMission(context.Background(), db, id)
	if err := Run(context.Background(), cfg, db, m, make(chan ExternalKillReason, 1), func() {}); err != nil {
		t.Fatalf("Run: %v", err)
	}
	got := loadMission(t, db, id)
	if got.Outcome.String != "success" {
		t.Fatalf("outcome=%q want success (fail=%q msg=%q)", got.Outcome.String, got.FailReason.String, got.FailMessage.String)
	}
	refs, err := storage.RefsByMission(context.Background(), db, id)
	if err != nil {
		t.Fatalf("refs: %v", err)
	}
	var outputRefs []storage.StagingRef
	for _, r := range refs {
		if r.RefKind == storage.RefOutput {
			outputRefs = append(outputRefs, r)
		}
	}
	if len(outputRefs) != 1 || outputRefs[0].Role != "result" {
		t.Errorf("output refs=%v", outputRefs)
	}
}

func TestSpawnExecMissingOutputFails(t *testing.T) {
	db := openTestDB(t)
	cfg := execRuntimeCfg(t)
	id := insertExecMission(t, db, "normal", execPayload{
		Lane:    "normal",
		Command: []string{"true"},
		Out:     []execPayloadOut{{Key: "expected"}},
	}, 0)
	m, _ := storage.GetMission(context.Background(), db, id)
	if err := Run(context.Background(), cfg, db, m, make(chan ExternalKillReason, 1), func() {}); err != nil {
		t.Fatalf("Run: %v", err)
	}
	got := loadMission(t, db, id)
	if got.Outcome.String != "failed" || got.FailReason.String != "missing_output" {
		t.Errorf("outcome=%q reason=%q want failed/missing_output", got.Outcome.String, got.FailReason.String)
	}
}

func TestSpawnExecOutputSymlinkRejected(t *testing.T) {
	db := openTestDB(t)
	cfg := execRuntimeCfg(t)
	id := insertExecMission(t, db, "normal", execPayload{
		Lane:    "normal",
		Command: []string{"sh", "-c", `ln -s /etc/passwd "$LETTS_OUT_leak"`},
		Out:     []execPayloadOut{{Key: "leak"}},
	}, 0)
	m, _ := storage.GetMission(context.Background(), db, id)
	if err := Run(context.Background(), cfg, db, m, make(chan ExternalKillReason, 1), func() {}); err != nil {
		t.Fatalf("Run: %v", err)
	}
	got := loadMission(t, db, id)
	if got.Outcome.String != "failed" || got.FailReason.String != "output_path_escape" {
		t.Errorf("outcome=%q reason=%q want failed/output_path_escape (msg=%q)", got.Outcome.String, got.FailReason.String, got.FailMessage.String)
	}
}

// TestSpawnExecNonzeroExitFails covers the exit_nonzero branch of
// DeriveExecOutcome through a full lifecycle.
func TestSpawnExecNonzeroExitFails(t *testing.T) {
	db := openTestDB(t)
	cfg := execRuntimeCfg(t)
	id := insertExecMission(t, db, "normal", execPayload{
		Lane:    "normal",
		Command: []string{"sh", "-c", "exit 7"},
	}, 0)
	m, _ := storage.GetMission(context.Background(), db, id)
	if err := Run(context.Background(), cfg, db, m, make(chan ExternalKillReason, 1), func() {}); err != nil {
		t.Fatalf("Run: %v", err)
	}
	got := loadMission(t, db, id)
	if got.Outcome.String != "failed" || got.FailReason.String != "exit_nonzero" {
		t.Errorf("outcome=%q reason=%q want failed/exit_nonzero", got.Outcome.String, got.FailReason.String)
	}
	if got.ExitCode.Int64 != 7 {
		t.Errorf("ExitCode=%d want 7", got.ExitCode.Int64)
	}
}

// TestSpawnExecReturnsPromptlyWhenDescendantHoldsStdio mirrors the mission
// waiter's prompt-return guarantee for the exec runtime: a command that
// backgrounds a child and exits leaves the stdout/stderr pipe write-ends
// open in the survivor, and Wait must not block on them past
// reader_post_exit_grace. The bounded wait (exec.ErrWaitDelay) must not be
// misclassified either — ProcessState stays valid, so exit 0 still derives
// outcome=success.
func TestSpawnExecReturnsPromptlyWhenDescendantHoldsStdio(t *testing.T) {
	db := openTestDB(t)
	cfg := execRuntimeCfg(t)
	// No redirections on the backgrounded sleep — it must inherit the pipes.
	id := insertExecMission(t, db, "normal", execPayload{
		Lane:    "normal",
		Command: []string{"sh", "-c", "(sleep 15 &); exit 0"},
	}, 0)
	m, _ := storage.GetMission(context.Background(), db, id)

	start := time.Now()
	if err := Run(context.Background(), cfg, db, m, make(chan ExternalKillReason, 1), func() {}); err != nil {
		t.Fatalf("Run: %v", err)
	}
	elapsed := time.Since(start)
	if elapsed > 10*time.Second {
		t.Errorf("Run took %v; pipes held by a surviving descendant must not block reaping", elapsed)
	}

	got := loadMission(t, db, id)
	if got.Status != storage.StatusDone || got.Outcome.String != "success" {
		t.Errorf("status=%q outcome=%q (reason=%q msg=%q), want done/success",
			got.Status, got.Outcome.String, got.FailReason.String, got.FailMessage.String)
	}
	if got.ExitCode.Int64 != 0 {
		t.Errorf("ExitCode=%d, want 0", got.ExitCode.Int64)
	}
}

// TestSpawnExecKillByAPIWins asserts the kill supervisor overrides natural
// exit derivation: a SIGTERM via killCh produces outcome=killed even when the
// child happened to exit cleanly under the signal.
func TestSpawnExecKillByAPIWins(t *testing.T) {
	db := openTestDB(t)
	cfg := execRuntimeCfg(t)
	id := insertExecMission(t, db, "normal", execPayload{
		Lane:    "normal",
		Command: []string{"sh", "-c", "sleep 30"},
	}, 0)
	m, _ := storage.GetMission(context.Background(), db, id)

	killCh := make(chan ExternalKillReason, 1)
	done := make(chan error, 1)
	go func() { done <- Run(context.Background(), cfg, db, m, killCh, func() {}) }()

	time.Sleep(50 * time.Millisecond)
	killCh <- KillByAPI

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after kill")
	}
	got := loadMission(t, db, id)
	if got.Outcome.String != "killed" || got.FailReason.String != "killed_by_api" {
		t.Errorf("outcome=%q reason=%q want killed/killed_by_api", got.Outcome.String, got.FailReason.String)
	}
}

func mustJSON(t *testing.T, v any) []byte {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return b
}
