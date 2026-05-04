package mission

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"sync"
	"syscall"
	"time"

	"letts/internal/config"
	"letts/internal/eventfile"
	"letts/internal/ids"
	"letts/internal/outputfile"
	"letts/internal/storage"
)

// execPayload mirrors the JSON shape persisted to missions.input for exec
// missions (handlers.ExecRequest). Decoded locally so internal/mission does
// not import internal/server/handlers (which would form a cycle).
type execPayload struct {
	Lane           string             `json:"lane"`
	Command        []string           `json:"command"`
	Script         *execPayloadScript `json:"script,omitempty"`
	In             []execPayloadIn    `json:"in,omitempty"`
	Out            []execPayloadOut   `json:"out,omitempty"`
	Stdin          string             `json:"stdin,omitempty"`
	StdinStagingID string             `json:"stdin_staging_id,omitempty"`
	Timeout        string             `json:"timeout,omitempty"`
	GroupID        string             `json:"group_id,omitempty"`
	DisplayName    string             `json:"display_name,omitempty"`
}

type execPayloadScript struct {
	StagingID string `json:"staging_id"`
}

type execPayloadIn struct {
	Key       string `json:"key"`
	StagingID string `json:"staging_id"`
}

type execPayloadOut struct {
	Key string `json:"key"`
}

// PrepareExecWorkdir creates work/<exec_id>/{in,out,tmp,script}, materializes
// staging files referenced by refs into the appropriate subdir, and returns
// the workdir path, the clean env, and the stdin path (when an
// __stdin__ input ref exists).
//
// Staging file resolution uses stagingMeta[StagingID].Path which is relative
// to cfg.DataDir.
func PrepareExecWorkdir(
	cfg *config.DugdaleConfig,
	m *storage.Mission,
	stagingMeta map[string]*storage.StagingFile,
	refs []storage.StagingRef,
) (workdir string, env []string, stdinPath string, err error) {
	workdir = filepath.Join(cfg.DataDir, "work", m.ID)

	// Clean any stale workdir from a previous run.
	if err = os.RemoveAll(workdir); err != nil {
		return "", nil, "", fmt.Errorf("remove stale workdir: %w", err)
	}
	for _, sub := range []string{"", "in", "out", "tmp", "script"} {
		if err = os.MkdirAll(filepath.Join(workdir, sub), 0o755); err != nil {
			return "", nil, "", fmt.Errorf("mkdir %s: %w", sub, err)
		}
	}

	// Materialize staging files per ref.
	for _, r := range refs {
		st, ok := stagingMeta[r.StagingID]
		if !ok || st == nil {
			return "", nil, "", fmt.Errorf("staging meta missing for %s", r.StagingID)
		}
		src := filepath.Join(cfg.DataDir, st.Path)
		switch {
		case r.RefKind == storage.RefScript:
			dst := filepath.Join(workdir, "script", "script")
			if err = copyFile(src, dst); err != nil {
				return "", nil, "", fmt.Errorf("materialize script: %w", err)
			}
			if err = os.Chmod(dst, 0o555); err != nil {
				return "", nil, "", fmt.Errorf("chmod script: %w", err)
			}
		case r.RefKind == storage.RefInput && r.Role == "__stdin__":
			dst := filepath.Join(workdir, "tmp", ".stdin")
			if err = copyFile(src, dst); err != nil {
				return "", nil, "", fmt.Errorf("materialize stdin: %w", err)
			}
			if err = os.Chmod(dst, 0o444); err != nil {
				return "", nil, "", fmt.Errorf("chmod stdin: %w", err)
			}
			stdinPath = dst
		case r.RefKind == storage.RefInput:
			dst := filepath.Join(workdir, "in", r.Role)
			if err = copyFile(src, dst); err != nil {
				return "", nil, "", fmt.Errorf("materialize input %s: %w", r.Role, err)
			}
			if err = os.Chmod(dst, 0o444); err != nil {
				return "", nil, "", fmt.Errorf("chmod input %s: %w", r.Role, err)
			}
		}
	}

	// Decode payload to build env. Failure here means the row's input is
	// corrupt — surface as spawn_failed.
	var p execPayload
	if len(m.Input) > 0 {
		if err = json.Unmarshal(m.Input, &p); err != nil {
			return "", nil, "", fmt.Errorf("decode exec payload: %w", err)
		}
	}

	env = BuildExecEnv(workdir, m, &p, stagingMeta)
	return workdir, env, stdinPath, nil
}

// BuildExecEnv composes the clean env slice for an exec mission: no parent
// inherit, no HOME/TZ, only the LETTS_* vars and a POSIX PATH.
//
// LETTS_GROUP_ID is omitted when payload.GroupID == "" (group_id is optional)
// and LETTS_SCRIPT is omitted when payload has no script ref.
func BuildExecEnv(
	workdir string,
	m *storage.Mission,
	payload *execPayload,
	stagingMeta map[string]*storage.StagingFile,
) []string {
	env := []string{
		"LETTS_EXEC_ID=" + m.ID,
		"LETTS_KIND=exec",
		"LETTS_LANE=" + m.Lane,
		"LETTS_WORKDIR=" + workdir,
		"LETTS_TMPDIR=" + filepath.Join(workdir, "tmp"),
	}
	if payload.GroupID != "" {
		env = append(env, "LETTS_GROUP_ID="+payload.GroupID)
	}
	if payload.Script != nil {
		env = append(env, "LETTS_SCRIPT="+filepath.Join(workdir, "script", "script"))
	}
	// Per-input vars: LETTS_IN_<key>=path, __SHA256, __SIZE.
	for _, in := range payload.In {
		path := filepath.Join(workdir, "in", in.Key)
		env = append(env, "LETTS_IN_"+in.Key+"="+path)
		if sm, ok := stagingMeta[in.StagingID]; ok && sm != nil {
			env = append(env,
				"LETTS_IN_"+in.Key+"__SHA256="+sm.Sha256,
				fmt.Sprintf("LETTS_IN_%s__SIZE=%d", in.Key, sm.Size),
			)
		}
	}
	// Per-output placeholder LETTS_OUT_<key>=path; the file does not exist yet.
	for _, o := range payload.Out {
		env = append(env, "LETTS_OUT_"+o.Key+"="+filepath.Join(workdir, "out", o.Key))
	}
	env = append(env, "PATH=/usr/local/bin:/usr/bin:/bin")
	return env
}

// extractExecResult pulls the (exitCode, signal) pair out of a finished
// cmd's ProcessState. Returns ok=false when ProcessState is nil — that
// usually means cmd.Wait() completed without populating state (a rare
// Go-runtime / mis-configured-Cmd path); callers must NOT treat the
// returned exitCode=-1 as a legitimate exit because we genuinely don't
// know what happened to the process.
func extractExecResult(ps *os.ProcessState) (exitCode int, sig syscall.Signal, ok bool) {
	if ps == nil {
		return -1, 0, false
	}
	exitCode = ps.ExitCode()
	if ws, wsOK := ps.Sys().(syscall.WaitStatus); wsOK && ws.Signaled() {
		sig = ws.Signal()
	}
	return exitCode, sig, true
}

// DeriveExecOutcome maps the (exitCode, signal, timedOut) triple from an exec
// process to an OutcomeResult. timedOut takes precedence over
// signal which takes precedence over non-zero exit; success requires exit=0
// with no signal and no timeout.
func DeriveExecOutcome(exitCode int, signal syscall.Signal, timedOut bool) OutcomeResult {
	switch {
	case timedOut:
		return OutcomeResult{Outcome: "timeout", FailReason: "timeout", ExitCode: exitCode}
	case signal != 0:
		return OutcomeResult{
			Outcome:     "killed",
			FailReason:  "signal",
			FailMessage: signal.String(), // human-readable long form for the message
			ExitCode:    exitCode,
			Signal:      sigName(signal), // stable symbolic name for the field
		}
	case exitCode == 0:
		return OutcomeResult{Outcome: "success", ExitCode: 0}
	default:
		return OutcomeResult{
			Outcome:     "failed",
			FailReason:  "exit_nonzero",
			FailMessage: fmt.Sprintf("exit code %d", exitCode),
			ExitCode:    exitCode,
		}
	}
}

// runExec executes one exec mission lifecycle: payload and ref resolution,
// workdir prep, spawn (no fd3), wait with kill supervisor, outcome derivation,
// declared-output collection, finalize.
func runExec(ctx context.Context, cfg *config.DugdaleConfig, db *sql.DB, m *storage.Mission, killCh <-chan ExternalKillReason) error {
	// Decode payload up front; we need it for declared-output collection
	// regardless of whether the spawn itself succeeds (we still want a
	// deterministic outcome even on a corrupt row).
	var payload execPayload
	if len(m.Input) > 0 {
		if err := json.Unmarshal(m.Input, &payload); err != nil {
			return finalizeCrashed(ctx, cfg, db, m, "spawn_failed", "decode exec payload: "+err.Error())
		}
	}
	if len(payload.Command) == 0 {
		return finalizeCrashed(ctx, cfg, db, m, "spawn_failed", "empty command")
	}

	// Load refs and staging meta needed by PrepareExecWorkdir and env build.
	refs, err := storage.RefsByMission(ctx, db, m.ID)
	if err != nil {
		return finalizeCrashed(ctx, cfg, db, m, "spawn_failed", "load refs: "+err.Error())
	}
	stagingMeta := make(map[string]*storage.StagingFile, len(refs))
	for _, r := range refs {
		if _, seen := stagingMeta[r.StagingID]; seen {
			continue
		}
		st, gErr := storage.GetStaging(ctx, db, r.StagingID)
		if gErr != nil {
			return finalizeCrashed(ctx, cfg, db, m, "spawn_failed", "get staging "+r.StagingID+": "+gErr.Error())
		}
		stagingMeta[r.StagingID] = st
	}

	workdir, env, stdinPath, err := PrepareExecWorkdir(cfg, m, stagingMeta, refs)
	if err != nil {
		return finalizeCrashed(ctx, cfg, db, m, crashedReasonFromOS(err, "input_materialization_failed"), err.Error())
	}

	// Output files: same layout as mission spawns.
	shard, _ := ids.ShardPath(m.ID)
	outDir := filepath.Join(cfg.DataDir, "output", shard)
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		return finalizeCrashed(ctx, cfg, db, m, crashedReasonFromOS(err, "spawn_failed"), "mkdir output: "+err.Error())
	}
	stdoutF, err := os.OpenFile(filepath.Join(outDir, m.ID+"-stdout"), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return finalizeCrashed(ctx, cfg, db, m, crashedReasonFromOS(err, "spawn_failed"), "open stdout: "+err.Error())
	}
	defer func() { _ = stdoutF.Close() }()
	stderrF, err := os.OpenFile(filepath.Join(outDir, m.ID+"-stderr"), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return finalizeCrashed(ctx, cfg, db, m, crashedReasonFromOS(err, "spawn_failed"), "open stderr: "+err.Error())
	}
	defer func() { _ = stderrF.Close() }()
	combF, err := os.OpenFile(filepath.Join(outDir, m.ID+"-combined"), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return finalizeCrashed(ctx, cfg, db, m, crashedReasonFromOS(err, "spawn_failed"), "open combined: "+err.Error())
	}
	defer func() { _ = combF.Close() }()
	tw := outputfile.New(cfg.Limits.MaxOutputBuffer, stdoutF, stderrF, combF)

	// Stdin: prefer staged file; fall back to /dev/null so the process gets
	// a real fd 0 instead of inheriting dugdale's.
	openedStdinPath := stdinPath
	if openedStdinPath == "" {
		openedStdinPath = os.DevNull
	}
	stdinF, err := os.Open(openedStdinPath)
	if err != nil {
		return finalizeCrashed(ctx, cfg, db, m, "spawn_failed", "open stdin: "+err.Error())
	}
	defer func() { _ = stdinF.Close() }()

	cmd := exec.Command(payload.Command[0], payload.Command[1:]...)
	cmd.Env = env
	cmd.Dir = workdir
	cmd.Stdin = stdinF
	cmd.Stdout = tw.Stdout()
	cmd.Stderr = tw.Stderr()
	cmd.ExtraFiles = nil // explicit: no fd3
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	// Invariant: stdout/stderr write-ends inherited by surviving descendants
	// must not block reaping the leader. Stdout/Stderr are io.Writers, so
	// os/exec pipes them internally and Wait blocks until EOF — a command
	// that backgrounds a child would hold the lane slot until that child
	// exits. Bound the post-exit pipe drain by the same grace that bounds
	// the mission path's fd3 shutdown (exec has no fd3, so this is the only
	// pipe budget needed). Wait then returns exec.ErrWaitDelay on a clean
	// exit with ProcessState intact; assigned before Start so every Wait
	// call site is covered without races.
	cmd.WaitDelay = cfg.Limits.ReaderPostExitGrace

	if err := cmd.Start(); err != nil {
		return finalizeCrashed(ctx, cfg, db, m, "spawn_failed", err.Error())
	}
	// Parent no longer needs the stdin fd; the child has its own dup.
	_ = stdinF.Close()

	pid := int64(cmd.Process.Pid)
	pgid := pid
	procStart := readProcStarttime(int(pid))

	// Open events writer for the running and done events. Recreate if dispatch
	// somehow lost it so the on-disk transcript is always present.
	ew, err := eventfile.Open(outDir, m.ID)
	if err != nil {
		ew, err = eventfile.Create(outDir, m.ID)
		if err != nil {
			// Never kill before cmd.Wait is in-flight (avoids the
			// signal-then-wait race where the runtime hasn't registered the
			// wait state yet). Start Wait in a goroutine, then signal.
			waitErr := make(chan error, 1)
			go func() { waitErr <- cmd.Wait() }()
			killProcessGroup(int(pgid), syscall.SIGKILL)
			<-waitErr
			return finalizeCrashed(ctx, cfg, db, m, "spawn_failed", "open events: "+err.Error())
		}
	}
	ew.SetLimits(eventfile.Limits{
		MaxEventsBuffer:  cfg.Limits.MaxEventsBuffer,
		MaxEventLineSize: cfg.Limits.MaxEventLineSize,
	})

	nowMs := time.Now().UnixMilli()
	if _, err := ew.Append(eventfile.KindRunning, map[string]any{
		"time":         nowMs,
		"pid":          pid,
		"time_started": nowMs,
	}, false); err != nil {
		slog.Default().Warn("append exec running event failed", "exec_id", m.ID, "err", err)
	}

	// Same shape as the mission path: fill OS pid and align
	// time_started with the post-spawn nowMs.
	if err := storage.WithWriter(ctx, db, func(c *sql.Conn) error {
		return storage.UpdateRunningPidAndTimeStarted(ctx, c, m.ID, pid, pgid, procStart, nowMs)
	}); err != nil && err != storage.ErrNotFound {
		slog.Default().Warn("UpdateRunningPidAndTimeStarted failed (exec)", "exec_id", m.ID, "err", err)
	}

	// Kill supervisor: timeout, killCh, ctx cancel. First call wins.
	var killMu sync.Mutex
	var killReason ExternalKillReason
	processExited := make(chan struct{})

	killProcess := func(r ExternalKillReason) {
		killMu.Lock()
		if killReason != "" {
			killMu.Unlock()
			return
		}
		// Refuse to signal pgid after the process has been
		// reaped (race with close(processExited) after cmd.Wait()).
		// See waiter.go for the same guard and rationale.
		select {
		case <-processExited:
			killMu.Unlock()
			return
		default:
		}
		killReason = r
		killMu.Unlock()
		select {
		case <-processExited:
			return
		default:
		}
		killProcessGroup(int(pgid), syscall.SIGTERM)
		select {
		case <-processExited:
			return
		case <-time.After(cfg.Limits.DefaultKillGrace):
			select {
			case <-processExited:
				return
			default:
			}
			killProcessGroup(int(pgid), syscall.SIGKILL)
		}
	}

	var killWg sync.WaitGroup
	if m.TimeoutMs.Valid && m.TimeoutMs.Int64 > 0 {
		killWg.Add(1)
		go func() {
			defer killWg.Done()
			select {
			case <-time.After(time.Duration(m.TimeoutMs.Int64) * time.Millisecond):
				killProcess(KillTimeout)
			case <-processExited:
			}
		}()
	}
	killWg.Add(1)
	go func() {
		defer killWg.Done()
		select {
		case reason, ok := <-killCh:
			if ok && reason != "" {
				killProcess(reason)
			}
		case <-processExited:
		}
	}()
	killWg.Add(1)
	go func() {
		defer killWg.Done()
		select {
		case <-ctx.Done():
			killProcess(KillDugdaleShutdown)
		case <-processExited:
		}
	}()

	// Wait's error is deliberately ignored: outcome derivation reads only
	// ProcessState. That covers exec.ErrWaitDelay too — the WaitDelay
	// watchdog closing the pipes after a clean exit must not be classified
	// as a wait failure or lose the exit code; ProcessState is populated
	// before Wait returns it.
	_ = cmd.Wait()
	close(processExited)
	killWg.Wait()
	_ = ew.Close()

	exitCode, sig, stateOK := extractExecResult(cmd.ProcessState)

	killMu.Lock()
	finalKill := killReason
	killMu.Unlock()

	// External kill takes priority over the natural exit derivation so that
	// timeout/force_delete/lane_removed/dugdale_shutdown/killed_by_api all
	// surface the operator-visible reason that initiated the kill.
	//
	// sig may be 0 if the child exited cleanly between the kill signal
	// dispatch and the wait return — empty signal string in that case,
	// matching the mission path's `if ws.Signaled() { ... }` guard.
	sigStr := ""
	if sig != 0 {
		sigStr = sigName(sig)
	}
	var o OutcomeResult
	switch {
	case finalKill == KillTimeout:
		o = OutcomeResult{Outcome: "timeout", FailReason: "timeout", ExitCode: exitCode, Signal: sigStr}
	case finalKill == KillForceDelete:
		o = OutcomeResult{Outcome: "killed", FailReason: "force_delete", ExitCode: exitCode, Signal: sigStr}
	case finalKill == KillLaneRemoved:
		o = OutcomeResult{Outcome: "killed", FailReason: "lane_removed", ExitCode: exitCode, Signal: sigStr}
	case finalKill == KillDugdaleShutdown:
		o = OutcomeResult{Outcome: "killed", FailReason: "dugdale_shutdown", ExitCode: exitCode, Signal: sigStr}
	case finalKill == KillByAPI:
		o = OutcomeResult{Outcome: "killed", FailReason: "killed_by_api", ExitCode: exitCode, Signal: sigStr}
	case !stateOK:
		// cmd.ProcessState was nil after Wait() — a Go-runtime
		// edge with no reliable exit info. Mark crashed with a specific
		// FailReason so the client sees more than the sentinel exit=-1.
		slog.Default().Warn("exec wait completed without ProcessState",
			"mission_id", m.ID, "kind", m.Kind, "lane", m.Lane)
		o = OutcomeResult{Outcome: "crashed", FailReason: "no_process_state",
			FailMessage: "wait() returned without populating ProcessState; exit info unknown",
			ExitCode:    -1}
	default:
		o = DeriveExecOutcome(exitCode, sig, false)
	}

	// Collect declared outputs on success only. Declared keys
	// come from the request payload (out[]), NOT from mission_staging_refs —
	// output refs land only after Finalize commits.
	var collected []CollectedOutput
	if o.Outcome == "success" && len(payload.Out) > 0 {
		keys := make([]string, 0, len(payload.Out))
		for _, e := range payload.Out {
			keys = append(keys, e.Key)
		}
		sort.Strings(keys)
		col, cerr := CollectOutputs(workdir, cfg.DataDir, keys, cfg.Limits.MaxOutputFileSize, missionCollectQuota(cfg))
		if cerr != nil {
			o.Outcome = "failed"
			o.FailReason = mapCollectErrorToReason(cerr)
			o.FailMessage = cerr.Error()
			o.DropReturn = true
			o.Return = nil
		} else {
			collected = col
		}
	}

	// Detach from ctx so a shutdown-driven kill can still durably finalize.
	return Finalize(context.WithoutCancel(ctx), db, FinalizeInputs{
		MissionID:     m.ID,
		Kind:          string(m.Kind),
		Lane:          m.Lane,
		Outcome:       o,
		Outputs:       collected,
		TimeStartedMs: nowMs,
		Cfg: FinalizeConfig{
			DataDir:        cfg.DataDir,
			MaxReturnValue: cfg.Limits.MaxReturnValueSize,
			MaxFailMessage: cfg.Limits.MaxFailMessageSize,
			MaxFailDetails: cfg.Limits.MaxFailDetailsSize,
			// Exec must set the TTL policy too, else output staging
			// keeps the 24h sentinel instead of exec_success_ttl/exec_failed_ttl.
			TTL: TTLPolicyFromConfig(cfg),
		},
	})
}
