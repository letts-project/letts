package mission

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"letts/internal/config"
	"letts/internal/eventfile"
	"letts/internal/ids"
	"letts/internal/outputfile"
	"letts/internal/storage"
)

// Run executes one mission lifecycle from spawn through finalize.
//
// release is the lane runner's concurrency-slot release callback; it's
// invoked exactly once before Run returns regardless of outcome.
//
// killCh receives an external kill reason (force_delete, lane_removed,
// killed_by_api). Both ctx cancellation and a sent killCh value cause
// graceful SIGTERM → grace → SIGKILL of the mission process group.
func Run(ctx context.Context, cfg *config.DugdaleConfig, db *sql.DB, m *storage.Mission, killCh <-chan ExternalKillReason, release func()) error {
	defer release()

	// Exec missions take a completely different runtime path: no fd3, no
	// mission_runtime row, env per the exec env contract, declared-output
	// collection. runExec handles its own finalize.
	if m.Kind == storage.KindExec {
		return runExec(ctx, cfg, db, m, killCh)
	}

	rt, err := storage.GetRuntime(ctx, db, m.ID)
	if err != nil {
		return finalizeCrashed(ctx, cfg, db, m, "spawn_failed", "get runtime: "+err.Error())
	}
	argv, err := ResolveCommand(rt, m.MissionName)
	if err != nil {
		// ResolveCommand wraps ErrMissionNotFound / ErrMissionNotInDir for
		// the two file-resolution failures called out by name.
		switch {
		case errors.Is(err, ErrMissionNotFound):
			return finalizeCrashed(ctx, cfg, db, m, "mission_not_found", err.Error())
		case errors.Is(err, ErrMissionNotInDir):
			return finalizeCrashed(ctx, cfg, db, m, "mission_not_in_dir", err.Error())
		}
		return finalizeCrashed(ctx, cfg, db, m, "spawn_failed", err.Error())
	}
	inputs, err := LoadInputs(ctx, db, cfg.DataDir, m.ID)
	if err != nil {
		return finalizeCrashed(ctx, cfg, db, m, "input_materialization_failed", err.Error())
	}
	workdir, err := PrepareWorkdir(cfg.DataDir, m.ID, inputs)
	if err != nil {
		return finalizeCrashed(ctx, cfg, db, m, crashedReasonFromOS(err, "input_materialization_failed"), err.Error())
	}

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
	var oomFlag atomic.Bool
	stderrSink := NewOOMDetector(tw.Stderr(), &oomFlag)

	envInputs := make([]EnvInputs, 0, len(inputs))
	for _, in := range inputs {
		envInputs = append(envInputs, EnvInputs{
			Role: in.Role, Path: filepath.Join(workdir, "in", in.Role),
			Sha256: in.Sha256, Size: in.Size,
		})
	}
	dugdaleHome, _ := os.UserHomeDir()
	env, err := BuildEnv(dugdaleHome, cfg.MissionEnv, envInputs, BaseVars{
		MissionID: m.ID, Kind: string(m.Kind), Lane: m.Lane, Workdir: workdir,
	}, os.LookupEnv)
	if err != nil {
		return finalizeCrashed(ctx, cfg, db, m, "spawn_failed", "env: "+err.Error())
	}

	// Write the user input JSON to <workdir>/input.json and pass its
	// path to Spawn so the mission process reads from fd 0. Stdin
	// carries the user payload only; file metadata is delivered via env
	// vars LETTS_IN_<role>{,__SHA256,__SIZE}.
	stdinPath, err := writeStdinEnvelope(workdir, m.Input)
	if err != nil {
		return finalizeCrashed(ctx, cfg, db, m, crashedReasonFromOS(err, "spawn_failed"), "stdin envelope: "+err.Error())
	}

	// ReaderPostExitGrace doubles as cmd.WaitDelay: the same post-exit budget
	// that bounds our fd3 reader shutdown bounds os/exec's stdout/stderr
	// pipe drain, so a backgrounded descendant can't wedge the lane slot.
	res, err := Spawn(argv, env, workdir, tw.Stdout(), stderrSink, stdinPath, cfg.Limits.ReaderPostExitGrace)
	if err != nil {
		return finalizeCrashed(ctx, cfg, db, m, "spawn_failed", err.Error())
	}

	pid := int64(res.Cmd.Process.Pid)
	pgid := pid
	procStart := readProcStarttime(int(pid))

	// Open events writer (created during dispatch; if missing, recreate so
	// crash-recovery still has a per-mission file to attach to).
	ew, err := eventfile.Open(outDir, m.ID)
	if err != nil {
		ew, err = eventfile.Create(outDir, m.ID)
		if err != nil {
			// Killing the process group before cmd.Wait is in-flight is
			// forbidden: a Signal-then-Wait sequence can race the
			// runtime's wait-state registration. Start Wait in a goroutine
			// first, then signal.
			waitErr := make(chan error, 1)
			go func() { waitErr <- res.Cmd.Wait() }()
			killProcessGroup(int(pgid), syscall.SIGKILL)
			<-waitErr
			_ = res.Fd3Reader.Close()
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
		slog.Default().Warn("append running event failed", "mission_id", m.ID, "err", err)
	}
	// Structured INFO line for log-only observers (separate from the
	// events file consumers).
	slog.Default().Info("mission", "phase", "started",
		"mission_id", m.ID, "kind", string(m.Kind), "lane", m.Lane, "pid", pid)

	// Reader/writer goroutines start immediately so the fd3 pipe never blocks.
	// Channel capacity sized from cfg.Limits.ProgressBufferSize ("256 KiB
	// default"); each ProgressEvent is small (timestamp, value, and a short
	// message), so divide by ~256 bytes / event to derive slots. Floor at 64
	// so a misconfigured 0 still gets a usable buffer.
	progressBufSlots := int(cfg.Limits.ProgressBufferSize / 256)
	if progressBufSlots < 64 {
		progressBufSlots = 64
	}
	progressCh := make(chan ProgressEvent, progressBufSlots)
	state := &Fd3State{}
	readerDone := make(chan struct{})
	writerDone := make(chan struct{})
	var droppedProgress int64

	runCtx, runCancel := context.WithCancel(ctx)
	defer runCancel()

	go func() {
		ReadFd3(runCtx, res.Fd3Reader, Fd3Limits{
			MaxEventLineSize:     cfg.Limits.MaxEventLineSize,
			MaxOutputFilesPerMsn: cfg.Limits.MaxOutputFilesPerMsn,
		}, progressCh, state)
		close(readerDone)
	}()
	go RunFd3Writer(runCtx, progressCh, ew, cfg.Limits.MaxProgressRate, &droppedProgress, writerDone)

	// Lane runner already transitioned the row to status='running' with
	// pid=0 atomically with PickQueuedForLane. Fill in the real OS pid and
	// align time_started with the post-spawn nowMs that the done event
	// will reference. ErrNotFound is fine: external kill or repair flipped
	// the row out from under us; that owner controls the outcome and we
	// shouldn't overwrite.
	if err := storage.WithWriter(ctx, db, func(c *sql.Conn) error {
		return storage.UpdateRunningPidAndTimeStarted(ctx, c, m.ID, pid, pgid, procStart, nowMs)
	}); err != nil && err != storage.ErrNotFound {
		slog.Default().Warn("UpdateRunningPidAndTimeStarted failed", "mission_id", m.ID, "err", err)
	}

	// Kill supervisor: timeout, killCh, ctx cancellation. First call wins;
	// killProcess waits for grace before SIGKILL.
	var killMu sync.Mutex
	var killReason ExternalKillReason
	processExited := make(chan struct{})

	killProcess := func(r ExternalKillReason) {
		killMu.Lock()
		if killReason != "" {
			killMu.Unlock()
			return
		}
		// Refuse to signal a pgid whose leader (pid) the kernel may have
		// already reaped via cmd.Wait(). Without this guard, a kill
		// goroutine racing close(processExited) can fire kill on a recycled
		// pid/pgid — a known pitfall: once the group leader is reaped, the
		// kernel may reuse its pid/pgid.
		select {
		case <-processExited:
			killMu.Unlock()
			return
		default:
		}
		killReason = r
		killMu.Unlock()
		// Second check immediately before the syscall — narrow window
		// but cheaper than the alternative (pidfd). Race with kernel
		// reap still possible; baseline cleanup is best-effort —
		// process-group kill is not a sandbox.
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
			// Final check before SIGKILL.
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

	// Wait's error is deliberately ignored: a non-zero exit surfaces via
	// ProcessState, and exec.ErrWaitDelay (the WaitDelay watchdog closed the
	// stdout/stderr pipes a grace period after a clean exit because a
	// descendant still held them) is expected — ProcessState remains valid
	// and outcome derivation below reads only that.
	_ = res.Cmd.Wait()
	close(processExited)
	killWg.Wait()

	// Bounded reader shutdown: process has exited, any background descendants
	// holding fd 3 won't keep us indefinitely.
	select {
	case <-readerDone:
	case <-time.After(cfg.Limits.ReaderPostExitGrace):
		_ = res.Fd3Reader.Close()
		<-readerDone
	}
	_ = res.Fd3Reader.Close()
	<-writerDone
	_ = ew.Close()

	killMu.Lock()
	finalKill := killReason
	killMu.Unlock()

	exitCode, sig, stateOK := extractExecResult(res.Cmd.ProcessState)
	signalName := ""
	if sig != 0 {
		signalName = sigName(sig)
	}

	var o OutcomeResult
	if !stateOK && finalKill == "" {
		// cmd.Wait returned without populating ProcessState — symmetric
		// with the exec path's fix. exit=-1 is sentinel, not real.
		o = OutcomeResult{
			Outcome:     "crashed",
			FailReason:  "no_process_state",
			FailMessage: "cmd.Wait returned without ProcessState",
			ExitCode:    -1,
		}
		slog.Default().Warn("mission: cmd.Wait returned nil ProcessState",
			"mission_id", m.ID)
	} else {
		o = Compute(OutcomeInputs{
			ExternalKill:  finalKill,
			OOMDetected:   oomFlag.Load(),
			ExitCode:      exitCode,
			Signal:        signalName,
			Fd3Final:      state.Final,
			Fd3Violations: state.Violations,
		})
	}

	var collected []CollectedOutput
	if o.Outcome == "success" && len(state.OutputFiles) > 0 {
		keys := make([]string, 0, len(state.OutputFiles))
		for k := range state.OutputFiles {
			keys = append(keys, k)
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

	// Sum progress drops from all three pipeline stages so the done event's
	// progress_dropped reflects the true loss between mission-side emits
	// and what's persisted (reader channel-full, writer rate-limit/append
	// errors, and eventfile per-line/per-buffer caps).
	progressDropped := state.ProgressDrops + droppedProgress + ew.ProgressDrops()

	// Detach from ctx so a shutdown-driven kill can still durably finalize.
	// TimeStartedMs is the nowMs we captured before appending the running
	// event and writing missions.time_started — same value, so duration_ms
	// in the done event matches GET /v1/missions/{id}.duration_ms exactly.
	return Finalize(context.WithoutCancel(ctx), db, FinalizeInputs{
		MissionID:       m.ID,
		Kind:            string(m.Kind),
		Lane:            m.Lane,
		Outcome:         o,
		Outputs:         collected,
		TimeStartedMs:   nowMs,
		ProgressDropped: progressDropped,
		Cfg: FinalizeConfig{
			DataDir:        cfg.DataDir,
			MaxReturnValue: cfg.Limits.MaxReturnValueSize,
			MaxFailMessage: cfg.Limits.MaxFailMessageSize,
			MaxFailDetails: cfg.Limits.MaxFailDetailsSize,
			TTL:            TTLPolicyFromConfig(cfg),
		},
	})
}

// TTLPolicyFromConfig builds the staging retention policy from dugdale config.
// Shared by the mission waiter and the exec runtime so output-staging TTL
// recalc can't silently diverge between paths (an empty policy on the
// exec path would leave exec outputs on the 24h sentinel).
func TTLPolicyFromConfig(cfg *config.DugdaleConfig) storage.TTLPolicy {
	return storage.TTLPolicy{
		MissionSuccess: cfg.Cleanup.SuccessTTL,
		MissionFailed:  cfg.Cleanup.FailedTTL,
		ExecSuccess:    cfg.Exec.ExecSuccessTTL,
		ExecFailed:     cfg.Exec.ExecFailedTTL,
		StagingTTL:     cfg.Cleanup.StagingTTL,
		DownloadGrace:  cfg.Cleanup.DownloadedGrace,
	}
}

func killProcessGroup(pgid int, sig syscall.Signal) {
	if pgid <= 0 {
		return
	}
	_ = syscall.Kill(-pgid, sig)
}

func mapCollectErrorToReason(err error) string {
	// ENOSPC during output collect → disk_quota_exceeded.
	if errors.Is(err, syscall.ENOSPC) {
		return "disk_quota_exceeded"
	}
	s := err.Error()
	switch {
	case strings.Contains(s, "missing_output"):
		return "missing_output"
	case strings.Contains(s, "output_path_escape"):
		return "output_path_escape"
	case strings.Contains(s, "output_too_large"):
		return "output_too_large"
	case strings.Contains(s, "output_not_regular_file"):
		return "output_not_regular_file"
	case strings.Contains(s, "data_dir_quota_exceeded"):
		// Soft-cap hit during CollectOutputs. Map to the same fail_reason
		// as ENOSPC so operators see a single "quota" bucket regardless of
		// whether the OS or our soft limit tripped.
		return "disk_quota_exceeded"
	default:
		return "output_collect_failed"
	}
}

// crashedReasonFromOS returns "disk_quota_exceeded" if err wraps ENOSPC,
// otherwise the provided fallback (typically "spawn_failed" or
// "input_materialization_failed"). Used by the crashed-outcome callsites
// in Run / runExec where the underlying os.* failure may be a disk-full
// condition called out by name.
func crashedReasonFromOS(err error, fallback string) string {
	if errors.Is(err, syscall.ENOSPC) {
		return "disk_quota_exceeded"
	}
	return fallback
}

// finalizeCrashed records a spawn-time failure as outcome=crashed without
// having spawned a process. It creates an events file if dispatch hadn't
// already, so Finalize has somewhere to append the done event.
//
// Crashed missions must carry one of these reasons in fail_reason:
// spawn_failed, mission_not_found, mission_not_in_dir,
// input_materialization_failed, disk_quota_exceeded. The caller passes the
// specific reason; the original error message (often prefixed with the
// reason for human readability) goes into fail_message.
func finalizeCrashed(ctx context.Context, cfg *config.DugdaleConfig, db *sql.DB, m *storage.Mission, reason, msg string) error {
	shard, _ := ids.ShardPath(m.ID)
	outDir := filepath.Join(cfg.DataDir, "output", shard)
	if err := os.MkdirAll(outDir, 0o755); err == nil {
		if w, err := eventfile.Create(outDir, m.ID); err == nil {
			_, _ = w.Append(eventfile.KindRunning, map[string]any{"time": time.Now().UnixMilli()}, false)
			_ = w.Close()
		}
	}
	o := OutcomeResult{Outcome: "crashed", FailReason: reason, FailMessage: msg, ExitCode: 0}
	if err := Finalize(context.WithoutCancel(ctx), db, FinalizeInputs{
		MissionID: m.ID,
		Kind:      string(m.Kind),
		Lane:      m.Lane,
		Outcome:   o,
		// TimeStartedMs intentionally 0 — finalizeCrashed runs before a
		// spawn so duration_ms is meaningless; commitFinalize records
		// duration=0 into the histogram (counter still increments).
		Cfg: FinalizeConfig{
			DataDir:        cfg.DataDir,
			MaxReturnValue: cfg.Limits.MaxReturnValueSize,
			MaxFailMessage: cfg.Limits.MaxFailMessageSize,
			MaxFailDetails: cfg.Limits.MaxFailDetailsSize,
			TTL: storage.TTLPolicy{
				MissionSuccess: cfg.Cleanup.SuccessTTL,
				MissionFailed:  cfg.Cleanup.FailedTTL,
				ExecSuccess:    cfg.Exec.ExecSuccessTTL,
				ExecFailed:     cfg.Exec.ExecFailedTTL,
				StagingTTL:     cfg.Cleanup.StagingTTL,
				DownloadGrace:  cfg.Cleanup.DownloadedGrace,
			},
		},
	}); err != nil {
		return fmt.Errorf("finalize crashed: %w (orig: %s)", err, msg)
	}
	return nil
}
