package mission

import (
	"fmt"
	"io"
	"os"
	"os/exec"
	"syscall"
	"time"
)

// SpawnResult bundles the running process and the parent-side fd3 read end.
// The fd3 reader goroutine drains this.
type SpawnResult struct {
	Cmd       *exec.Cmd
	Fd3Reader *os.File // parent-side read end of fd 3 pipe
}

// Spawn builds the command per argv with given env, wires stdout/stderr to
// outWriter/errWriter, sets Setpgid, attaches fd 3 via ExtraFiles, and calls
// Start. Returns after Start; caller is responsible for Wait and for killing
// on error.
//
// stdinPath, if non-empty, is opened read-only and wired to cmd.Stdin. The
// parent's copy is closed after Start (child has its own dup'd fd).
//
// waitDelay bounds how long cmd.Wait blocks on the stdout/stderr pipes after
// the leader exits (see the cmd.WaitDelay assignment below); 0 keeps Go's
// default read-until-EOF behavior.
func Spawn(argv []string, env []string, workdir string, stdout, stderr io.Writer, stdinPath string, waitDelay time.Duration) (*SpawnResult, error) {
	if len(argv) == 0 {
		return nil, fmt.Errorf("spawn: empty argv")
	}

	cmd := exec.Command(argv[0], argv[1:]...)
	cmd.Env = env
	cmd.Dir = workdir
	cmd.Stdout = stdout
	cmd.Stderr = stderr
	// Invariant: stdout/stderr write-ends inherited by surviving descendants
	// must not block reaping the leader. Stdout/Stderr above are io.Writers,
	// so os/exec creates pipes and copy goroutines internally and Wait blocks
	// until EOF on them — i.e. until every process holding the write-ends
	// exits. A mission that daemonizes something would pin the lane slot
	// indefinitely. WaitDelay forces those pipes closed that long after the
	// leader exits (Wait then reports exec.ErrWaitDelay with ProcessState
	// intact; callers must tolerate it). The fd3 pipe is ours, not os/exec's
	// — it has its own bounded shutdown in the waiter; this bounds only the
	// pipes os/exec owns. The field is consumed by Wait; assigning it here,
	// before Start, keeps every Wait call site covered without races.
	cmd.WaitDelay = waitDelay

	// Open stdin file before Start; close parent copy after (child holds a dup).
	var stdinFile *os.File
	if stdinPath != "" {
		var err error
		stdinFile, err = os.Open(stdinPath)
		if err != nil {
			return nil, fmt.Errorf("spawn: open stdin %s: %w", stdinPath, err)
		}
		cmd.Stdin = stdinFile
	}

	// Create fd3 pipe. cmd.ExtraFiles[0] becomes child fd 3
	// (stdin/stdout/stderr occupy 0/1/2 — ExtraFiles start at 3).
	r3, w3, err := os.Pipe()
	if err != nil {
		if stdinFile != nil {
			_ = stdinFile.Close()
		}
		return nil, fmt.Errorf("spawn: pipe fd3: %w", err)
	}
	cmd.ExtraFiles = []*os.File{w3}

	cmd.SysProcAttr = &syscall.SysProcAttr{
		Setpgid: true,
	}

	if err := cmd.Start(); err != nil {
		_ = r3.Close()
		_ = w3.Close()
		if stdinFile != nil {
			_ = stdinFile.Close()
		}
		return nil, fmt.Errorf("spawn: start: %w", err)
	}

	// Close parent copies now that child has inherited them.
	_ = w3.Close()
	if stdinFile != nil {
		_ = stdinFile.Close()
	}

	return &SpawnResult{Cmd: cmd, Fd3Reader: r3}, nil
}
