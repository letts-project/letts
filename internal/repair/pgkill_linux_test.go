//go:build linux

package repair_test

import (
	"os/exec"
	"syscall"
	"testing"
	"time"

	"letts/internal/repair"
)

// readProcStarttime is duplicated from the linux variant to read the spawned
// child's starttime; we can't import the unexported version.
func readProcStarttimeForTest(t *testing.T, pid int) int64 {
	t.Helper()
	b, err := exec.Command("cat", "/proc/"+itoa(pid)+"/stat").Output()
	if err != nil {
		t.Fatalf("read stat: %v", err)
	}
	s := string(b)
	close := -1
	for i := len(s) - 1; i >= 0; i-- {
		if s[i] == ')' {
			close = i
			break
		}
	}
	if close < 0 {
		t.Fatalf("malformed stat: %q", s)
	}
	rest := s[close+2:]
	var fields []string
	cur := ""
	for _, r := range rest {
		if r == ' ' || r == '\n' {
			if cur != "" {
				fields = append(fields, cur)
				cur = ""
			}
		} else {
			cur += string(r)
		}
	}
	if cur != "" {
		fields = append(fields, cur)
	}
	if len(fields) < 20 {
		t.Fatalf("not enough fields: %v", fields)
	}
	v := int64(0)
	for _, c := range fields[19] {
		if c < '0' || c > '9' {
			t.Fatalf("non-digit in starttime: %q", fields[19])
		}
		v = v*10 + int64(c-'0')
	}
	return v
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	s := ""
	for i > 0 {
		s = string(rune('0'+i%10)) + s
		i /= 10
	}
	return s
}

func TestBestEffortKillPgidLinuxKills(t *testing.T) {
	cmd := exec.Command("sleep", "30")
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := cmd.Start(); err != nil {
		t.Skipf("can't spawn sleep: %v", err)
	}
	pid := cmd.Process.Pid
	pgid, err := syscall.Getpgid(pid)
	if err != nil {
		cmd.Process.Kill()
		t.Fatalf("getpgid: %v", err)
	}
	starttime := readProcStarttimeForTest(t, pid)
	if starttime == 0 {
		cmd.Process.Kill()
		t.Fatal("starttime=0")
	}

	if !repair.BestEffortKillPgid(pid, pgid, starttime) {
		cmd.Process.Kill()
		t.Fatal("BestEffortKillPgid returned false on matching identity")
	}

	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		cmd.Process.Kill()
		t.Fatal("process didn't exit after kill")
	}
}

func TestBestEffortKillPgidLinuxIdentityMismatch(t *testing.T) {
	cmd := exec.Command("sleep", "30")
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := cmd.Start(); err != nil {
		t.Skipf("can't spawn sleep: %v", err)
	}
	defer cmd.Process.Kill()
	pid := cmd.Process.Pid
	pgid, _ := syscall.Getpgid(pid)
	starttime := readProcStarttimeForTest(t, pid)
	bogusStart := starttime + 1234567

	if repair.BestEffortKillPgid(pid, pgid, bogusStart) {
		t.Error("kill issued despite identity mismatch")
	}
	// Process should still be alive.
	if err := cmd.Process.Signal(syscall.Signal(0)); err != nil {
		t.Errorf("process unexpectedly dead: %v", err)
	}
}
