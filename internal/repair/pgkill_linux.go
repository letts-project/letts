//go:build linux

package repair

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"syscall"
)

func platformKillPgid(pid, pgid int, expectedStarttime int64) bool {
	actual := readProcStarttime(pid)
	if actual == 0 || actual != expectedStarttime {
		return false
	}
	_ = syscall.Kill(-pgid, syscall.SIGKILL)
	return true
}

// readProcStarttime parses /proc/<pid>/stat field 22 (starttime expressed in
// clock ticks since boot). Returns 0 on any error.
func readProcStarttime(pid int) int64 {
	b, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
	if err != nil {
		return 0
	}
	closeIdx := strings.LastIndexByte(string(b), ')')
	if closeIdx < 0 || closeIdx+2 >= len(b) {
		return 0
	}
	fields := strings.Fields(string(b[closeIdx+2:]))
	if len(fields) < 20 {
		return 0
	}
	v, err := strconv.ParseInt(fields[19], 10, 64)
	if err != nil {
		return 0
	}
	return v
}
