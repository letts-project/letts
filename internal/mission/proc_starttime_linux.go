//go:build linux

package mission

import (
	"fmt"
	"os"
	"strconv"
	"strings"
)

// readProcStarttime reads /proc/<pid>/stat field 22 (clock-tick offset since
// boot). Used together with pid to detect pid reuse across dugdale restarts.
// Returns 0 on any error.
func readProcStarttime(pid int) int64 {
	b, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
	if err != nil {
		return 0
	}
	// Field 2 is "(comm)" possibly containing spaces; locate the trailing ')'
	// and split the remainder by whitespace.
	close := strings.LastIndexByte(string(b), ')')
	if close < 0 || close+2 >= len(b) {
		return 0
	}
	fields := strings.Fields(string(b[close+2:]))
	// After ')' the remaining fields start at #3 (state). Field 22 is index 19.
	if len(fields) < 20 {
		return 0
	}
	v, err := strconv.ParseInt(fields[19], 10, 64)
	if err != nil {
		return 0
	}
	return v
}
