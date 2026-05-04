package repair_test

import (
	"runtime"
	"testing"

	"letts/internal/repair"
)

func TestBestEffortKillPgidGuardsZeroArgs(t *testing.T) {
	if repair.BestEffortKillPgid(0, 1, 1) {
		t.Error("pid=0 should return false")
	}
	if repair.BestEffortKillPgid(1, 0, 1) {
		t.Error("pgid=0 should return false")
	}
	if repair.BestEffortKillPgid(1, 1, 0) {
		t.Error("starttime=0 should return false")
	}
}

func TestBestEffortKillPgidNoOpOnNonLinux(t *testing.T) {
	if runtime.GOOS == "linux" {
		t.Skip("linux variant covered by TestBestEffortKillPgidLinux*")
	}
	// Even with apparently valid args, non-linux platformKillPgid returns false.
	if repair.BestEffortKillPgid(1, 1, 12345) {
		t.Error("non-linux platforms should never claim a kill")
	}
}

func TestBestEffortKillPgidUnknownPidReturnsFalse(t *testing.T) {
	// pid 99999999 should not exist on any test machine; expect false.
	if repair.BestEffortKillPgid(99999999, 99999999, 12345) {
		t.Error("unknown pid should return false (identity mismatch)")
	}
}
