package e2e_test

import (
	"testing"
	"time"
)

// TestDaemonStartsAndStops verifies the harness itself: dugdale builds,
// starts on a random loopback port, /healthz returns 200, and SIGTERM
// shuts it down cleanly within the grace window.
func TestDaemonStartsAndStops(t *testing.T) {
	d := startDaemon(t, daemonOpts{})

	// Sanity: /healthz is reachable through the helper.
	resp, err := d.Do("GET", "/v1/healthz", "", nil)
	if err != nil {
		t.Fatalf("healthz: %v", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("healthz status=%d", resp.StatusCode)
	}

	d.Stop()
	code, err := d.WaitExit(5 * time.Second)
	if err != nil {
		t.Fatalf("daemon exit: %v\nlogs:\n%s", err, d.Logs())
	}
	if code != 0 {
		t.Errorf("exit code = %d, want 0", code)
	}
}
