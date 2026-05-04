package e2e_test

import (
	"testing"
)

// TestYAMLPausedFalseUnpauses_RealDaemon is the real-process variant of
// internal/apply's TestApplyPauseProvenance_YAMLUnpausesYAMLOrigin. Boots a
// dugdale subprocess, applies a yaml-origin pause via /v1/admin/apply,
// re-applies with paused:false, and asserts the persisted state shows the
// lane unpaused. An earlier behaviour treated all pauses as sticky and
// would have left paused:true persisted.
func TestYAMLPausedFalseUnpauses_RealDaemon(t *testing.T) {
	d := startDaemon(t, daemonOpts{})

	apply := func(paused bool) {
		t.Helper()
		body := map[string]any{
			"mission_dir": "/tmp",
			"lanes": map[string]any{
				"work": map[string]any{"concurrency": 1, "paused": paused},
			},
		}
		if err := d.DoJSON("POST", "/v1/admin/apply", d.AdminTok, body, nil); err != nil {
			t.Fatalf("apply paused=%v: %v", paused, err)
		}
	}

	readLane := func() (paused bool, pausedBy string) {
		t.Helper()
		var resp map[string]any
		if err := d.DoJSON("GET", "/v1/admin/state", d.AdminTok, nil, &resp); err != nil {
			t.Fatalf("state: %v", err)
		}
		state, _ := resp["state"].(map[string]any)
		lanes, _ := state["lanes"].(map[string]any)
		work, _ := lanes["work"].(map[string]any)
		paused, _ = work["paused"].(bool)
		pausedBy, _ = work["paused_by"].(string)
		return
	}

	apply(true)
	if p, by := readLane(); !p || by != "yaml" {
		t.Fatalf("after yaml-pause: paused=%v by=%q want true/yaml", p, by)
	}

	apply(false)
	if p, by := readLane(); p || by != "" {
		t.Errorf("after yaml unpause: paused=%v by=%q want false/empty", p, by)
	}
}

// TestCtlPauseSurvivesYAMLReapply_RealDaemon: pause via the admin
// /lanes/{name}/pause endpoint (ctl path) and then apply a YAML with
// paused:false. The lane must STAY paused (preserve ctl pauses).
func TestCtlPauseSurvivesYAMLReapply_RealDaemon(t *testing.T) {
	d := startDaemon(t, daemonOpts{})

	initial := map[string]any{
		"mission_dir": "/tmp",
		"lanes":       map[string]any{"work": map[string]any{"concurrency": 1}},
	}
	if err := d.DoJSON("POST", "/v1/admin/apply", d.AdminTok, initial, nil); err != nil {
		t.Fatalf("initial apply: %v", err)
	}

	// Ctl pause.
	if err := d.DoJSON("POST", "/v1/admin/lanes/work/pause", d.AdminTok, nil, nil); err != nil {
		t.Fatalf("ctl pause: %v", err)
	}

	// Re-apply YAML with paused:false. Must preserve ctl pause.
	yamlUnpause := map[string]any{
		"mission_dir": "/tmp",
		"lanes":       map[string]any{"work": map[string]any{"concurrency": 1, "paused": false}},
	}
	if err := d.DoJSON("POST", "/v1/admin/apply", d.AdminTok, yamlUnpause, nil); err != nil {
		t.Fatalf("re-apply: %v", err)
	}

	var resp map[string]any
	if err := d.DoJSON("GET", "/v1/admin/state", d.AdminTok, nil, &resp); err != nil {
		t.Fatalf("state: %v", err)
	}
	state, _ := resp["state"].(map[string]any)
	lanes, _ := state["lanes"].(map[string]any)
	work, _ := lanes["work"].(map[string]any)
	paused, _ := work["paused"].(bool)
	by, _ := work["paused_by"].(string)
	if !paused || by != "ctl" {
		t.Errorf("after ctl-pause and yaml unpause: paused=%v by=%q want true/ctl", paused, by)
	}

	// `letts ctl lanes continue` (via API) clears the pause.
	if err := d.DoJSON("POST", "/v1/admin/lanes/work/continue", d.AdminTok, nil, nil); err != nil {
		t.Fatalf("continue: %v", err)
	}
	if err := d.DoJSON("GET", "/v1/admin/state", d.AdminTok, nil, &resp); err != nil {
		t.Fatalf("state2: %v", err)
	}
	state, _ = resp["state"].(map[string]any)
	lanes, _ = state["lanes"].(map[string]any)
	work, _ = lanes["work"].(map[string]any)
	paused, _ = work["paused"].(bool)
	by, _ = work["paused_by"].(string)
	if paused || by != "" {
		t.Errorf("after continue: paused=%v by=%q want false/empty", paused, by)
	}
}
