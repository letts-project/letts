package main

import (
	"testing"

	"letts/pkg/lettsconfig"
)

func TestMergeOverlayReplacesDugdaleByID(t *testing.T) {
	base := &lettsconfig.Config{
		Dugdales: []lettsconfig.Dugdale{
			{ID: "s1", Host: "old-host"},
			{ID: "s2", Host: "stable"},
		},
	}
	overlay := &lettsconfig.Config{
		Dugdales: []lettsconfig.Dugdale{
			{ID: "s1", Host: "new-host"},
			{ID: "s3", Host: "added"},
		},
	}
	got := MergeConfigs(base, overlay)
	if len(got.Dugdales) != 3 {
		t.Fatalf("got %d dugdales", len(got.Dugdales))
	}
	for _, d := range got.Dugdales {
		switch d.ID {
		case "s1":
			if d.Host != "new-host" {
				t.Errorf("s1.host = %q want new-host", d.Host)
			}
		case "s2":
			if d.Host != "stable" {
				t.Errorf("s2.host = %q want stable", d.Host)
			}
		case "s3":
			if d.Host != "added" {
				t.Errorf("s3.host = %q want added", d.Host)
			}
		default:
			t.Errorf("unexpected dugdale id %q", d.ID)
		}
	}
}

// TestMergeOverlayDugdaleFieldLevel enforces that when an
// overlay carries only a subset of a Dugdale's fields (the canonical
// example: a dev overlay that just bumps `port`), the merge must
// preserve base's lanes/labels/runtime/tokens. The previous impl
// did `out[i] = overlay`, wiping every field the overlay didn't
// explicitly set — a major footgun for the layered-config workflow.
func TestMergeOverlayDugdaleFieldLevel(t *testing.T) {
	base := &lettsconfig.Config{
		Dugdales: []lettsconfig.Dugdale{
			{
				ID:         "s1",
				Host:       "prod.example.com",
				Port:       7180,
				MissionDir: "/var/www/missions",
				Labels:     []string{"prod", "web"},
				Token:      "prod-tok",
				Lanes: map[string]lettsconfig.LaneCfg{
					"fast": {Concurrency: 10},
					"slow": {Concurrency: 2},
				},
			},
		},
	}
	overlay := &lettsconfig.Config{
		Dugdales: []lettsconfig.Dugdale{
			{ID: "s1", Port: 7181}, // dev overlay: only port changes
		},
	}
	got := MergeConfigs(base, overlay)
	if len(got.Dugdales) != 1 {
		t.Fatalf("got %d dugdales", len(got.Dugdales))
	}
	d := got.Dugdales[0]
	if d.Host != "prod.example.com" {
		t.Errorf("host wiped: got %q", d.Host)
	}
	if d.Port != 7181 {
		t.Errorf("port not overridden: got %d", d.Port)
	}
	if d.MissionDir != "/var/www/missions" {
		t.Errorf("mission_dir wiped: got %q", d.MissionDir)
	}
	if len(d.Labels) != 2 {
		t.Errorf("labels wiped: got %v", d.Labels)
	}
	if d.Token != "prod-tok" {
		t.Errorf("token wiped: got %q", d.Token)
	}
	if len(d.Lanes) != 2 {
		t.Errorf("lanes wiped: got %v", d.Lanes)
	}
}

// TestMergeOverlayDugdaleLanesMerged verifies that lane maps merge per-key
// on the overlay path (base's untouched lanes stay, overlay's overrides
// win on collision).
func TestMergeOverlayDugdaleLanesMerged(t *testing.T) {
	base := &lettsconfig.Config{
		Dugdales: []lettsconfig.Dugdale{
			{
				ID: "s1",
				Lanes: map[string]lettsconfig.LaneCfg{
					"keep":   {Concurrency: 5},
					"resize": {Concurrency: 1},
				},
			},
		},
	}
	overlay := &lettsconfig.Config{
		Dugdales: []lettsconfig.Dugdale{
			{
				ID: "s1",
				Lanes: map[string]lettsconfig.LaneCfg{
					"resize": {Concurrency: 99}, // override
					"new":    {Concurrency: 3},  // add
				},
			},
		},
	}
	got := MergeConfigs(base, overlay)
	d := got.Dugdales[0]
	if d.Lanes["keep"].Concurrency != 5 {
		t.Errorf("keep wiped: %v", d.Lanes["keep"])
	}
	if d.Lanes["resize"].Concurrency != 99 {
		t.Errorf("resize not overridden: %v", d.Lanes["resize"])
	}
	if d.Lanes["new"].Concurrency != 3 {
		t.Errorf("new not added: %v", d.Lanes["new"])
	}
}

// TestMergeOverlayDropsExplicitValidateMissionFileTrue verifies the
// overlay carrying `runtime: validate_mission_file: false` over a base
// that's true should leave the result false. yaml.v3's bool zero-value
// ambiguity defeats the "if overlay.Runtime.ValidateMissionFile" guard;
// the sentinel must thread through the layered merge too.
func TestMergeOverlayDropsExplicitValidateMissionFileTrue(t *testing.T) {
	base := &lettsconfig.Config{
		Dugdales: []lettsconfig.Dugdale{
			{ID: "s1", Runtime: lettsconfig.Runtime{ValidateMissionFile: true}},
		},
	}
	base.Dugdales[0].SetExplicitValidateMissionFile(true)

	overlay := &lettsconfig.Config{
		Dugdales: []lettsconfig.Dugdale{
			{ID: "s1", Runtime: lettsconfig.Runtime{ValidateMissionFile: false}},
		},
	}
	overlay.Dugdales[0].SetExplicitValidateMissionFile(true)

	got := MergeConfigs(base, overlay)
	if got.Dugdales[0].Runtime.ValidateMissionFile {
		t.Errorf("overlay explicit false did not override base true")
	}
}

// TestMergeOverlayNullifiesLane verifies an overlay using
// `lanes: <name>: null` must drop the inherited lane (mirror of the
// extends behavior for templates).
func TestMergeOverlayNullifiesLane(t *testing.T) {
	base := &lettsconfig.Config{
		Dugdales: []lettsconfig.Dugdale{
			{ID: "s1", Lanes: map[string]lettsconfig.LaneCfg{
				"keep": {Concurrency: 5},
				"drop": {Concurrency: 1},
			}},
		},
	}
	overlay := &lettsconfig.Config{
		Dugdales: []lettsconfig.Dugdale{
			{ID: "s1"},
		},
	}
	overlay.Dugdales[0].SetNullifiedLanes([]string{"drop"})

	got := MergeConfigs(base, overlay)
	d := got.Dugdales[0]
	if _, ok := d.Lanes["drop"]; ok {
		t.Errorf("nullified lane %q still present", "drop")
	}
	if d.Lanes["keep"].Concurrency != 5 {
		t.Errorf("non-nullified lane wiped: %v", d.Lanes["keep"])
	}
}

// TestMergeOverlayNullifiesTemplateInheritedLane verifies the lane being
// dropped is inherited from a template via extends, so it isn't in base.Lanes
// at merge time. The merge must carry the nullification sentinel forward so the
// post-merge ResolveExtends suppresses the template lane (the earlier fix only
// handled lanes already present in base.Lanes).
func TestMergeOverlayNullifiesTemplateInheritedLane(t *testing.T) {
	base := &lettsconfig.Config{
		Templates: map[string]lettsconfig.Template{
			"tmpl": {Lanes: map[string]lettsconfig.LaneCfg{
				"normal":  {Concurrency: 5},
				"parsers": {Concurrency: 3},
			}},
		},
		Dugdales: []lettsconfig.Dugdale{{ID: "s1", Host: "h", Extends: "tmpl"}},
	}
	overlay := &lettsconfig.Config{Dugdales: []lettsconfig.Dugdale{{ID: "s1"}}}
	overlay.Dugdales[0].SetNullifiedLanes([]string{"parsers"})

	merged := MergeConfigs(base, overlay)
	if err := lettsconfig.ResolveExtends(merged); err != nil {
		t.Fatalf("ResolveExtends: %v", err)
	}
	d := merged.Dugdales[0]
	if _, ok := d.Lanes["parsers"]; ok {
		t.Errorf("template-inherited lane %q should be dropped by overlay null, got %v", "parsers", d.Lanes)
	}
	if d.Lanes["normal"].Concurrency != 5 {
		t.Errorf("non-nullified inherited lane wiped: %v", d.Lanes)
	}
}

func TestMergeOverlayMapsAreUnion(t *testing.T) {
	base := &lettsconfig.Config{
		Aliases: map[string]string{"a": "1", "b": "2"},
		Routes:  map[string]lettsconfig.Route{"r": {Host: "h", Lane: "l"}},
	}
	overlay := &lettsconfig.Config{
		Aliases: map[string]string{"b": "3", "c": "4"},
		Routes:  map[string]lettsconfig.Route{"r2": {Host: "h2", Lane: "l2"}},
	}
	got := MergeConfigs(base, overlay)
	if got.Aliases["a"] != "1" || got.Aliases["b"] != "3" || got.Aliases["c"] != "4" {
		t.Errorf("aliases = %v", got.Aliases)
	}
	if got.Routes["r"].Host != "h" || got.Routes["r2"].Host != "h2" {
		t.Errorf("routes = %v", got.Routes)
	}
}
