package lettsconfig

import (
	"os"
	"path/filepath"
	"testing"
)

// TestResolveExtendsDeletesLaneByNull verifies: a dugdale that
// extends a template can suppress an inherited lane via the YAML
// `lanes: <name>: null` syntax. The dugdale ends up with everything
// the template provided except the nullified lane.
func TestResolveExtendsDeletesLaneByNull(t *testing.T) {
	src := `
templates:
  base:
    mission_dir: /srv/m
    lanes:
      high: {concurrency: 5}
      low:  {concurrency: 1}

dugdales:
  - id: s1
    host: server1
    extends: base
    lanes:
      low: null
`
	c, err := Load([]byte(src))
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if err := ResolveExtends(c); err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if len(c.Dugdales) != 1 {
		t.Fatalf("dugdales=%d", len(c.Dugdales))
	}
	d := c.Dugdales[0]
	if _, ok := d.Lanes["high"]; !ok {
		t.Error("inherited high lane was dropped")
	}
	if _, ok := d.Lanes["low"]; ok {
		t.Error("explicit null on low did NOT remove the inherited lane")
	}
}

// A dugdale that extends a template setting
// validate_mission_file=true must be able to OPT-OUT by setting the
// field to `false` explicitly. The bool zero-value collapses
// `unset` and `false` together, so the previous merge logic ignored
// explicit `false` and inherited template's `true`.
//
// AST tracking (mirroring the lanes-null trick) records whether
// the YAML carried a value for the key; the merge then trusts the
// dugdale's value whenever it was explicitly set.
func TestResolveExtendsExplicitFalseOverridesTemplateTrue(t *testing.T) {
	src := `
templates:
  strict:
    runtime:
      validate_mission_file: true

dugdales:
  - id: s1
    host: a
    extends: strict
    runtime:
      validate_mission_file: false
`
	c, err := Load([]byte(src))
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if err := ResolveExtends(c); err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if c.Dugdales[0].Runtime.ValidateMissionFile {
		t.Error("dugdale's explicit false was overridden by template's true")
	}
}

// Sanity: an absent runtime block on the dugdale still inherits the
// template's value (the regression we'd cause by treating false as
// "explicit" universally).
func TestResolveExtendsAbsentInheritsTemplateTrue(t *testing.T) {
	src := `
templates:
  strict:
    runtime:
      validate_mission_file: true

dugdales:
  - id: s1
    host: a
    extends: strict
`
	c, err := Load([]byte(src))
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if err := ResolveExtends(c); err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if !c.Dugdales[0].Runtime.ValidateMissionFile {
		t.Error("dugdale with no runtime block must inherit template's true")
	}
}

// TestResolveExtendsNullDoesntAffectUnrelatedLanes ensures lane null in
// dugdale A doesn't bleed into dugdale B's inherited template lanes.
func TestResolveExtendsNullDoesntAffectUnrelatedLanes(t *testing.T) {
	src := `
templates:
  base:
    lanes:
      shared: {concurrency: 3}

dugdales:
  - id: s1
    host: a
    extends: base
    lanes:
      shared: null
  - id: s2
    host: b
    extends: base
`
	c, err := Load([]byte(src))
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if err := ResolveExtends(c); err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if _, ok := c.Dugdales[0].Lanes["shared"]; ok {
		t.Error("s1 still has nullified lane")
	}
	if _, ok := c.Dugdales[1].Lanes["shared"]; !ok {
		t.Error("s2 lost its inherited lane")
	}
}

// TestLoadAndResolveInsecureBypassesPermsCheck verifies that
// LoadAndResolveWithOpts({Insecure:true}) accepts a plain-text-token
// letts.yaml even with world-readable 0644 perms. Mirrors the dugdale
// --insecure-config-permissions flag.
func TestLoadAndResolveInsecureBypassesPermsCheck(t *testing.T) {
	src := "auth:\n  token: \"plain-abc\"\ndugdales: []\n"
	tmp := t.TempDir()
	p := filepath.Join(tmp, "letts.yaml")
	if err := os.WriteFile(p, []byte(src), 0o644); err != nil {
		t.Fatal(err)
	}
	// Strict mode (default) rejects 0644 with a plain token.
	if _, err := LoadAndResolve(p); err == nil {
		t.Error("expected permissions error in strict mode")
	}
	// Insecure mode lets it load.
	c, err := LoadAndResolveWithOpts(p, ResolveOpts{Insecure: true})
	if err != nil {
		t.Errorf("insecure load: unexpected error: %v", err)
	}
	if c == nil || c.Auth.Token != "plain-abc" {
		t.Errorf("config not loaded correctly: %+v", c)
	}
}

func TestLoadAndResolveFullPipeline(t *testing.T) {
	src := `
auth:
  token: "${TOK}"

templates:
  k:
    mission_dir: /var/www/missions
    labels: [prod]
    lanes:
      normal: {concurrency: 10}

dugdales:
  - id: s1
    host: server1
    extends: k
  - id: s2
    host: server2
    extends: k
    lanes:
      high: {concurrency: 4}
`
	tmp := t.TempDir()
	p := filepath.Join(tmp, "letts.yaml")
	if err := os.WriteFile(p, []byte(src), 0644); err != nil {
		t.Fatal(err)
	}
	c, err := LoadAndResolve(p)
	if err != nil {
		t.Fatal(err)
	}
	if c.Dugdales[0].MissionDir != "/var/www/missions" {
		t.Errorf("s1.MissionDir not inherited from template: %q", c.Dugdales[0].MissionDir)
	}
	if _, ok := c.Dugdales[1].Lanes["high"]; !ok {
		t.Error("s2.high missing after merge")
	}
	if _, ok := c.Dugdales[1].Lanes["normal"]; !ok {
		t.Error("s2.normal missing (should inherit from template)")
	}
}
