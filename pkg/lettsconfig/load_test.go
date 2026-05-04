package lettsconfig

import (
	"testing"
)

func TestLoadMinimal(t *testing.T) {
	src := `
dugdales:
  - id: s1
    host: server1.internal
    token: tok-disp
    lanes:
      normal: {concurrency: 4}
`
	c, err := Load([]byte(src))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if len(c.Dugdales) != 1 {
		t.Fatalf("want 1 dugdale, got %d", len(c.Dugdales))
	}
	if c.Dugdales[0].ID != "s1" {
		t.Errorf("id = %q, want s1", c.Dugdales[0].ID)
	}
	if c.Dugdales[0].Lanes["normal"].Concurrency != 4 {
		t.Errorf("normal.concurrency = %d, want 4", c.Dugdales[0].Lanes["normal"].Concurrency)
	}
}

func TestLoadFullExample(t *testing.T) {
	src := `
auth:
  token:       "${LETTS_DISPATCH_TOKEN}"
  admin_token: "${LETTS_ADMIN_TOKEN}"

defaults:
  port: 7180

selector:
  match: [prod, web]

routes:
  normal: {host: local, lane: normal}

aliases:
  local: s7

templates:
  k:
    mission_dir: /var/www/missions
    labels: [prod, web]
    lanes:
      normal: {concurrency: 10}

dugdales:
  - id: s1
    host: server1.internal
    extends: k
  - id: s7
    host: server7.internal
    extends: k
`
	c, err := Load([]byte(src))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if c.Defaults.Port != 7180 {
		t.Errorf("port = %d, want 7180", c.Defaults.Port)
	}
	if c.Routes["normal"].Lane != "normal" {
		t.Errorf("route normal.lane = %q", c.Routes["normal"].Lane)
	}
	if c.Aliases["local"] != "s7" {
		t.Errorf("alias local = %q", c.Aliases["local"])
	}
	if len(c.Templates) != 1 || c.Templates["k"].MissionDir != "/var/www/missions" {
		t.Errorf("template k missing or wrong: %+v", c.Templates)
	}
	if len(c.Dugdales) != 2 {
		t.Fatalf("want 2 dugdales, got %d", len(c.Dugdales))
	}
}

func TestLoadInvalidYAML(t *testing.T) {
	if _, err := Load([]byte("dugdales: [not-a-mapping]")); err == nil {
		t.Fatal("expected error on invalid yaml shape")
	}
}

// TestLoadRejectsUnknownField exercises the strict-mode KnownFields(true)
// decoder setting: typos and out-of-schema keys must fail rather than be
// silently dropped. Otherwise a misspelled `dispatch_token` would resolve
// to a missing token at runtime.
func TestLoadRejectsUnknownField(t *testing.T) {
	cases := []struct {
		name string
		src  string
	}{
		{
			name: "unknown top-level field",
			src: `
mystery_field: 42
dugdales:
  - id: s1
    host: h
`,
		},
		{
			name: "unknown field in dugdale entry",
			src: `
dugdales:
  - id: s1
    host: h
    dispach_token: oops
`,
		},
		{
			name: "unknown field in lane",
			src: `
dugdales:
  - id: s1
    host: h
    lanes:
      normal: {concurrency: 4, paralellism: 8}
`,
		},
		{
			name: "unknown field in template",
			src: `
templates:
  k:
    mission_directory: /var/www
dugdales:
  - id: s1
    host: h
`,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Load([]byte(tc.src))
			if err == nil {
				t.Fatal("expected KnownFields error on unknown key, got nil")
			}
		})
	}
}
