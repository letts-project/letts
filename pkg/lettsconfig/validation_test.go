package lettsconfig

import (
	"strings"
	"testing"
)

// TestValidateSyntaxAllowsHostlessFragment: per-file validation in
// `letts apply -f base -f overlay` must run only syntax checks, so a delta
// overlay fragment that patches a dugdale by id (inheriting host from the base
// file) isn't rejected for lacking host/url. The full Validate (run on the
// merged config) must still enforce host/url.
func TestValidateSyntaxAllowsHostlessFragment(t *testing.T) {
	c := &Config{Dugdales: []Dugdale{{ID: "s1"}}} // patch fragment: no host/url
	if err := ValidateSyntax(c); err != nil {
		t.Errorf("ValidateSyntax should accept a host-less fragment: %v", err)
	}
	if err := Validate(c); err == nil {
		t.Error("full Validate must still require host/url on the merged config")
	}
}

func TestValidateDugdaleID(t *testing.T) {
	good := []string{"s1", "server-1", "web_prod", "a", "abc1_2-3"}
	bad := []string{"", "1s", "S1", "with space", "Длинный", "a-b-c-d-e-f-g-h-i-j-k-l-m-n-o-p-q-r-s-t-u-v-w-x-y-z-0-1-2-3-4-5-6-7-8-9-too-long-much"}
	for _, s := range good {
		if err := ValidateDugdaleID(s); err != nil {
			t.Errorf("ValidateDugdaleID(%q) unexpected error: %v", s, err)
		}
	}
	for _, s := range bad {
		if err := ValidateDugdaleID(s); err == nil {
			t.Errorf("ValidateDugdaleID(%q) want error, got nil", s)
		}
	}
}

func TestValidateLaneName(t *testing.T) {
	good := []string{"normal", "high", "a", "lane-1", "lane_2"}
	bad := []string{"", "Normal", "1lane", "with space", "this_lane_is_way_too_long_for_thirty_two_chars_limit"}
	for _, s := range good {
		if err := ValidateLaneName(s); err != nil {
			t.Errorf("ValidateLaneName(%q) unexpected error: %v", s, err)
		}
	}
	for _, s := range bad {
		if err := ValidateLaneName(s); err == nil {
			t.Errorf("ValidateLaneName(%q) want error, got nil", s)
		}
	}
}

func TestValidateConfigCatchesAliasCollision(t *testing.T) {
	c := &Config{
		Aliases:  map[string]string{"s1": "real-s1"},
		Dugdales: []Dugdale{{ID: "s1", Host: "h", Token: "t"}},
	}
	if err := Validate(c); err == nil {
		t.Fatal("expected alias-collision error (alias key matches dugdale id)")
	}
}

func TestValidateConfigCatchesBadIDs(t *testing.T) {
	c := &Config{
		Dugdales: []Dugdale{{ID: "BAD ID", Host: "h", Token: "t"}},
	}
	if err := Validate(c); err == nil {
		t.Fatal("expected error on invalid dugdale id")
	}
}

// TestValidateRejectsAliasCycleAtLoad pins the contract: alias
// chains forming a cycle are caught at config-load time, not deferred
// until a dispatch resolution. Mis-configured letts.yaml fails fast.
func TestValidateRejectsAliasCycleAtLoad(t *testing.T) {
	cases := []struct {
		name    string
		aliases map[string]string
	}{
		{"self-ref", map[string]string{"a": "a"}},
		{"two-way", map[string]string{"a": "b", "b": "a"}},
		{"three-way", map[string]string{"a": "b", "b": "c", "c": "a"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := &Config{Aliases: tc.aliases}
			if err := Validate(c); err == nil {
				t.Errorf("Validate(%v) → nil; want cycle/self-ref error", tc.aliases)
			}
		})
	}
}

// TestValidateRejectsDanglingAlias: an alias pointing at a string that is
// neither a known dugdale id nor another alias must error at load.
func TestValidateRejectsDanglingAlias(t *testing.T) {
	c := &Config{Aliases: map[string]string{"prod": "nope"}}
	if err := Validate(c); err == nil {
		t.Errorf("Validate(prod→nope) → nil; want dangling-id error")
	}
}

// TestValidateAcceptsAliasValueWithEnvVar ensures alias values that
// resolve via ${VAR} at runtime are NOT rejected on parse-time validation.
func TestValidateAcceptsAliasValueWithEnvVar(t *testing.T) {
	cases := []map[string]string{
		{"prod": "${PROD_HOST}"},
		{"prod": "${ENV}-web"},
		{"prod": "tenant-${ENV}-prod"},
	}
	for _, aliases := range cases {
		c := &Config{Aliases: aliases}
		if err := Validate(c); err != nil {
			t.Errorf("Validate(%v) unexpected error: %v", aliases, err)
		}
	}
}

// TestValidateErrorPathPrefixes ensures every error has a consistent
// path-prefix that points at the offending section/key.
func TestValidateErrorPathPrefixes(t *testing.T) {
	cases := []struct {
		name    string
		cfg     *Config
		wantPfx string
	}{
		{
			name:    "bad dugdale id",
			cfg:     &Config{Dugdales: []Dugdale{{ID: "BAD"}}},
			wantPfx: "dugdales[0]:",
		},
		{
			name:    "bad lane name under dugdale",
			cfg:     &Config{Dugdales: []Dugdale{{ID: "s1", Lanes: map[string]LaneCfg{"BAD": {}}}}},
			wantPfx: "dugdales[0].lanes:",
		},
		{
			name:    "bad label under dugdale",
			cfg:     &Config{Dugdales: []Dugdale{{ID: "s1", Labels: []string{"BAD"}}}},
			wantPfx: "dugdales[0].labels:",
		},
		{
			name:    "bad template name",
			cfg:     &Config{Templates: map[string]Template{"BAD": {}}},
			wantPfx: `templates["BAD"]:`,
		},
		{
			name:    "bad lane name under template",
			cfg:     &Config{Templates: map[string]Template{"t1": {Lanes: map[string]LaneCfg{"BAD": {}}}}},
			wantPfx: `templates["t1"].lanes:`,
		},
		{
			name:    "bad label under template",
			cfg:     &Config{Templates: map[string]Template{"t1": {Labels: []string{"BAD"}}}},
			wantPfx: `templates["t1"].labels:`,
		},
		{
			name:    "bad route name",
			cfg:     &Config{Routes: map[string]Route{"BAD": {}}},
			wantPfx: `routes["BAD"]:`,
		},
		{
			name:    "bad alias key",
			cfg:     &Config{Aliases: map[string]string{"BAD": "s1"}},
			wantPfx: `aliases["BAD"]:`,
		},
		{
			name: "alias collision",
			cfg: &Config{
				Aliases:  map[string]string{"s1": "real-s1"},
				Dugdales: []Dugdale{{ID: "s1"}},
			},
			wantPfx: `aliases["s1"]:`,
		},
		{
			name:    "empty alias value",
			cfg:     &Config{Aliases: map[string]string{"prod": ""}},
			wantPfx: `aliases["prod"]:`,
		},
		{
			name:    "bad literal alias value",
			cfg:     &Config{Aliases: map[string]string{"prod": "BAD ID"}},
			wantPfx: `aliases["prod"] value:`,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := Validate(tc.cfg)
			if err == nil {
				t.Fatalf("Validate returned nil, want error with prefix %q", tc.wantPfx)
			}
			if !strings.HasPrefix(err.Error(), tc.wantPfx) {
				t.Errorf("Validate error %q, want prefix %q", err.Error(), tc.wantPfx)
			}
		})
	}
}
