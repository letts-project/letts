package lettsconfig

import (
	"reflect"
	"testing"
)

func TestExtendsScalarInheritance(t *testing.T) {
	c := &Config{
		Templates: map[string]Template{
			"k": {
				MissionDir: "/var/www",
				Token:      "tpl-token",
				AdminToken: "tpl-admin",
				Labels:     []string{"prod"},
				Lanes:      map[string]LaneCfg{"normal": {Concurrency: 5}},
			},
		},
		Dugdales: []Dugdale{
			{ID: "s1", Host: "h", Extends: "k"},
			{ID: "s2", Host: "h", Extends: "k", Token: "own-token"},
		},
	}
	if err := ResolveExtends(c); err != nil {
		t.Fatal(err)
	}
	if c.Dugdales[0].MissionDir != "/var/www" {
		t.Errorf("s1.MissionDir = %q", c.Dugdales[0].MissionDir)
	}
	if c.Dugdales[0].Token != "tpl-token" {
		t.Errorf("s1.Token = %q want tpl-token", c.Dugdales[0].Token)
	}
	if c.Dugdales[1].Token != "own-token" {
		t.Errorf("s2.Token = %q want own-token (dugdale overrides)", c.Dugdales[1].Token)
	}
	if !reflect.DeepEqual(c.Dugdales[0].Labels, []string{"prod"}) {
		t.Errorf("s1.Labels = %v", c.Dugdales[0].Labels)
	}
}

func TestExtendsLanesDeepMerge(t *testing.T) {
	c := &Config{
		Templates: map[string]Template{
			"k": {
				Lanes: map[string]LaneCfg{
					"normal":  {Concurrency: 10},
					"parsers": {Concurrency: 3},
				},
			},
		},
		Dugdales: []Dugdale{
			{
				ID:      "s7",
				Host:    "h",
				Extends: "k",
				Lanes: map[string]LaneCfg{
					"rutracker": {Concurrency: 1},
					"parsers":   {Concurrency: 0},
				},
			},
		},
	}
	if err := ResolveExtends(c); err != nil {
		t.Fatal(err)
	}
	got := c.Dugdales[0].Lanes
	if got["normal"].Concurrency != 10 {
		t.Errorf("normal = %d", got["normal"].Concurrency)
	}
	if got["parsers"].Concurrency != 0 {
		t.Errorf("parsers = %d want override 0", got["parsers"].Concurrency)
	}
	if got["rutracker"].Concurrency != 1 {
		t.Errorf("rutracker = %d", got["rutracker"].Concurrency)
	}
}

func TestExtendsArrayReplace(t *testing.T) {
	c := &Config{
		Templates: map[string]Template{
			"k": {Labels: []string{"prod"}},
		},
		Dugdales: []Dugdale{
			{ID: "s", Host: "h", Extends: "k", Labels: []string{"dev", "web"}},
		},
	}
	if err := ResolveExtends(c); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(c.Dugdales[0].Labels, []string{"dev", "web"}) {
		t.Errorf("labels = %v want [dev web]", c.Dugdales[0].Labels)
	}
}

func TestExtendsUnknownTemplate(t *testing.T) {
	c := &Config{
		Dugdales: []Dugdale{{ID: "s", Host: "h", Extends: "missing"}},
	}
	if err := ResolveExtends(c); err == nil {
		t.Fatal("expected error for missing template")
	}
}
