package main

import (
	"reflect"
	"testing"

	"letts/internal/apply"
	"letts/pkg/lettsconfig"
)

func TestBuildPayload(t *testing.T) {
	d := lettsconfig.Dugdale{
		ID: "s1", MissionDir: "/var/www", Labels: []string{"prod"},
		Runtime: lettsconfig.Runtime{CommandTemplate: []string{"php", "{mission}.php"}},
		Lanes:   map[string]lettsconfig.LaneCfg{"normal": {Concurrency: 4}},
	}
	got := BuildAppliedState(&d)
	want := apply.AppliedState{
		MissionDir: "/var/www",
		Labels:     []string{"prod"},
		Runtime:    apply.Runtime{CommandTemplate: []string{"php", "{mission}.php"}},
		Lanes:      map[string]apply.LaneCfg{"normal": {Concurrency: 4}},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %+v want %+v", got, want)
	}
}
