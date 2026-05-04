package main

import (
	"letts/internal/apply"
	"letts/pkg/lettsconfig"
)

// BuildAppliedState converts a resolved Dugdale (post-extends) into the
// wire shape POSTed to /v1/admin/apply. Identity fields are stripped.
func BuildAppliedState(d *lettsconfig.Dugdale) apply.AppliedState {
	lanes := map[string]apply.LaneCfg{}
	for n, lc := range d.Lanes {
		lanes[n] = apply.LaneCfg{Concurrency: lc.Concurrency, Paused: lc.Paused}
	}
	return apply.AppliedState{
		MissionDir: d.MissionDir,
		Labels:     append([]string(nil), d.Labels...),
		Lanes:      lanes,
		Runtime: apply.Runtime{
			MissionPathTemplate: d.Runtime.MissionPathTemplate,
			CommandTemplate:     append([]string(nil), d.Runtime.CommandTemplate...),
			ValidateMissionFile: d.Runtime.ValidateMissionFile,
		},
	}
}
