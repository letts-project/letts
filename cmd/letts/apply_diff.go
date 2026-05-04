package main

import (
	"reflect"
	"sort"

	"letts/internal/apply"
)

// ClientDiff is the human-readable diff between current and desired state.
type ClientDiff struct {
	AddedLanes       []string
	RemovedLanes     []string
	ChangedLanes     []LaneChange
	MissionDirChange *StringChange
	LabelsChange     *LabelsChange
	RuntimeChange    *RuntimeChange
}

// LaneChange describes a per-lane configuration change.
type LaneChange struct {
	Name           string
	OldConcurrency int
	NewConcurrency int
	OldPaused      bool
	NewPaused      bool
}

// StringChange describes an old→new string transition.
type StringChange struct{ Old, New string }

// LabelsChange describes an old→new labels transition.
type LabelsChange struct{ Old, New []string }

// RuntimeChange describes an old→new runtime transition.
type RuntimeChange struct {
	OldCommandTemplate, NewCommandTemplate         []string
	OldMissionPathTemplate, NewMissionPathTemplate string
	OldValidate, NewValidate                       bool
}

// DiffAppliedState computes a ClientDiff between cur (server) and desired (CLI).
func DiffAppliedState(cur, desired apply.AppliedState) ClientDiff {
	var d ClientDiff
	if cur.MissionDir != desired.MissionDir {
		d.MissionDirChange = &StringChange{Old: cur.MissionDir, New: desired.MissionDir}
	}
	if !reflect.DeepEqual(cur.Labels, desired.Labels) {
		d.LabelsChange = &LabelsChange{Old: cur.Labels, New: desired.Labels}
	}
	if !reflect.DeepEqual(cur.Runtime, desired.Runtime) {
		d.RuntimeChange = &RuntimeChange{
			OldCommandTemplate:     cur.Runtime.CommandTemplate,
			NewCommandTemplate:     desired.Runtime.CommandTemplate,
			OldMissionPathTemplate: cur.Runtime.MissionPathTemplate,
			NewMissionPathTemplate: desired.Runtime.MissionPathTemplate,
			OldValidate:            cur.Runtime.ValidateMissionFile,
			NewValidate:            desired.Runtime.ValidateMissionFile,
		}
	}
	currentNames := keys(cur.Lanes)
	desiredNames := keys(desired.Lanes)
	for _, n := range currentNames {
		if _, ok := desired.Lanes[n]; !ok {
			d.RemovedLanes = append(d.RemovedLanes, n)
		}
	}
	for _, n := range desiredNames {
		c, hadOld := cur.Lanes[n]
		if !hadOld {
			d.AddedLanes = append(d.AddedLanes, n)
			continue
		}
		want := desired.Lanes[n]
		if c.Concurrency != want.Concurrency || c.Paused != want.Paused {
			d.ChangedLanes = append(d.ChangedLanes, LaneChange{
				Name:           n,
				OldConcurrency: c.Concurrency,
				NewConcurrency: want.Concurrency,
				OldPaused:      c.Paused,
				NewPaused:      want.Paused,
			})
		}
	}
	sort.Strings(d.AddedLanes)
	sort.Strings(d.RemovedLanes)
	sort.Slice(d.ChangedLanes, func(i, j int) bool { return d.ChangedLanes[i].Name < d.ChangedLanes[j].Name })
	return d
}

func keys(m map[string]apply.LaneCfg) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
