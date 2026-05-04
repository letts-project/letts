package main

import (
	"testing"

	"letts/internal/apply"
)

func TestDiffAppliedStateAddedLane(t *testing.T) {
	cur := apply.AppliedState{Lanes: map[string]apply.LaneCfg{"a": {Concurrency: 2}}}
	want := apply.AppliedState{Lanes: map[string]apply.LaneCfg{"a": {Concurrency: 2}, "b": {Concurrency: 1}}}
	d := DiffAppliedState(cur, want)
	if len(d.AddedLanes) != 1 || d.AddedLanes[0] != "b" {
		t.Errorf("added = %v", d.AddedLanes)
	}
}

func TestDiffAppliedStateRemovedLane(t *testing.T) {
	cur := apply.AppliedState{Lanes: map[string]apply.LaneCfg{"a": {Concurrency: 2}, "b": {Concurrency: 1}}}
	want := apply.AppliedState{Lanes: map[string]apply.LaneCfg{"a": {Concurrency: 2}}}
	d := DiffAppliedState(cur, want)
	if len(d.RemovedLanes) != 1 || d.RemovedLanes[0] != "b" {
		t.Errorf("removed = %v", d.RemovedLanes)
	}
}

func TestDiffAppliedStateChangedConcurrency(t *testing.T) {
	cur := apply.AppliedState{Lanes: map[string]apply.LaneCfg{"a": {Concurrency: 2}}}
	want := apply.AppliedState{Lanes: map[string]apply.LaneCfg{"a": {Concurrency: 5}}}
	d := DiffAppliedState(cur, want)
	if len(d.ChangedLanes) != 1 || d.ChangedLanes[0].Name != "a" || d.ChangedLanes[0].OldConcurrency != 2 || d.ChangedLanes[0].NewConcurrency != 5 {
		t.Errorf("changed = %+v", d.ChangedLanes)
	}
}

func TestDiffAppliedStateRuntimeChange(t *testing.T) {
	cur := apply.AppliedState{MissionDir: "/old"}
	want := apply.AppliedState{MissionDir: "/new"}
	d := DiffAppliedState(cur, want)
	if d.MissionDirChange == nil {
		t.Fatal("expected MissionDirChange")
	}
	if d.MissionDirChange.Old != "/old" || d.MissionDirChange.New != "/new" {
		t.Errorf("got %+v", d.MissionDirChange)
	}
}
