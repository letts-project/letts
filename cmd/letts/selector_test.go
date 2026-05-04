package main

import (
	"strings"
	"testing"
	"time"

	"letts/pkg/lettsclient"
)

// TestParseSelectorAllKeys verifies every supported key parses to the
// matching Selector field, including both relative and absolute time forms.
func TestParseSelectorAllKeys(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	s, err := ParseSelector("status=done,outcome=failed,lane=normal,mission=Demo,mission_prefix=De,since=-1h,until=1714600000123", now)
	if err != nil {
		t.Fatalf("ParseSelector: %v", err)
	}
	if s.Status != "done" {
		t.Errorf("Status=%q", s.Status)
	}
	if s.Outcome != "failed" {
		t.Errorf("Outcome=%q", s.Outcome)
	}
	if s.Lane != "normal" {
		t.Errorf("Lane=%q", s.Lane)
	}
	if s.Mission != "Demo" {
		t.Errorf("Mission=%q", s.Mission)
	}
	if s.MissionPrefix != "De" {
		t.Errorf("MissionPrefix=%q", s.MissionPrefix)
	}
	want := now.Add(-time.Hour).UnixMilli()
	if s.SinceMs != want {
		t.Errorf("SinceMs=%d want %d", s.SinceMs, want)
	}
	if s.UntilMs != 1714600000123 {
		t.Errorf("UntilMs=%d want 1714600000123", s.UntilMs)
	}
}

// TestParseSelectorEmpty — empty input is not an error; callers use this to
// distinguish "no --selector" from "bad selector".
func TestParseSelectorEmpty(t *testing.T) {
	s, err := ParseSelector("", time.Now())
	if err != nil {
		t.Fatalf("ParseSelector empty: %v", err)
	}
	if (s != Selector{}) {
		t.Errorf("empty selector should yield zero Selector, got %+v", s)
	}
}

// TestParseSelectorRelativeSinceDays — `-7d` is a parseSinceTime extension
// (Go's time.ParseDuration tops at hours); verify it's plumbed through.
func TestParseSelectorRelativeSinceDays(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	s, err := ParseSelector("since=-7d", now)
	if err != nil {
		t.Fatalf("ParseSelector: %v", err)
	}
	want := now.Add(-7 * 24 * time.Hour).UnixMilli()
	if s.SinceMs != want {
		t.Errorf("SinceMs=%d want %d", s.SinceMs, want)
	}
}

// TestParseSelectorErrors covers malformed pairs, unknown keys, and bad
// time values; each must surface a descriptive error so the CLI can map
// it to BadUsageError.
func TestParseSelectorErrors(t *testing.T) {
	now := time.Now()
	cases := []struct {
		in       string
		errSubst string
	}{
		{"=value", "bad selector pair"},
		{"nokey", "bad selector pair"},
		{"bogus=x", `unknown selector key "bogus"`},
		{"since=garbage", "since:"},
		{"until=garbage", "until:"},
	}
	for _, tc := range cases {
		_, err := ParseSelector(tc.in, now)
		if err == nil {
			t.Errorf("%q: want error, got nil", tc.in)
			continue
		}
		if !strings.Contains(err.Error(), tc.errSubst) {
			t.Errorf("%q: error %q missing %q", tc.in, err.Error(), tc.errSubst)
		}
	}
}

// TestSelectorToListOpts asserts the wire-shape mapping: every field the
// daemon understands round-trips, including MissionPrefix. Kind is always
// "mission" — bulk selectors must never select exec records (those are
// managed individually via `ctl exec`).
func TestSelectorToListOpts(t *testing.T) {
	s := Selector{
		Status:        "done",
		Outcome:       "success",
		Lane:          "normal",
		Mission:       "Demo",
		MissionPrefix: "De",
		SinceMs:       1714600000123,
		UntilMs:       1714600099999,
	}
	got := s.ToListOpts()
	want := lettsclient.ListMissionsOpts{
		Status:        "done",
		Outcome:       "success",
		Lane:          "normal",
		Mission:       "Demo",
		MissionPrefix: "De",
		Kind:          "mission",
		SinceMs:       1714600000123,
		UntilMs:       1714600099999,
	}
	if got != want {
		t.Errorf("ToListOpts mismatch:\n got=%+v\nwant=%+v", got, want)
	}
}

// TestSelectorToListOptsZeroValuePinsKind — even an empty selector (match
// everything) must stay scoped to kind=mission so an unqualified bulk
// `--selector` cannot touch exec rows.
func TestSelectorToListOptsZeroValuePinsKind(t *testing.T) {
	got := Selector{}.ToListOpts()
	if got.Kind != "mission" {
		t.Errorf("zero selector Kind=%q, want %q", got.Kind, "mission")
	}
}
