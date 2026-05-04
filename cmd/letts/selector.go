package main

import (
	"fmt"
	"strings"
	"time"

	"letts/pkg/lettsclient"
)

// Selector is the parsed form of a `--selector` flag value.
// Zero values mean "no filter" — the caller maps populated fields to a
// ListMissionsOpts before issuing GET /v1/missions.
//
// MissionPrefix narrows the listing to mission names starting with the given
// prefix (distinct from Mission, which is a substring match).
type Selector struct {
	Status        string
	Outcome       string
	Lane          string
	Mission       string
	MissionPrefix string
	SinceMs       int64
	UntilMs       int64
}

// ParseSelector parses a `key=value[,key=value...]` string. Empty input
// yields a zero Selector with no error so callers can treat "no --selector"
// uniformly. `now` is injected so tests don't depend on wall clock.
//
// Supported keys: status, outcome, lane, mission, mission_prefix, since,
// until. The since/until values follow parseSinceTime semantics (relative
// `-1h`/`-30m`/`-7d` or absolute Unix milliseconds).
func ParseSelector(s string, now time.Time) (Selector, error) {
	var sel Selector
	if s == "" {
		return sel, nil
	}
	for _, kv := range strings.Split(s, ",") {
		i := strings.IndexByte(kv, '=')
		if i < 1 {
			return sel, fmt.Errorf("bad selector pair %q", kv)
		}
		key, val := kv[:i], kv[i+1:]
		switch key {
		case "status":
			sel.Status = val
		case "outcome":
			sel.Outcome = val
		case "lane":
			sel.Lane = val
		case "mission":
			sel.Mission = val
		case "mission_prefix":
			sel.MissionPrefix = val
		case "since":
			ms, err := parseSinceTime(val, now)
			if err != nil {
				return sel, fmt.Errorf("since: %w", err)
			}
			sel.SinceMs = ms
		case "until":
			ms, err := parseSinceTime(val, now)
			if err != nil {
				return sel, fmt.Errorf("until: %w", err)
			}
			sel.UntilMs = ms
		default:
			return sel, fmt.Errorf("unknown selector key %q", key)
		}
	}
	return sel, nil
}

// ToListOpts converts a Selector into the wire-shaped ListMissionsOpts.
//
// Kind is pinned to "mission": the only consumers of ToListOpts are the bulk
// restart/delete runners, and bulk selectors operate on named missions only —
// exec records share the same id namespace but are managed individually via
// `ctl exec` (which has no --selector by design). Without the pin the daemon
// would return BOTH kinds for an unqualified selector, so a bulk restart
// would re-execute ad-hoc exec commands and a bulk delete would wipe exec
// history as collateral.
func (s Selector) ToListOpts() lettsclient.ListMissionsOpts {
	return lettsclient.ListMissionsOpts{
		Status:        s.Status,
		Outcome:       s.Outcome,
		Lane:          s.Lane,
		Mission:       s.Mission,
		MissionPrefix: s.MissionPrefix,
		Kind:          "mission",
		SinceMs:       s.SinceMs,
		UntilMs:       s.UntilMs,
	}
}
