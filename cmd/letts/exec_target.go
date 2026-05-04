package main

import (
	"fmt"

	"letts/pkg/lettsconfig"
)

// execTargetFlags holds the subset of exec flags relevant for target resolution.
// Exactly one of host/match/all must be set; lane is required.
type execTargetFlags struct {
	lane  string
	host  string   // comma-separated ID list
	match []string // label strings, AND semantics
	all   bool
}

// resolveExecTargets resolves --host / --match / --all to a deduplicated
// list of dugdale IDs. Exec requires an explicit target — implicit
// auto-select via selector.match is unsafe for ad-hoc RCE. Empty result is
// treated as BadUsageError ("no dugdales matched ..."), not nil-slice
// success.
//
// getenv is threaded into ResolveHost so aliases whose value contains
// ${VAR} resolve correctly; with nil, any env-driven alias would error
// at validate-time, and the returned slice would be the user's literal
// input (alias unresolved), failing the later lookup in cfg.Dugdales.
func resolveExecTargets(cfg *lettsconfig.Config, f execTargetFlags, getenv lettsconfig.EnvLookup) ([]string, error) {
	if f.lane == "" {
		return nil, NewBadUsageError("--lane required")
	}
	pick := 0
	if f.host != "" {
		pick++
	}
	if len(f.match) > 0 {
		pick++
	}
	if f.all {
		pick++
	}
	if pick != 1 {
		return nil, NewBadUsageError("exec requires exactly one of --host, --match, --all")
	}

	if f.host != "" {
		raw := splitHosts(f.host)
		resolved := make([]string, len(raw))
		for i, id := range raw {
			real, err := lettsconfig.ResolveHost(cfg, id, getenv)
			if err != nil {
				return nil, NewBadUsageError(fmt.Sprintf("--host %s: %s", id, err))
			}
			resolved[i] = real
		}
		return resolved, nil
	}

	cands := lettsconfig.Candidates(cfg, f.lane, f.match)
	if len(cands) == 0 {
		if f.all {
			return nil, NewBadUsageError(fmt.Sprintf("no dugdales matched --all with --lane=%s", f.lane))
		}
		return nil, NewBadUsageError(fmt.Sprintf("no dugdales matched --match=%v with --lane=%s", f.match, f.lane))
	}
	ids := make([]string, len(cands))
	for i, d := range cands {
		ids[i] = d.ID
	}
	return ids, nil
}
