package main

import (
	"errors"
	"fmt"
	"sync"

	"letts/pkg/lettsclient"
	"letts/pkg/lettsconfig"
)

// FanOutResult carries one goroutine's outcome from FanOutByID. Exposed so
// future callers can build richer aggregators on top of the generic helper
// (today only FanOutByID itself consumes it).
type FanOutResult[T any] struct {
	HostID string
	Value  T
	Err    error
}

// FanOutByID runs fn with each candidate host's admin client concurrently
// and resolves the outcomes by the rules of fanOutByIDHosts. Running on EVERY
// candidate host makes it safe only for idempotent reads (it backs the locate
// step of LocateThenActByID); reads open to non-admin tokens go through
// getMissionByID or fanOutByIDHosts with callWithKindScopes. Mutating
// operations (restart/kill/delete) must go through LocateThenActByID instead —
// fanning a mutation out to all hosts would execute it everywhere the id
// exists.
func FanOutByID[T any](ac *appCtx, match []string, fn func(*lettsclient.Client) (T, error)) (T, string, error) {
	return fanOutByIDHosts(ac, match, func(hostID string) (T, error) {
		c, err := ac.ClientForHost(hostID, lettsconfig.ScopeAdmin)
		if err != nil {
			var zero T
			return zero, err
		}
		return fn(c)
	})
}

// LocateThenActByID is the destructive-operation sibling of FanOutByID.
//
// FanOutByID runs its closure on every candidate host concurrently and only
// afterwards inspects how many succeeded. That is fine for reads but
// catastrophic for mutations: with the same id present on 2+ hosts a restart
// would create a new mission on each of them, and the CLI would then report
// a conflict implying nothing happened. Destructive commands therefore
// locate first and act second:
//
//  1. Locate: GET /v1/missions/{id} fans out across the candidate hosts
//     (honouring the --match label filter) with the standard read semantics —
//     404s are ignored; zero owners yields the not-found / joined non-404
//     error shape; 2+ owners yields the "found on multiple hosts" conflict
//     demanding --host. Nothing has been mutated at that point.
//  2. Act: fn runs against the single owning host only.
//
// Locating deliberately uses the admin scope (the scope the mutation itself
// requires) with no dispatch/exec fallback, so an operator who cannot
// perform the mutation cannot probe ids through this path either. A mutation
// 404 after a successful locate (the row expired in between) surfaces as a
// normal error from fn. fn must address the same id that was located —
// the helper cannot enforce that the closure and the locate agree.
func LocateThenActByID[T any](ac *appCtx, id string, match []string, fn func(*lettsclient.Client) (T, error)) (T, string, error) {
	var zero T
	_, hostID, err := FanOutByID(ac, match, func(c *lettsclient.Client) (*lettsclient.Mission, error) {
		return lettsclient.GetMission(c, id)
	})
	if err != nil {
		return zero, "", err
	}
	c, err := ac.ClientForHost(hostID, lettsconfig.ScopeAdmin)
	if err != nil {
		return zero, "", err
	}
	v, err := fn(c)
	return v, hostID, err
}

// fanOutByIDHosts runs fn concurrently for every dugdale carrying all match
// labels (--match; empty means every dugdale) and resolves the outcomes:
//   - 0 successes: return every non-404 error joined, or "not found on any of
//     N" if every host 404'd.
//   - 1 success:   return its value and host id.
//   - 2+ successes: a mission id should be globally unique — refuse to pick
//     and return a "found on multiple hosts" conflict so the operator can
//     disambiguate with --host.
func fanOutByIDHosts[T any](ac *appCtx, match []string, fn func(hostID string) (T, error)) (T, string, error) {
	var zero T
	hosts := ac.Config.Dugdales
	if len(match) > 0 {
		filtered := make([]lettsconfig.Dugdale, 0, len(hosts))
		for _, h := range hosts {
			ok := true
			for _, m := range match {
				if !h.HasLabel(m) {
					ok = false
					break
				}
			}
			if ok {
				filtered = append(filtered, h)
			}
		}
		hosts = filtered
	}
	if len(hosts) == 0 {
		return zero, "", fmt.Errorf("no dugdales match labels %v", match)
	}

	results := make([]FanOutResult[T], len(hosts))
	var wg sync.WaitGroup
	for i, h := range hosts {
		i, hid := i, h.ID
		wg.Add(1)
		go func() {
			defer wg.Done()
			v, err := fn(hid)
			results[i] = FanOutResult[T]{HostID: hid, Value: v, Err: err}
		}()
	}
	wg.Wait()

	var wins []FanOutResult[T]
	// Collect every non-404 error, not just one of them: with three hosts
	// each reporting a distinct problem (auth, disk, network), surfacing
	// only whichever happened to finish last would hide correlated cluster
	// failures. Prefix each with its host id for readability and
	// errors.Join them so callers can errors.As across the full set.
	var non404 []error
	for _, r := range results {
		if r.Err == nil {
			wins = append(wins, r)
			continue
		}
		var he *lettsclient.HTTPError
		if errors.As(r.Err, &he) && he.Status == 404 {
			continue
		}
		non404 = append(non404, fmt.Errorf("%s: %w", r.HostID, r.Err))
	}
	switch len(wins) {
	case 0:
		if len(non404) > 0 {
			return zero, "", errors.Join(non404...)
		}
		return zero, "", fmt.Errorf("not found on any of %d candidate dugdales", len(hosts))
	case 1:
		return wins[0].Value, wins[0].HostID, nil
	default:
		return zero, "", fmt.Errorf("mission id found on multiple hosts (%d); pass --host explicitly", len(wins))
	}
}

// clientForFirstAvailableScope returns a client for the first scope in
// scopes with a configured token, or the last scope's configuration error.
func clientForFirstAvailableScope(ac *appCtx, hostID string, scopes []lettsconfig.Scope) (*lettsclient.Client, error) {
	var lastErr error
	for _, s := range scopes {
		c, err := ac.ClientForHost(hostID, s)
		if err == nil {
			return c, nil
		}
		lastErr = err
	}
	return nil, lastErr
}

// readScopesMission and readScopesExec order the tokens tried for reads that
// any of the three scopes may serve, depending on the mission's kind.
// inspectScopes orders them for lane/daemon summaries, whose queue counts
// the daemon narrows to the token's kind unless the token is admin.
var (
	readScopesMission = []lettsconfig.Scope{lettsconfig.ScopeDispatch, lettsconfig.ScopeExec, lettsconfig.ScopeAdmin}
	readScopesExec    = []lettsconfig.Scope{lettsconfig.ScopeExec, lettsconfig.ScopeDispatch, lettsconfig.ScopeAdmin}
	inspectScopes     = []lettsconfig.Scope{lettsconfig.ScopeAdmin, lettsconfig.ScopeDispatch, lettsconfig.ScopeExec}
)

// callWithKindScopes calls fn with a client for each configured scope in
// order, moving on only while the daemon rejects the token with 403
// forbidden_kind (the token is valid but its scope may not touch this
// mission's kind). Returns the forbidden_kind error when no later scope is
// configured, else the last configuration error.
func callWithKindScopes[T any](ac *appCtx, hostID string, scopes []lettsconfig.Scope, fn func(*lettsclient.Client) (T, error)) (T, error) {
	var zero T
	var forbidden, cfgErr error
	for _, s := range scopes {
		c, err := ac.ClientForHost(hostID, s)
		if err != nil {
			cfgErr = err
			continue
		}
		v, err := fn(c)
		var he *lettsclient.HTTPError
		if errors.As(err, &he) && he.Status == 403 && he.Code == "forbidden_kind" {
			forbidden = err
			continue
		}
		return v, err
	}
	if forbidden != nil {
		return zero, forbidden
	}
	return zero, cfgErr
}
