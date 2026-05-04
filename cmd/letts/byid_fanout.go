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

// FanOutByID is the admin-scoped variant of FanOutByIDForScope. It runs fn
// on EVERY candidate host concurrently, so it is only safe for idempotent
// reads (it backs the locate step of LocateThenActByID); for read operations
// available to non-admin operators prefer FanOutByIDForScope with
// ScopeDispatch or ScopeExec. Mutating operations (restart/kill/delete) must
// go through LocateThenActByID instead — fanning a mutation out to all hosts
// would execute it everywhere the id exists.
func FanOutByID[T any](ac *appCtx, match []string, fn func(*lettsclient.Client) (T, error)) (T, string, error) {
	return FanOutByIDForScope(ac, match, lettsconfig.ScopeAdmin, fn)
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

// FanOutByIDForScope is the scope-aware sibling of FanOutByID. It tries
// the requested scope first per host and falls back to ScopeAdmin only
// when the requested scope (dispatch or exec) has no token configured.
// ScopeAdmin requests never fall back — mutating commands stay admin-only.
//
// match is the label filter (--match). Empty means no filtering; non-empty
// requires the host to carry ALL listed labels.
//
// Resolution rules (same as legacy FanOutByID):
//   - 0 successes: return the last non-404 error, or "not found on any of N"
//     if every host 404'd.
//   - 1 success:   return its value and host id.
//   - 2+ successes: a mission id should be globally unique — refuse to pick
//     and return a "found on multiple hosts" conflict so the operator can
//     disambiguate with --host.
func FanOutByIDForScope[T any](ac *appCtx, match []string, scope lettsconfig.Scope, fn func(*lettsclient.Client) (T, error)) (T, string, error) {
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

	scopes := scopeChain(scope)

	results := make([]FanOutResult[T], len(hosts))
	var wg sync.WaitGroup
	for i, h := range hosts {
		i, hid := i, h.ID
		wg.Add(1)
		go func() {
			defer wg.Done()
			c, err := clientForFirstAvailableScope(ac, hid, scopes)
			if err != nil {
				results[i] = FanOutResult[T]{HostID: hid, Err: err}
				return
			}
			v, err := fn(c)
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

// scopeChain returns the resolution order for client tokens. Non-admin
// preferred scopes fall back to admin when not configured; admin requests
// never fall back (mutation must require admin).
func scopeChain(preferred lettsconfig.Scope) []lettsconfig.Scope {
	if preferred == lettsconfig.ScopeAdmin {
		return []lettsconfig.Scope{lettsconfig.ScopeAdmin}
	}
	return []lettsconfig.Scope{preferred, lettsconfig.ScopeAdmin}
}

// clientForFirstAvailableScope walks scopes in order, returning the first
// client whose token resolves. Used by FanOutByIDForScope so a CLI caller
// without an admin token can still read by id with their dispatch/exec
// token. Returns the LAST scope's error when every attempt fails so the
// surfaced message is the most-specific (admin token missing) rather
// than the preferred-scope error.
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
