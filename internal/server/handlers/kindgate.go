package handlers

import (
	"context"
	"net/http"

	"letts/internal/server/httputil"
	"letts/internal/server/middleware"
	"letts/internal/storage"
)

// RequireKindForScope enforces kind isolation: dispatch tokens may
// access only kind='mission' missions, exec tokens may access only kind='exec'
// missions, and admin tokens may access anything. Returns true when access is
// permitted; on denial it writes 403 forbidden_kind and returns false (caller
// should return immediately).
//
// Callers must run this AFTER any 404/410 status checks so that the existence
// (or pseudo-non-existence for deleting rows) of a record stays uncorrelated
// with the caller's scope.
func RequireKindForScope(w http.ResponseWriter, r *http.Request, m *storage.Mission) bool {
	allowed, missingIdentity := kindAllowedForCtx(r.Context(), m)
	if missingIdentity {
		httputil.WriteError(w, http.StatusInternalServerError, "internal", "missing identity", nil)
		return false
	}
	if allowed {
		return true
	}
	httputil.WriteError(w, http.StatusForbidden, "forbidden_kind",
		"token scope does not permit access to this mission kind", nil)
	return false
}

// kindAllowedForCtx is the pure-ctx sibling of RequireKindForScope: it
// returns whether the caller's scope (looked up from ctx) is allowed to
// touch m.Kind. The second return is true when the request had no
// Identity at all (handler-chain misconfiguration); callers should treat
// that as 500 internal. Used by per-id bulk loops that need an outcome
// without writing directly to the response.
func kindAllowedForCtx(ctx context.Context, m *storage.Mission) (allowed, missingIdentity bool) {
	id, ok := middleware.FromCtx(ctx)
	if !ok {
		return false, true
	}
	switch id.Scope {
	case middleware.ScopeAdmin:
		return true, false
	case middleware.ScopeDispatch:
		if m.Kind == storage.KindMission {
			return true, false
		}
	case middleware.ScopeExec:
		if m.Kind == storage.KindExec {
			return true, false
		}
	}
	return false, false
}

// gateKindForMission returns a non-zero opOutcome when ctx's scope is not
// permitted to act on m.Kind. Returns the zero opOutcome (Status=0) when
// access is allowed. The Status field can be inspected to branch in
// restartOne/deleteOne and translate into either an HTTP error response
// (single-mission handler) or a per-id bulk-result entry (bulk handler).
func gateKindForMission(ctx context.Context, m *storage.Mission) opOutcome {
	allowed, missing := kindAllowedForCtx(ctx, m)
	if missing {
		return opOutcome{Status: http.StatusInternalServerError, ErrorCode: "internal",
			ErrorMsg: "missing identity"}
	}
	if allowed {
		return opOutcome{}
	}
	return opOutcome{Status: http.StatusForbidden, ErrorCode: "forbidden_kind",
		ErrorMsg: "token scope does not permit access to this mission kind"}
}
