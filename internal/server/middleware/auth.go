// Package middleware contains composable HTTP middleware for the dugdale server.
package middleware

import (
	"context"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"

	"letts/internal/metrics"
	"letts/internal/server/httputil"
)

// Scope identifies a capability level required by an endpoint.
type Scope int

const (
	ScopeDispatch Scope = iota
	ScopeAdmin
)

// Identity is attached to the request context after successful auth.
type Identity struct {
	Scope Scope
}

type ctxKey struct{}

// FromCtx returns the Identity stored in ctx, or zero value and false.
func FromCtx(ctx context.Context) (Identity, bool) {
	id, ok := ctx.Value(ctxKey{}).(Identity)
	return id, ok
}

// AuthConfig carries the active token sets per scope and (optionally) the
// brute-force tracker that protects admin endpoints.
type AuthConfig struct {
	Dispatch []string
	Admin    []string

	// BruteForce, when non-nil, enforces exponential backoff on auth
	// failures for admin-scoped endpoints. Disabled for ScopeDispatch
	// (matches IncAdminAuthFailure counter scoping — the operator signal
	// pair is consistent; a busy CI clobbering its own dispatch token
	// shouldn't trip backoff).
	BruteForce *BruteForceTracker
	// TrustedProxies and UseXForwardedFor configure ClientIP() so the
	// tracker keys by the originating client, not a hop in front of the
	// daemon.
	TrustedProxies   []*net.IPNet
	UseXForwardedFor bool
}

// bfKey returns the brute-force tracker key for r based on the AuthConfig's
// trusted-proxies wiring. Empty string disables tracking for this request.
func (cfg AuthConfig) bfKey(r *http.Request) string {
	if cfg.BruteForce == nil {
		return ""
	}
	return ClientIP(r, cfg.TrustedProxies, cfg.UseXForwardedFor)
}

// writeBruteForceBlocked emits the 429 response when a key is within its
// backoff window. Retry-After uses ceil(seconds) so a 100ms backoff still
// rounds up to 1 (HTTP Retry-After is integer seconds).
func writeBruteForceBlocked(w http.ResponseWriter, d time.Duration) {
	seconds := int64(d.Seconds())
	if d > 0 && time.Duration(seconds)*time.Second < d {
		seconds++
	}
	if seconds < 1 {
		seconds = 1
	}
	w.Header().Set("Retry-After", strconv.FormatInt(seconds, 10))
	httputil.WriteError(w, http.StatusTooManyRequests, "rate_limited",
		"too many auth failures from this source", nil)
}

// Auth wraps h requiring the caller to present a Bearer token with at least
// the given scope. Admin tokens satisfy any required scope (superset).
//
// Response codes:
//   - 401 if no/wrong Authorization header, or token not recognised in any list
//   - 403 if token is recognised but insufficient for required scope
//   - 200+ if token matches
//
// Every 401 at an endpoint guarded by ScopeAdmin bumps the
// letts_admin_auth_failures_total counter. 401s at lower-scope
// endpoints don't — that's a separate operational signal we don't track yet.
func Auth(cfg AuthConfig, required Scope, h http.HandlerFunc) http.HandlerFunc {
	protected := required == ScopeAdmin && cfg.BruteForce != nil
	return func(w http.ResponseWriter, r *http.Request) {
		var key string
		if protected {
			key = cfg.bfKey(r)
			if d := cfg.BruteForce.CheckBlocked(key); d > 0 {
				writeBruteForceBlocked(w, d)
				return
			}
		}
		recordFail := func() {
			if protected {
				cfg.BruteForce.RecordFailure(key)
			}
		}
		hdr := r.Header.Get("Authorization")
		if hdr == "" {
			observeAdminAuthFailure(required)
			recordFail()
			httputil.WriteError(w, http.StatusUnauthorized, "unauthorized", "missing Authorization header", nil)
			return
		}
		const prefix = "Bearer "
		if !strings.HasPrefix(hdr, prefix) {
			observeAdminAuthFailure(required)
			recordFail()
			httputil.WriteError(w, http.StatusUnauthorized, "unauthorized", "expected Bearer token", nil)
			return
		}
		tok := strings.TrimSpace(hdr[len(prefix):])

		// Admin token grants all scopes (superset).
		if contains(cfg.Admin, tok) {
			if protected {
				cfg.BruteForce.RecordSuccess(key)
			}
			ctx := context.WithValue(r.Context(), ctxKey{}, Identity{Scope: ScopeAdmin})
			h.ServeHTTP(w, r.WithContext(ctx))
			return
		}

		if required == ScopeDispatch && contains(cfg.Dispatch, tok) {
			ctx := context.WithValue(r.Context(), ctxKey{}, Identity{Scope: ScopeDispatch})
			h.ServeHTTP(w, r.WithContext(ctx))
			return
		}

		// If the token appears in any other scope list → recognised but wrong scope → 403.
		// If unknown entirely → 401.
		if tokenKnown(cfg, tok) {
			httputil.WriteError(w, http.StatusForbidden, "forbidden", "token does not have required scope", nil)
		} else {
			observeAdminAuthFailure(required)
			recordFail()
			httputil.WriteError(w, http.StatusUnauthorized, "unauthorized", "unknown token", nil)
		}
	}
}

// observeAdminAuthFailure bumps the admin-auth-failures counter, but only
// when the required scope was Admin (so generic Bearer-token noise on
// dispatch endpoints stays out of the operator-attention signal).
func observeAdminAuthFailure(required Scope) {
	if required == ScopeAdmin {
		metrics.IncAdminAuthFailure()
	}
}

// tokenKnown returns true if tok appears in any of the token lists.
func tokenKnown(cfg AuthConfig, tok string) bool {
	return contains(cfg.Dispatch, tok) || contains(cfg.Admin, tok)
}

// contains does a constant-time membership check against a string slice.
func contains(set []string, v string) bool {
	if v == "" {
		return false
	}
	for _, s := range set {
		if constantTimeEq(s, v) {
			return true
		}
	}
	return false
}

// constantTimeEq compares two strings in constant time relative to their length.
func constantTimeEq(a, b string) bool {
	if len(a) != len(b) {
		return false
	}
	var x byte
	for i := 0; i < len(a); i++ {
		x |= a[i] ^ b[i]
	}
	return x == 0
}
