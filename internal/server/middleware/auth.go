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
	ScopeExec
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
// brute-force tracker that protects admin/exec endpoints.
type AuthConfig struct {
	Dispatch []string
	Exec     []string
	Admin    []string

	// BruteForce, when non-nil, enforces exponential backoff on auth
	// failures for endpoints whose required/allowed scope includes Admin
	// or Exec. Disabled for ScopeDispatch (matches IncAdminAuthFailure
	// counter scoping — the operator signal pair is consistent).
	BruteForce *BruteForceTracker
	// TrustedProxies and UseXForwardedFor configure ClientIP() so the
	// tracker keys by the originating client, not a hop in front of the
	// daemon.
	TrustedProxies   []*net.IPNet
	UseXForwardedFor bool
}

// scopeIncludesProtected reports whether the brute-force tracker should
// gate this scope/endpoint. Admin/exec get protection; pure dispatch does
// not (a busy CI clobbering its own dispatch token shouldn't trip backoff).
func scopeIncludesProtected(required Scope, allowed []Scope) bool {
	if required == ScopeAdmin || required == ScopeExec {
		return true
	}
	for _, s := range allowed {
		if s == ScopeAdmin || s == ScopeExec {
			return true
		}
	}
	return false
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
	protected := scopeIncludesProtected(required, nil) && cfg.BruteForce != nil
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

		// Check scope-specific token list.
		switch required {
		case ScopeDispatch:
			if contains(cfg.Dispatch, tok) {
				ctx := context.WithValue(r.Context(), ctxKey{}, Identity{Scope: ScopeDispatch})
				h.ServeHTTP(w, r.WithContext(ctx))
				return
			}
		case ScopeExec:
			if contains(cfg.Exec, tok) {
				if protected {
					cfg.BruteForce.RecordSuccess(key)
				}
				ctx := context.WithValue(r.Context(), ctxKey{}, Identity{Scope: ScopeExec})
				h.ServeHTTP(w, r.WithContext(ctx))
				return
			}
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

// AuthEither accepts a bearer matching ANY of the listed scopes. Admin tokens
// always pass (superset). On success the request's Identity scope is set to
// the matched scope. Unknown tokens → 401, recognised-but-wrong-scope → 403.
// Uses the same constant-time comparison as Auth.
//
// The admin-auth-failures counter is bumped on 401 only when ScopeAdmin
// appears in the allowed slice — endpoints that merely accept admin as a
// superset (e.g. AuthEither with [Dispatch, Exec]) aren't admin-scoped and
// shouldn't pollute that operator signal.
func AuthEither(cfg AuthConfig, allowed []Scope, h http.HandlerFunc) http.HandlerFunc {
	protected := scopeIncludesProtected(0, allowed) && cfg.BruteForce != nil
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
			observeAdminAuthFailureFor(allowed)
			recordFail()
			httputil.WriteError(w, http.StatusUnauthorized, "unauthorized", "missing Authorization header", nil)
			return
		}
		const prefix = "Bearer "
		if !strings.HasPrefix(hdr, prefix) {
			observeAdminAuthFailureFor(allowed)
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
		// Try each allowed scope in order.
		for _, s := range allowed {
			switch s {
			case ScopeDispatch:
				if contains(cfg.Dispatch, tok) {
					// Legitimate dispatch traffic must reset
					// the brute-force counter on a protected endpoint,
					// otherwise a stray failed-auth burst from a shared
					// NAT egress slowly starves dispatch traffic that
					// would have otherwise cleared the counter.
					if protected {
						cfg.BruteForce.RecordSuccess(key)
					}
					ctx := context.WithValue(r.Context(), ctxKey{}, Identity{Scope: ScopeDispatch})
					h.ServeHTTP(w, r.WithContext(ctx))
					return
				}
			case ScopeExec:
				if contains(cfg.Exec, tok) {
					if protected {
						cfg.BruteForce.RecordSuccess(key)
					}
					ctx := context.WithValue(r.Context(), ctxKey{}, Identity{Scope: ScopeExec})
					h.ServeHTTP(w, r.WithContext(ctx))
					return
				}
			case ScopeAdmin:
				// Already handled above as a superset; nothing to do here.
			}
		}
		// Token unknown or token recognised but not in any allowed scope.
		if tokenKnown(cfg, tok) {
			httputil.WriteError(w, http.StatusForbidden, "forbidden", "token does not have required scope", nil)
		} else {
			observeAdminAuthFailureFor(allowed)
			recordFail()
			httputil.WriteError(w, http.StatusUnauthorized, "unauthorized", "unknown token", nil)
		}
	}
}

// IdentityCtxKey returns the sentinel key used to attach Identity to a
// request context. Public so test setup can inject identities without
// going through Auth() / AuthEither().
func IdentityCtxKey() any { return ctxKey{} }

// observeAdminAuthFailure bumps the admin-auth-failures counter, but only
// when the required scope was Admin (so generic Bearer-token noise on
// dispatch endpoints stays out of the operator-attention signal).
func observeAdminAuthFailure(required Scope) {
	if required == ScopeAdmin {
		metrics.IncAdminAuthFailure()
	}
}

// observeAdminAuthFailureFor mirrors observeAdminAuthFailure for the
// AuthEither path: bump the counter when ScopeAdmin is one of the allowed
// scopes (so the endpoint genuinely is admin-scoped, even if it also
// tolerates lower scopes). Otherwise admin tokens passing as a superset
// don't make the endpoint admin-attention-worthy.
func observeAdminAuthFailureFor(allowed []Scope) {
	for _, s := range allowed {
		if s == ScopeAdmin {
			metrics.IncAdminAuthFailure()
			return
		}
	}
}

// tokenKnown returns true if tok appears in any of the token lists.
func tokenKnown(cfg AuthConfig, tok string) bool {
	return contains(cfg.Dispatch, tok) || contains(cfg.Exec, tok) || contains(cfg.Admin, tok)
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
