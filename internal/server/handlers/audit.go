package handlers

import (
	"log/slog"
	"net/http"

	"letts/internal/server/middleware"
)

// auditLog emits a single structured audit-tagged log line for an admin
// or exec lifecycle action. All lifecycle
// endpoints (pause / continue / kill / restart / delete / apply) MUST
// produce one of these so operators grepping for audit:true see every
// privileged mutation.
//
// Caller should pass the same `action` string used in examples
// (e.g. "lane.pause", "mission.kill", "admin.apply") and any extra
// fields specific to the action (e.g. lane name, mission_id). Pass nil
// extras when there's nothing to add.
//
// remote_addr is extracted from r.RemoteAddr verbatim — proxy-aware IP
// resolution is the BruteForceTracker's job; the audit log records the
// raw transport endpoint for ground-truth correlation with kernel logs.
//
// When an X-Forwarded-For header is present the audit line also carries
// a forwarded_for field (the raw header value, not a parsed IP). An
// operator-facing log needs the actual client IP for incident
// response, not the upstream proxy address. Trust decisions stay with
// BruteForceTracker via cfg.TrustedProxies — this field is informational.
func auditLog(logger *slog.Logger, r *http.Request, action string, extras ...any) {
	if logger == nil {
		logger = slog.Default()
	}
	actor := "anonymous"
	if id, ok := middleware.FromCtx(r.Context()); ok {
		switch id.Scope {
		case middleware.ScopeAdmin:
			actor = "admin-token"
		case middleware.ScopeExec:
			actor = "exec-token"
		case middleware.ScopeDispatch:
			actor = "dispatch-token"
		}
	}
	base := []any{
		"audit", true,
		"action", action,
		"actor", actor,
		"remote_addr", r.RemoteAddr,
	}
	if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
		base = append(base, "forwarded_for", xff)
	}
	base = append(base, extras...)
	logger.Info(action, base...)
}
