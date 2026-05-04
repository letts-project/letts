package handlers_test

import (
	"context"
	"net/http/httptest"
	"strings"
	"testing"

	"letts/internal/server/handlers"
	"letts/internal/server/middleware"
	"letts/internal/storage"
)

// TestRequireKindForScope exercises the kind-vs-scope matrix:
// dispatch-tokens may only touch kind='mission', exec-tokens may only touch
// kind='exec', and admin-tokens see anything. Mismatch → 403 forbidden_kind.
func TestRequireKindForScope(t *testing.T) {
	cases := []struct {
		name     string
		scope    middleware.Scope
		kind     storage.Kind
		wantPass bool
		wantCode int
	}{
		{"admin+mission", middleware.ScopeAdmin, storage.KindMission, true, 200},
		{"admin+exec", middleware.ScopeAdmin, storage.KindExec, true, 200},
		{"dispatch+mission", middleware.ScopeDispatch, storage.KindMission, true, 200},
		{"dispatch+exec", middleware.ScopeDispatch, storage.KindExec, false, 403},
		{"exec+mission", middleware.ScopeExec, storage.KindMission, false, 403},
		{"exec+exec", middleware.ScopeExec, storage.KindExec, true, 200},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.WithValue(context.Background(),
				middleware.IdentityCtxKey(),
				middleware.Identity{Scope: tc.scope})
			req := httptest.NewRequest("GET", "/x", nil).WithContext(ctx)
			rec := httptest.NewRecorder()
			m := &storage.Mission{Kind: tc.kind}
			got := handlers.RequireKindForScope(rec, req, m)
			if got != tc.wantPass {
				t.Errorf("got pass=%v, want %v", got, tc.wantPass)
			}
			// On pass, RequireKindForScope must NOT write a response — the
			// caller chooses what to send. Recorder default is 200 (untouched).
			if tc.wantPass && rec.Code != 200 {
				t.Errorf("pass: code=%d, want 200 (untouched)", rec.Code)
			}
			if !tc.wantPass && rec.Code != tc.wantCode {
				t.Errorf("denied: code=%d, want %d", rec.Code, tc.wantCode)
			}
		})
	}
}

// TestRequireKindForScopeMissingIdentity verifies that when the request
// context lacks an Identity (handler chain bug) we 500 rather than crash.
func TestRequireKindForScopeMissingIdentity(t *testing.T) {
	req := httptest.NewRequest("GET", "/x", nil)
	rec := httptest.NewRecorder()
	m := &storage.Mission{Kind: storage.KindMission}
	if handlers.RequireKindForScope(rec, req, m) {
		t.Errorf("expected false on missing identity")
	}
	if rec.Code != 500 {
		t.Errorf("code=%d, want 500", rec.Code)
	}
}

// TestRequireKindForScopeBodyContainsForbiddenKind verifies the 403 body
// uses the `forbidden_kind` error code.
func TestRequireKindForScopeBodyContainsForbiddenKind(t *testing.T) {
	ctx := context.WithValue(context.Background(),
		middleware.IdentityCtxKey(),
		middleware.Identity{Scope: middleware.ScopeDispatch})
	req := httptest.NewRequest("GET", "/x", nil).WithContext(ctx)
	rec := httptest.NewRecorder()
	m := &storage.Mission{Kind: storage.KindExec}
	handlers.RequireKindForScope(rec, req, m)
	if rec.Code != 403 {
		t.Fatalf("code=%d, want 403", rec.Code)
	}
	body := rec.Body.String()
	if !strings.Contains(body, "forbidden_kind") {
		t.Errorf("body=%q, expected to contain 'forbidden_kind'", body)
	}
}
