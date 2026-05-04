package lettsconfig

import (
	"errors"
	"strings"
	"testing"
)

func envFromMap(m map[string]string) EnvLookup {
	return func(k string) (string, bool) {
		v, ok := m[k]
		return v, ok
	}
}

// TestResolveTokenEmptyEnvIsError: a ${VAR} that resolves to an empty
// string (env var set but empty — a common ops mistake) must be a clear error,
// not a silently-empty token. An empty token makes the client omit the
// Authorization header and the request fails with a confusing server-side 401.
func TestResolveTokenEmptyEnvIsError(t *testing.T) {
	c := &Config{
		Auth:     Auth{Token: "${LETTS_DISPATCH_TOKEN}"},
		Dugdales: []Dugdale{{ID: "s1", Host: "h"}},
	}
	_, err := ResolveToken(c, "s1", ScopeDispatch,
		envFromMap(map[string]string{"LETTS_DISPATCH_TOKEN": ""}))
	if err == nil {
		t.Fatal("expected error for empty-resolved token")
	}
	if !strings.Contains(err.Error(), "empty") {
		t.Errorf("error = %q, want mention of 'empty'", err.Error())
	}
}

func TestResolveDispatchTokenFromDugdale(t *testing.T) {
	c := &Config{
		Auth:     Auth{Token: "global-disp"},
		Dugdales: []Dugdale{{ID: "s1", Host: "h", Token: "own-disp"}},
	}
	tok, err := ResolveToken(c, "s1", ScopeDispatch, envFromMap(nil))
	if err != nil {
		t.Fatal(err)
	}
	if tok != "own-disp" {
		t.Errorf("got %q want own-disp", tok)
	}
}

func TestResolveDispatchTokenFromGlobal(t *testing.T) {
	c := &Config{
		Auth:     Auth{Token: "global-disp"},
		Dugdales: []Dugdale{{ID: "s1", Host: "h"}},
	}
	tok, err := ResolveToken(c, "s1", ScopeDispatch, envFromMap(nil))
	if err != nil {
		t.Fatal(err)
	}
	if tok != "global-disp" {
		t.Errorf("got %q want global-disp", tok)
	}
}

func TestResolveDispatchTokenMissingErrors(t *testing.T) {
	c := &Config{Dugdales: []Dugdale{{ID: "s1", Host: "h"}}}
	_, err := ResolveToken(c, "s1", ScopeDispatch, envFromMap(nil))
	if err == nil {
		t.Fatal("expected error when no dispatch token resolves")
	}
}

func TestResolveAdminTokenMissingIsErr(t *testing.T) {
	c := &Config{Dugdales: []Dugdale{{ID: "s1", Host: "h"}}}
	_, err := ResolveToken(c, "s1", ScopeAdmin, envFromMap(nil))
	if err == nil {
		t.Fatal("expected error for admin scope with no admin token configured")
	}
}

func TestResolveTokenEnvSubstitution(t *testing.T) {
	c := &Config{
		Auth:     Auth{Token: "${TOK}"},
		Dugdales: []Dugdale{{ID: "s1", Host: "h"}},
	}
	tok, err := ResolveToken(c, "s1", ScopeDispatch, envFromMap(map[string]string{"TOK": "resolved-disp"}))
	if err != nil {
		t.Fatal(err)
	}
	if tok != "resolved-disp" {
		t.Errorf("got %q", tok)
	}
}

func TestResolveTokenUnknownDugdale(t *testing.T) {
	c := &Config{Dugdales: []Dugdale{{ID: "s1", Host: "h", Token: "x"}}}
	_, err := ResolveToken(c, "unknown", ScopeDispatch, envFromMap(nil))
	if err == nil {
		t.Fatal("expected error for unknown dugdale id")
	}
}

func TestResolveTokenMissingEnvSurfaces(t *testing.T) {
	c := &Config{Dugdales: []Dugdale{{ID: "s1", Host: "h", Token: "${UNSET}"}}}
	_, err := ResolveToken(c, "s1", ScopeDispatch, envFromMap(nil))
	var me *MissingEnvError
	if !errors.As(err, &me) {
		t.Fatalf("expected MissingEnvError, got %v", err)
	}
}

func TestResolveExecTokenFromDugdale(t *testing.T) {
	c := &Config{
		Auth:     Auth{ExecToken: "global-exec"},
		Dugdales: []Dugdale{{ID: "s1", Host: "h", ExecToken: "own-exec"}},
	}
	tok, err := ResolveToken(c, "s1", ScopeExec, envFromMap(nil))
	if err != nil {
		t.Fatal(err)
	}
	if tok != "own-exec" {
		t.Errorf("got %q want own-exec", tok)
	}
}

func TestResolveExecTokenFromGlobal(t *testing.T) {
	c := &Config{
		Auth:     Auth{ExecToken: "global-exec"},
		Dugdales: []Dugdale{{ID: "s1", Host: "h"}},
	}
	tok, err := ResolveToken(c, "s1", ScopeExec, envFromMap(nil))
	if err != nil {
		t.Fatal(err)
	}
	if tok != "global-exec" {
		t.Errorf("got %q want global-exec", tok)
	}
}

func TestResolveExecTokenMissingIsErr(t *testing.T) {
	c := &Config{Dugdales: []Dugdale{{ID: "s1", Host: "h"}}}
	_, err := ResolveToken(c, "s1", ScopeExec, envFromMap(nil))
	if err == nil {
		t.Fatal("expected error for exec scope with no exec token configured")
	}
}

func TestResolveExecTokenEnvSubstitution(t *testing.T) {
	c := &Config{
		Dugdales: []Dugdale{{ID: "s1", Host: "h", ExecToken: "${X}"}},
	}
	tok, err := ResolveToken(c, "s1", ScopeExec, envFromMap(map[string]string{"X": "resolved-exec"}))
	if err != nil {
		t.Fatal(err)
	}
	if tok != "resolved-exec" {
		t.Errorf("got %q", tok)
	}
}

// TestScopeString ensures every scope (including ScopeExec) has the
// correct human-readable name used in error messages.
func TestScopeString(t *testing.T) {
	cases := map[Scope]string{
		ScopeDispatch: "dispatch",
		ScopeAdmin:    "admin",
		ScopeExec:     "exec",
	}
	for s, want := range cases {
		if got := s.String(); got != want {
			t.Errorf("Scope(%d).String() = %q, want %q", s, got, want)
		}
	}
}
