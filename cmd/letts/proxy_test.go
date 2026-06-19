package main

import (
	"net/http/httptest"
	"testing"

	"letts/pkg/lettsconfig"
)

// TestClientForHostAppliesProxyAndIgnoreProxySkipsIt proves the proxy is
// actually threaded into the client when honored, and skipped under
// --ignore-proxy. The dugdale's proxy is given a deliberately invalid scheme
// (the Config is built directly, bypassing load-time validation) so that
// lettsclient.New rejects it ONLY when the proxy is actually applied — making
// the flag's effect observable without a live SOCKS5 server.
func TestClientForHostAppliesProxyAndIgnoreProxySkipsIt(t *testing.T) {
	srv := httptest.NewServer(nil)
	defer srv.Close()

	mk := func(ignore bool) *appCtx {
		return &appCtx{
			Config: &lettsconfig.Config{Dugdales: []lettsconfig.Dugdale{
				{ID: "s1", Host: "x", Token: "t", Proxy: "bogus://nope"},
			}},
			Getenv:       func(k string) (string, bool) { return "", false },
			BaseURLForID: map[string]string{"s1": srv.URL},
			clients:      map[clientKey]*hostClient{},
			IgnoreProxy:  ignore,
		}
	}

	if _, err := mk(false).ClientForHost("s1", lettsconfig.ScopeDispatch); err == nil {
		t.Error("expected a proxy scheme error when the proxy is honored")
	}
	if _, err := mk(true).ClientForHost("s1", lettsconfig.ScopeDispatch); err != nil {
		t.Errorf("--ignore-proxy must skip the proxy: %v", err)
	}
}
