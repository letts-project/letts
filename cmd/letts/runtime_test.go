package main

import (
	"net/http/httptest"
	"testing"

	"letts/pkg/lettsconfig"
)

func TestBaseURLForStripsExistingPortFromHost(t *testing.T) {
	// When an operator writes `host: server1:7180` (port
	// already baked into the host string), the previous Sprintf produced
	// `http://server1:7180:7180` which fails DNS at connect time. Strip
	// the existing :port and let it override Dugdale.Port / Defaults.Port.
	cases := []struct {
		name string
		d    lettsconfig.Dugdale
		want string
	}{
		{"plain host + port field", lettsconfig.Dugdale{ID: "s1", Host: "server1", Port: 9000}, "http://server1:9000"},
		{"host already has port", lettsconfig.Dugdale{ID: "s1", Host: "server1:7180"}, "http://server1:7180"},
		{"host port wins over port field", lettsconfig.Dugdale{ID: "s1", Host: "server1:7180", Port: 9000}, "http://server1:7180"},
		{"ipv6 + port field", lettsconfig.Dugdale{ID: "s1", Host: "::1", Port: 9000}, "http://[::1]:9000"},
		{"ipv6 with port", lettsconfig.Dugdale{ID: "s1", Host: "[::1]:7180"}, "http://[::1]:7180"},
		{"defaults port fallback", lettsconfig.Dugdale{ID: "s1", Host: "server1"}, "http://server1:7180"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ac := &appCtx{Config: &lettsconfig.Config{Dugdales: []lettsconfig.Dugdale{tc.d}}}
			got, err := ac.baseURLFor("s1")
			if err != nil {
				t.Fatalf("baseURLFor: %v", err)
			}
			if got != tc.want {
				t.Errorf("got %q want %q", got, tc.want)
			}
		})
	}
}

// TestClientForHostResolvesAlias: ctl/events/logs/staging must
// accept --host=<alias> like dispatch/run/exec do. ClientForHost("local") must
// resolve the alias to the real dugdale id before the URL and token lookup.
func TestClientForHostResolvesAlias(t *testing.T) {
	srv := httptest.NewServer(nil)
	defer srv.Close()
	ac := &appCtx{
		Config: &lettsconfig.Config{
			Aliases:  map[string]string{"local": "s7"},
			Dugdales: []lettsconfig.Dugdale{{ID: "s7", Host: "x", Token: "t"}},
		},
		Getenv:       func(k string) (string, bool) { return "", false },
		BaseURLForID: map[string]string{"s7": srv.URL},
		clients:      map[clientKey]*hostClient{},
	}
	if _, err := ac.ClientForHost("local", lettsconfig.ScopeDispatch); err != nil {
		t.Fatalf("ClientForHost(local) should resolve alias to s7: %v", err)
	}
}

func TestAppCtxClientCachedPerHost(t *testing.T) {
	srv := httptest.NewServer(nil)
	defer srv.Close()
	ac := &appCtx{
		Config: &lettsconfig.Config{Dugdales: []lettsconfig.Dugdale{
			{ID: "s1", Host: "x", Token: "t"},
			{ID: "s2", Host: "y", Token: "t"},
		}},
		Getenv:       func(k string) (string, bool) { return "", false },
		BaseURLForID: map[string]string{"s1": srv.URL, "s2": srv.URL},
		clients:      map[clientKey]*hostClient{},
	}
	c1a, _ := ac.ClientForHost("s1", lettsconfig.ScopeDispatch)
	c1b, _ := ac.ClientForHost("s1", lettsconfig.ScopeDispatch)
	c2, _ := ac.ClientForHost("s2", lettsconfig.ScopeDispatch)
	if c1a != c1b {
		t.Error("same key must reuse client")
	}
	if c1a == c2 {
		t.Error("different hosts must be distinct")
	}
}
