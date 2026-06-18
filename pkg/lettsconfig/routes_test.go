package lettsconfig

import "testing"

func TestResolveHostDirect(t *testing.T) {
	c := &Config{Dugdales: []Dugdale{{ID: "s1", Host: "h"}}}
	got, err := ResolveHost(c, "s1", envFromMap(nil))
	if err != nil {
		t.Fatal(err)
	}
	if got != "s1" {
		t.Errorf("got %q", got)
	}
}

func TestResolveHostViaAlias(t *testing.T) {
	c := &Config{
		Aliases:  map[string]string{"local": "s7"},
		Dugdales: []Dugdale{{ID: "s7", Host: "h"}},
	}
	got, err := ResolveHost(c, "local", envFromMap(nil))
	if err != nil {
		t.Fatal(err)
	}
	if got != "s7" {
		t.Errorf("got %q", got)
	}
}

func TestResolveHostViaNumericAlias(t *testing.T) {
	// A numeric alias key (e.g. `5: srv5`) must resolve when the host is
	// passed as "5". The host arrives as a plain string from argv and is used
	// verbatim as the map key, so no integer coercion is involved.
	c := &Config{
		Aliases:  map[string]string{"5": "srv5"},
		Dugdales: []Dugdale{{ID: "srv5", Host: "h"}},
	}
	if err := Validate(c); err != nil {
		t.Fatalf("Validate rejected a numeric alias key: %v", err)
	}
	got, err := ResolveHost(c, "5", envFromMap(nil))
	if err != nil {
		t.Fatal(err)
	}
	if got != "srv5" {
		t.Errorf("got %q, want %q", got, "srv5")
	}
}

func TestResolveHostAliasEnvSubst(t *testing.T) {
	c := &Config{
		Aliases:  map[string]string{"local": "${LOCAL_HOST}"},
		Dugdales: []Dugdale{{ID: "s7", Host: "h"}},
	}
	got, err := ResolveHost(c, "local", envFromMap(map[string]string{"LOCAL_HOST": "s7"}))
	if err != nil {
		t.Fatal(err)
	}
	if got != "s7" {
		t.Errorf("got %q", got)
	}
}

func TestResolveHostAliasCycle(t *testing.T) {
	c := &Config{
		Aliases: map[string]string{
			"a": "b",
			"b": "c",
			"c": "a",
		},
	}
	if _, err := ResolveHost(c, "a", envFromMap(nil)); err == nil {
		t.Fatal("expected cycle error")
	}
}

func TestResolveHostAliasSelfReference(t *testing.T) {
	c := &Config{Aliases: map[string]string{"a": "a"}}
	if _, err := ResolveHost(c, "a", envFromMap(nil)); err == nil {
		t.Fatal("expected self-reference error")
	}
}

func TestResolveHostUnknown(t *testing.T) {
	c := &Config{Dugdales: []Dugdale{{ID: "s1", Host: "h"}}}
	if _, err := ResolveHost(c, "missing", envFromMap(nil)); err == nil {
		t.Fatal("expected unknown-host error")
	}
}

func TestResolveRoute(t *testing.T) {
	c := &Config{
		Routes:   map[string]Route{"normal": {Host: "local", Lane: "normal"}},
		Aliases:  map[string]string{"local": "s7"},
		Dugdales: []Dugdale{{ID: "s7", Host: "h"}},
	}
	host, lane, err := ResolveRoute(c, "normal", envFromMap(nil))
	if err != nil {
		t.Fatal(err)
	}
	if host != "s7" || lane != "normal" {
		t.Errorf("got %q/%q want s7/normal", host, lane)
	}
}

func TestResolveRouteUnknown(t *testing.T) {
	c := &Config{}
	if _, _, err := ResolveRoute(c, "missing", envFromMap(nil)); err == nil {
		t.Fatal("expected unknown-route error")
	}
}
