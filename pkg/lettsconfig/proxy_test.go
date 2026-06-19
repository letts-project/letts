package lettsconfig

import (
	"errors"
	"strings"
	"testing"
)

func TestValidateProxy(t *testing.T) {
	good := []string{"", "socks5://127.0.0.1:1080", "socks5h://h:1080", "socks5h://u:p@h:1080", "${PROXY}", "socks5h://${HOST}:1080"}
	bad := []string{"http://127.0.0.1:8080", "https://h:1", "ftp://h", "socks4://h:1080", "not a url at all ::::"}
	for _, s := range good {
		if err := ValidateProxy(s); err != nil {
			t.Errorf("ValidateProxy(%q) unexpected error: %v", s, err)
		}
	}
	for _, s := range bad {
		if err := ValidateProxy(s); err == nil {
			t.Errorf("ValidateProxy(%q) want error, got nil", s)
		}
	}
}

func TestValidateSyntaxRejectsBadDugdaleProxy(t *testing.T) {
	c := &Config{Dugdales: []Dugdale{{ID: "s1", Host: "h", Proxy: "http://p:8080"}}}
	err := ValidateSyntax(c)
	if err == nil || !strings.Contains(err.Error(), "socks5") {
		t.Fatalf("want socks5 scheme error, got %v", err)
	}
}

func TestValidateSyntaxRejectsBadTemplateProxy(t *testing.T) {
	c := &Config{Templates: map[string]Template{"k": {Proxy: "ftp://x"}}}
	err := ValidateSyntax(c)
	if err == nil || !strings.Contains(err.Error(), "socks5") {
		t.Fatalf("want socks5 scheme error on template, got %v", err)
	}
}

func TestExtendsInheritsProxy(t *testing.T) {
	c := &Config{
		Templates: map[string]Template{"k": {Proxy: "socks5h://10.0.0.1:1080"}},
		Dugdales:  []Dugdale{{ID: "s1", Host: "h", Extends: "k"}},
	}
	if err := ResolveExtends(c); err != nil {
		t.Fatal(err)
	}
	if got := c.Dugdales[0].Proxy; got != "socks5h://10.0.0.1:1080" {
		t.Errorf("inherited proxy = %q", got)
	}
}

func TestExtendsProxyDugdaleOverrides(t *testing.T) {
	c := &Config{
		Templates: map[string]Template{"k": {Proxy: "socks5h://10.0.0.1:1080"}},
		Dugdales:  []Dugdale{{ID: "s1", Host: "h", Extends: "k", Proxy: "socks5h://127.0.0.1:9050"}},
	}
	if err := ResolveExtends(c); err != nil {
		t.Fatal(err)
	}
	if got := c.Dugdales[0].Proxy; got != "socks5h://127.0.0.1:9050" {
		t.Errorf("overridden proxy = %q", got)
	}
}

func TestResolveProxyEmptyIsNotAnError(t *testing.T) {
	c := &Config{Dugdales: []Dugdale{{ID: "s1", Host: "h"}}}
	p, err := ResolveProxy(c, "s1", envFromMap(nil))
	if err != nil {
		t.Fatalf("empty proxy must not error: %v", err)
	}
	if p != "" {
		t.Errorf("want empty proxy, got %q", p)
	}
}

func TestResolveProxyEnvSubstitution(t *testing.T) {
	c := &Config{Dugdales: []Dugdale{{ID: "s1", Host: "h", Proxy: "socks5h://${PHOST}:1080"}}}
	p, err := ResolveProxy(c, "s1", envFromMap(map[string]string{"PHOST": "10.0.0.9"}))
	if err != nil {
		t.Fatal(err)
	}
	if p != "socks5h://10.0.0.9:1080" {
		t.Errorf("got %q", p)
	}
}

func TestResolveProxyMissingEnvErrors(t *testing.T) {
	c := &Config{Dugdales: []Dugdale{{ID: "s1", Host: "h", Proxy: "socks5h://${PHOST}:1080"}}}
	_, err := ResolveProxy(c, "s1", envFromMap(nil))
	var me *MissingEnvError
	if !errors.As(err, &me) {
		t.Fatalf("want MissingEnvError, got %v", err)
	}
}

func TestLoadParsesProxy(t *testing.T) {
	yaml := []byte("dugdales:\n  - id: s1\n    host: h\n    proxy: \"socks5h://127.0.0.1:1080\"\n")
	c, err := Load(yaml)
	if err != nil {
		t.Fatal(err)
	}
	if got := c.Dugdales[0].Proxy; got != "socks5h://127.0.0.1:1080" {
		t.Errorf("parsed proxy = %q", got)
	}
}
