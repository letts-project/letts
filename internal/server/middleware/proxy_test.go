package middleware_test

import (
	"net"
	"net/http/httptest"
	"testing"

	"letts/internal/server/middleware"
)

func mustParseCIDR(s string) *net.IPNet {
	_, n, err := net.ParseCIDR(s)
	if err != nil {
		panic(err)
	}
	return n
}

var trustedCIDRs = []*net.IPNet{
	mustParseCIDR("10.0.0.0/8"),
	mustParseCIDR("127.0.0.1/32"),
}

func TestClientIPDirectNoXFF(t *testing.T) {
	req := httptest.NewRequest("GET", "/", nil)
	req.RemoteAddr = "203.0.113.5:54321"

	ip := middleware.ClientIP(req, trustedCIDRs, false)
	if ip != "203.0.113.5" {
		t.Errorf("got %q, want 203.0.113.5", ip)
	}
}

func TestClientIPXFFFromTrustedProxy(t *testing.T) {
	req := httptest.NewRequest("GET", "/", nil)
	req.RemoteAddr = "10.0.0.1:12345" // trusted
	req.Header.Set("X-Forwarded-For", "198.51.100.42, 10.0.0.1")

	ip := middleware.ClientIP(req, trustedCIDRs, true)
	if ip != "198.51.100.42" {
		t.Errorf("got %q, want 198.51.100.42", ip)
	}
}

func TestClientIPXFFFromNonTrustedProxy(t *testing.T) {
	req := httptest.NewRequest("GET", "/", nil)
	req.RemoteAddr = "203.0.113.9:12345" // not trusted
	req.Header.Set("X-Forwarded-For", "198.51.100.42")

	ip := middleware.ClientIP(req, trustedCIDRs, true)
	// XFF ignored; return RemoteAddr host
	if ip != "203.0.113.9" {
		t.Errorf("got %q, want 203.0.113.9", ip)
	}
}

func TestClientIPMalformedXFF(t *testing.T) {
	req := httptest.NewRequest("GET", "/", nil)
	req.RemoteAddr = "10.0.0.1:12345" // trusted
	req.Header.Set("X-Forwarded-For", "not-an-ip, garbage")

	ip := middleware.ClientIP(req, trustedCIDRs, true)
	// Malformed leftmost entry → fall back to RemoteAddr
	if ip != "10.0.0.1" {
		t.Errorf("got %q, want 10.0.0.1 fallback", ip)
	}
}

func TestClientIPUseXFFDisabled(t *testing.T) {
	req := httptest.NewRequest("GET", "/", nil)
	req.RemoteAddr = "10.0.0.1:12345" // trusted, but useXFF=false
	req.Header.Set("X-Forwarded-For", "198.51.100.42")

	ip := middleware.ClientIP(req, trustedCIDRs, false)
	// useXFF=false means ignore header entirely
	if ip != "10.0.0.1" {
		t.Errorf("got %q, want 10.0.0.1 (useXFF=false)", ip)
	}
}

func TestClientIPEmptyXFF(t *testing.T) {
	req := httptest.NewRequest("GET", "/", nil)
	req.RemoteAddr = "10.0.0.1:12345" // trusted
	// No X-Forwarded-For header

	ip := middleware.ClientIP(req, trustedCIDRs, true)
	if ip != "10.0.0.1" {
		t.Errorf("got %q, want 10.0.0.1 (empty XFF)", ip)
	}
}

func TestClientIPMalformedRemoteAddr(t *testing.T) {
	req := httptest.NewRequest("GET", "/", nil)
	req.RemoteAddr = "malformed-addr" // no port

	ip := middleware.ClientIP(req, trustedCIDRs, false)
	// Should return the raw RemoteAddr when parsing fails
	if ip == "" {
		t.Error("expected non-empty result for malformed RemoteAddr")
	}
}
