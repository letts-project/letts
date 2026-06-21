package lettsclient

import (
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync/atomic"
	"testing"
)

// startSOCKS5 stands up a minimal SOCKS5 CONNECT proxy on a loopback port for
// the test. It accepts the no-authentication method, handles CONNECT with
// IPv4 / domain / IPv6 targets, and relays bytes. The returned counter records
// how many CONNECTs it served, so a test can assert traffic went through it.
func startSOCKS5(t *testing.T) (addr string, connects *int32) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	var count int32
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go serveSOCKS5(conn, &count)
		}
	}()
	t.Cleanup(func() { _ = ln.Close() })
	return ln.Addr().String(), &count
}

func serveSOCKS5(c net.Conn, count *int32) {
	defer func() { _ = c.Close() }()
	hdr := make([]byte, 2)
	if _, err := io.ReadFull(c, hdr); err != nil || hdr[0] != 0x05 {
		return
	}
	methods := make([]byte, int(hdr[1]))
	if _, err := io.ReadFull(c, methods); err != nil {
		return
	}
	if _, err := c.Write([]byte{0x05, 0x00}); err != nil { // no-auth
		return
	}
	req := make([]byte, 4)
	if _, err := io.ReadFull(c, req); err != nil || req[1] != 0x01 { // CONNECT only
		return
	}
	var host string
	switch req[3] {
	case 0x01:
		b := make([]byte, 4)
		if _, err := io.ReadFull(c, b); err != nil {
			return
		}
		host = net.IP(b).String()
	case 0x03:
		l := make([]byte, 1)
		if _, err := io.ReadFull(c, l); err != nil {
			return
		}
		b := make([]byte, int(l[0]))
		if _, err := io.ReadFull(c, b); err != nil {
			return
		}
		host = string(b)
	case 0x04:
		b := make([]byte, 16)
		if _, err := io.ReadFull(c, b); err != nil {
			return
		}
		host = net.IP(b).String()
	default:
		return
	}
	pb := make([]byte, 2)
	if _, err := io.ReadFull(c, pb); err != nil {
		return
	}
	port := int(pb[0])<<8 | int(pb[1])
	atomic.AddInt32(count, 1)
	up, err := net.Dial("tcp", net.JoinHostPort(host, strconv.Itoa(port)))
	if err != nil {
		_, _ = c.Write([]byte{0x05, 0x01, 0x00, 0x01, 0, 0, 0, 0, 0, 0})
		return
	}
	defer func() { _ = up.Close() }()
	if _, err := c.Write([]byte{0x05, 0x00, 0x00, 0x01, 0, 0, 0, 0, 0, 0}); err != nil {
		return
	}
	go func() { _, _ = io.Copy(up, c) }()
	_, _ = io.Copy(c, up)
}

func TestClientRoutesThroughSOCKS5Proxy(t *testing.T) {
	var hit int32
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&hit, 1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"ok":true}`)
	}))
	defer backend.Close()

	socksAddr, connects := startSOCKS5(t)
	c, err := New(Options{BaseURL: backend.URL, ProxyURL: "socks5h://" + socksAddr})
	if err != nil {
		t.Fatal(err)
	}
	var out struct {
		OK bool `json:"ok"`
	}
	if err := c.DoJSON("GET", "/v1/ping", nil, nil, &out); err != nil {
		t.Fatal(err)
	}
	if !out.OK {
		t.Error("ok = false")
	}
	if atomic.LoadInt32(connects) == 0 {
		t.Error("request did not go through the SOCKS5 proxy")
	}
	if atomic.LoadInt32(&hit) == 0 {
		t.Error("backend was not reached")
	}
}

func TestClientProxyWithCredentials(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"ok":true}`)
	}))
	defer backend.Close()

	socksAddr, connects := startSOCKS5(t)
	// Embedded credentials must parse and not break the dial (our test proxy
	// selects no-auth, so the client proceeds without an auth exchange).
	c, err := New(Options{BaseURL: backend.URL, ProxyURL: "socks5h://user:pass@" + socksAddr})
	if err != nil {
		t.Fatal(err)
	}
	if err := c.DoJSON("GET", "/v1/ping", nil, nil, nil); err != nil {
		t.Fatal(err)
	}
	if atomic.LoadInt32(connects) == 0 {
		t.Error("request did not go through the SOCKS5 proxy")
	}
}

func TestNewRejectsUnknownProxyScheme(t *testing.T) {
	if _, err := New(Options{BaseURL: "http://example:7180", ProxyURL: "ftp://p:8080"}); err == nil {
		t.Fatal("expected error for unknown proxy scheme")
	}
}

func TestClientRoutesThroughHTTPProxy(t *testing.T) {
	var hit int32
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&hit, 1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"ok":true}`)
	}))
	defer backend.Close()

	// A minimal forward HTTP proxy: it round-trips the absolute-form request the
	// transport sends and counts proxied requests.
	var proxied int32
	proxySrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&proxied, 1)
		outReq, err := http.NewRequest(r.Method, r.URL.String(), r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadGateway)
			return
		}
		resp, err := http.DefaultTransport.RoundTrip(outReq)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadGateway)
			return
		}
		defer func() { _ = resp.Body.Close() }()
		w.WriteHeader(resp.StatusCode)
		_, _ = io.Copy(w, resp.Body)
	}))
	defer proxySrv.Close()

	c, err := New(Options{BaseURL: backend.URL, ProxyURL: proxySrv.URL}) // proxySrv.URL is http://...
	if err != nil {
		t.Fatal(err)
	}
	var out struct {
		OK bool `json:"ok"`
	}
	if err := c.DoJSON("GET", "/v1/ping", nil, nil, &out); err != nil {
		t.Fatal(err)
	}
	if !out.OK {
		t.Error("ok = false")
	}
	if atomic.LoadInt32(&proxied) == 0 {
		t.Error("request did not go through the HTTP proxy")
	}
	if atomic.LoadInt32(&hit) == 0 {
		t.Error("backend was not reached")
	}
}
