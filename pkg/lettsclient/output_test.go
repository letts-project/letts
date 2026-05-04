package lettsclient

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestOpenOutputStdout(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("stream") != "stdout" {
			t.Errorf("stream = %q", r.URL.Query().Get("stream"))
		}
		w.Header().Set("Content-Type", "application/octet-stream")
		_, _ = io.WriteString(w, "hello\nworld\n")
	}))
	defer srv.Close()
	c, _ := New(Options{BaseURL: srv.URL, Token: "t"})
	rc, err := OpenOutput(context.Background(), c, "abc", OutputOpts{Stream: "stdout"})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rc.Close() }()
	got, _ := io.ReadAll(rc)
	if !strings.Contains(string(got), "hello") {
		t.Errorf("got %q", got)
	}
}
