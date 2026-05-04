package lettsclient

import (
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestPauseResumeLane(t *testing.T) {
	var paths []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		paths = append(paths, r.Method+" "+r.URL.Path)
		_, _ = io.WriteString(w, `{"status":"ok"}`)
	}))
	defer srv.Close()

	c, _ := New(Options{BaseURL: srv.URL, Token: "t"})
	if err := PauseLane(c, "normal"); err != nil {
		t.Fatal(err)
	}
	if err := ContinueLane(c, "normal"); err != nil {
		t.Fatal(err)
	}
	if paths[0] != "POST /v1/admin/lanes/normal/pause" {
		t.Errorf("pause path = %q", paths[0])
	}
	if paths[1] != "POST /v1/admin/lanes/normal/continue" {
		t.Errorf("continue path = %q", paths[1])
	}
}
