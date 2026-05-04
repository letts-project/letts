package main

import (
	"bytes"
	"io"
	"net/http"
	"strings"
	"testing"
)

func TestCtlLanesList(t *testing.T) {
	ac, stop := stubAppCtx(t, "s1", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, `[{"name":"normal","concurrency":4,"paused":false,"queued":2,"running":1}]`)
	}))
	defer stop()
	var out bytes.Buffer
	if err := runCtlLanesList(ac, &out, "s1", FormatText); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "normal") {
		t.Errorf("got %q", out.String())
	}
}

func TestCtlLanesPauseContinue(t *testing.T) {
	var seen []string
	ac, stop := stubAppCtx(t, "s1", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen = append(seen, r.URL.Path)
		_, _ = io.WriteString(w, `{}`)
	}))
	defer stop()
	if err := runCtlLanesPause(ac, "s1", "normal"); err != nil {
		t.Fatal(err)
	}
	if err := runCtlLanesContinue(ac, "s1", "normal"); err != nil {
		t.Fatal(err)
	}
	want := []string{"/v1/admin/lanes/normal/pause", "/v1/admin/lanes/normal/continue"}
	for i := range want {
		if seen[i] != want[i] {
			t.Errorf("[%d] %q want %q", i, seen[i], want[i])
		}
	}
}
