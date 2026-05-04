package main

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"letts/internal/apply"
	"letts/pkg/lettsclient"
)

func TestApply(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" || r.URL.Path != "/v1/admin/apply" {
			t.Errorf("got %s %s", r.Method, r.URL.Path)
		}
		body, _ := io.ReadAll(r.Body)
		var got apply.AppliedState
		if err := json.Unmarshal(body, &got); err != nil {
			t.Fatal(err)
		}
		if got.MissionDir != "/var/www" {
			t.Errorf("mission_dir = %q", got.MissionDir)
		}
		w.WriteHeader(200)
		_, _ = io.WriteString(w, `{"diff":{"added":["normal"],"reason":""},"started":["normal"]}`)
	}))
	defer srv.Close()

	c, _ := lettsclient.New(lettsclient.Options{BaseURL: srv.URL, Token: "admin-token"})
	res, err := Apply(c, apply.AppliedState{
		MissionDir: "/var/www",
		Lanes:      map[string]apply.LaneCfg{"normal": {Concurrency: 4}},
	}, ApplyOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Started) != 1 || res.Started[0] != "normal" {
		t.Errorf("started = %v", res.Started)
	}
}

func TestApplyForceFlagInQuery(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.Contains(r.URL.RawQuery, "force=true") {
			t.Errorf("rawquery = %q", r.URL.RawQuery)
		}
		w.WriteHeader(200)
		_, _ = io.WriteString(w, `{"diff":{}}`)
	}))
	defer srv.Close()

	c, _ := lettsclient.New(lettsclient.Options{BaseURL: srv.URL, Token: "t"})
	_, err := Apply(c, apply.AppliedState{}, ApplyOptions{Force: true})
	if err != nil {
		t.Fatal(err)
	}
}

func TestGetState(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, `{"applied_at":1714,"source":"admin","state":{"mission_dir":"/x","lanes":{"normal":{"concurrency":2}}}}`)
	}))
	defer srv.Close()

	c, _ := lettsclient.New(lettsclient.Options{BaseURL: srv.URL, Token: "t"})
	st, err := GetState(c)
	if err != nil {
		t.Fatal(err)
	}
	if st.State.MissionDir != "/x" {
		t.Errorf("got %+v", st)
	}
}
