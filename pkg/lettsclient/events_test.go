package lettsclient

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func TestStreamEventsArchived(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/x-ndjson")
		_, _ = io.WriteString(w, `{"seq":1,"event":"queued"}`+"\n"+
			`{"seq":2,"event":"running"}`+"\n"+
			`{"seq":3,"event":"done","outcome":"success"}`+"\n")
	}))
	defer srv.Close()

	c, _ := New(Options{BaseURL: srv.URL, Token: "t"})
	var got []Event
	err := StreamEvents(context.Background(), c, "abc", StreamOpts{}, func(e Event) error {
		got = append(got, e)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 || got[2].Event != "done" {
		t.Fatalf("got %+v", got)
	}
}

func TestStreamEventsReconnectOnFollow(t *testing.T) {
	var attempt atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/x-ndjson")
		n := attempt.Add(1)
		if n == 1 {
			_, _ = io.WriteString(w, `{"seq":1,"event":"queued"}`+"\n"+
				`{"seq":2,"event":"running"}`+"\n")
			return
		}
		if got := r.URL.Query().Get("from"); got != "2" {
			t.Errorf("reconnect from = %q want 2", got)
		}
		_, _ = io.WriteString(w, `{"seq":3,"event":"done","outcome":"success"}`+"\n")
	}))
	defer srv.Close()

	c, _ := New(Options{BaseURL: srv.URL, Token: "t"})
	var got []Event
	err := StreamEvents(context.Background(), c, "abc",
		StreamOpts{Follow: true, ReconnectBackoff: 1 * time.Millisecond},
		func(e Event) error {
			got = append(got, e)
			return nil
		})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 || got[2].Event != "done" {
		t.Fatalf("got %+v", got)
	}
}

func TestStreamEventsContextCancel(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		flusher, _ := w.(http.Flusher)
		w.Header().Set("Content-Type", "application/x-ndjson")
		_, _ = io.WriteString(w, `{"seq":1,"event":"running"}`+"\n")
		flusher.Flush()
		<-r.Context().Done()
	}))
	defer srv.Close()

	c, _ := New(Options{BaseURL: srv.URL, Token: "t"})
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(10 * time.Millisecond)
		cancel()
	}()
	err := StreamEvents(ctx, c, "abc", StreamOpts{Follow: true}, func(e Event) error { return nil })
	if err == nil {
		t.Fatal("expected context error")
	}
}

// TestEventDecodeDoneWithOutputs verifies Event decodes the exact done-event
// shape the daemon emits: outputs is a map keyed by role with {staging_id,
// sha256, size}, the timestamp lives under "time_finished", and
// duration_ms is present.
func TestEventDecodeDoneWithOutputs(t *testing.T) {
	raw := `{"seq":3,"event":"done","outcome":"success","time_finished":1714600045123,"duration_ms":1234,"exit_code":0,"outputs":{"result":{"staging_id":"sid-1","sha256":"abc","size":42}}}`
	var e Event
	if err := json.Unmarshal([]byte(raw), &e); err != nil {
		t.Fatal(err)
	}
	if len(e.Outputs) != 1 {
		t.Fatalf("outputs len = %d, want 1: %+v", len(e.Outputs), e.Outputs)
	}
	out, ok := e.Outputs["result"]
	if !ok {
		t.Fatalf("missing 'result' role: %+v", e.Outputs)
	}
	if out.StagingID != "sid-1" || out.SHA256 != "abc" || out.Size != 42 {
		t.Errorf("outputs[result] = %+v", out)
	}
	if e.TimeFinished != 1714600045123 {
		t.Errorf("time_finished = %d", e.TimeFinished)
	}
	if e.DurationMs != 1234 {
		t.Errorf("duration_ms = %d", e.DurationMs)
	}
	if e.Outcome != "success" {
		t.Errorf("outcome = %q", e.Outcome)
	}
	if e.ExitCode == nil || *e.ExitCode != 0 {
		t.Errorf("exit_code = %v", e.ExitCode)
	}
}

// TestEventDecodeQueued verifies the queued-event shape from
// internal/server/handlers/dispatch.go (mission_id, time_created, lane, mission).
func TestEventDecodeQueued(t *testing.T) {
	raw := `{"seq":1,"event":"queued","mission_id":"abc","time_created":1714600000000,"lane":"build","mission":"compile"}`
	var e Event
	if err := json.Unmarshal([]byte(raw), &e); err != nil {
		t.Fatal(err)
	}
	if e.MissionID != "abc" || e.Lane != "build" || e.Mission != "compile" {
		t.Errorf("queued event = %+v", e)
	}
	if e.TimeCreated != 1714600000000 {
		t.Errorf("time_created = %d", e.TimeCreated)
	}
}

// TestEventDecodeRunning verifies the first-run shape from
// internal/mission/waiter.go (time, pid, time_started).
func TestEventDecodeRunning(t *testing.T) {
	raw := `{"seq":2,"event":"running","time":1714600001000,"pid":4242,"time_started":1714600001000}`
	var e Event
	if err := json.Unmarshal([]byte(raw), &e); err != nil {
		t.Fatal(err)
	}
	if e.Pid != 4242 || e.Time != 1714600001000 || e.TimeStarted != 1714600001000 {
		t.Errorf("running event = %+v", e)
	}
}

func TestStreamEventsFollowStopsOnClientError(t *testing.T) {
	for _, status := range []int{401, 403, 400} {
		var hits atomic.Int64
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			hits.Add(1)
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(status)
			_, _ = io.WriteString(w, `{"error":"forbidden_kind","message":"no"}`)
		}))
		c, _ := New(Options{BaseURL: srv.URL, Token: "t"})
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		err := StreamEvents(ctx, c, "abc", StreamOpts{Follow: true, ReconnectBackoff: time.Millisecond}, func(Event) error { return nil })
		cancel()
		srv.Close()
		var he *HTTPError
		if !errors.As(err, &he) || he.Status != status {
			t.Errorf("status %d: err=%v, want the HTTPError", status, err)
		}
		if hits.Load() != 1 {
			t.Errorf("status %d: %d requests, want 1", status, hits.Load())
		}
	}
}

func TestStreamEventsFollowRetriesServerErrors(t *testing.T) {
	var hits atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if hits.Add(1) < 3 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		_, _ = io.WriteString(w, `{"seq":1,"event":"done","outcome":"success"}`+"\n")
	}))
	defer srv.Close()
	c, _ := New(Options{BaseURL: srv.URL, Token: "t"})
	if err := StreamEvents(context.Background(), c, "abc", StreamOpts{Follow: true, ReconnectBackoff: time.Millisecond}, func(Event) error { return nil }); err != nil {
		t.Fatalf("err=%v", err)
	}
	if hits.Load() != 3 {
		t.Errorf("hits=%d, want 3", hits.Load())
	}
}

func TestStreamEventsSnapshotWithoutDoneSucceeds(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, `{"seq":1,"event":"queued"}`+"\n"+`{"seq":2,"event":"running"}`+"\n")
	}))
	defer srv.Close()
	c, _ := New(Options{BaseURL: srv.URL, Token: "t"})
	var n int
	if err := StreamEvents(context.Background(), c, "abc", StreamOpts{}, func(Event) error { n++; return nil }); err != nil || n != 2 {
		t.Fatalf("err=%v events=%d, want nil and 2", err, n)
	}
}

func TestStreamEventsFollowPastDoneFails(t *testing.T) {
	var streams atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/missions/abc/events" {
			streams.Add(1)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"mission_id":"abc","status":"done"}`)
	}))
	defer srv.Close()
	c, _ := New(Options{BaseURL: srv.URL, Token: "t"})
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	err := StreamEvents(ctx, c, "abc", StreamOpts{Follow: true, From: 9, ReconnectBackoff: time.Millisecond}, func(Event) error { return nil })
	if err == nil || ctx.Err() != nil {
		t.Fatalf("err=%v ctxErr=%v, want a prompt error", err, ctx.Err())
	}
	if streams.Load() != 2 {
		t.Errorf("streams=%d, want 2 (one more read after seeing done)", streams.Load())
	}
}

func TestStreamEventsFollowDoneRaceRereads(t *testing.T) {
	var streams atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/missions/abc/events" {
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, `{"mission_id":"abc","status":"done"}`)
			return
		}
		switch streams.Add(1) {
		case 1:
			_, _ = io.WriteString(w, `{"seq":1,"event":"queued"}`+"\n")
		case 3:
			_, _ = io.WriteString(w, `{"seq":2,"event":"done","outcome":"success"}`+"\n")
		}
	}))
	defer srv.Close()
	c, _ := New(Options{BaseURL: srv.URL, Token: "t"})
	var got []string
	err := StreamEvents(context.Background(), c, "abc", StreamOpts{Follow: true, ReconnectBackoff: time.Millisecond}, func(e Event) error {
		got = append(got, e.Event)
		return nil
	})
	if err != nil || len(got) != 2 || got[1] != "done" {
		t.Fatalf("err=%v events=%v, want queued then done", err, got)
	}
}

func TestStreamEventsFollowReturnsCallbackAndDecodeErrors(t *testing.T) {
	sentinel := errors.New("stop")
	for _, tc := range []struct {
		body string
		fn   func(Event) error
		want func(error) bool
	}{
		{`{"seq":1,"event":"queued"}` + "\n", func(Event) error { return sentinel }, func(err error) bool { return errors.Is(err, sentinel) }},
		{"not json\n", func(Event) error { return nil }, func(err error) bool { return err != nil }},
	} {
		var hits atomic.Int64
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			hits.Add(1)
			_, _ = io.WriteString(w, tc.body)
		}))
		c, _ := New(Options{BaseURL: srv.URL, Token: "t"})
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		err := StreamEvents(ctx, c, "abc", StreamOpts{Follow: true, ReconnectBackoff: time.Millisecond}, tc.fn)
		timedOut := ctx.Err() != nil
		cancel()
		srv.Close()
		if !tc.want(err) || timedOut || hits.Load() != 1 {
			t.Errorf("body %q: err=%v hits=%d, want a prompt error after one request", tc.body, err, hits.Load())
		}
	}
}

func TestStreamEventsFollowIdleCloseBacksOff(t *testing.T) {
	var streams atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/missions/abc/events" {
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, `{"mission_id":"abc","status":"running"}`)
			return
		}
		if streams.Add(1) == 3 {
			_, _ = io.WriteString(w, `{"seq":5,"event":"done","outcome":"success"}`+"\n")
		}
	}))
	defer srv.Close()
	c, _ := New(Options{BaseURL: srv.URL, Token: "t"})
	start := time.Now()
	err := StreamEvents(context.Background(), c, "abc", StreamOpts{Follow: true, ReconnectBackoff: 50 * time.Millisecond}, func(Event) error { return nil })
	if err != nil {
		t.Fatalf("err=%v", err)
	}
	if elapsed := time.Since(start); elapsed < 100*time.Millisecond {
		t.Errorf("reconnected after %v, want two backoffs", elapsed)
	}
}
