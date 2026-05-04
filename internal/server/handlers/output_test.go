package handlers_test

import (
	"context"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"letts/internal/ids"
	"letts/internal/server/handlers"
	"letts/internal/server/middleware"
	"letts/internal/storage"
)

func setupOutputTest(t *testing.T, status storage.Status) (*handlers.OutputHandler, string, string) {
	t.Helper()
	db := setupDB(t)
	dataDir := t.TempDir()
	id := ids.NewUUIDv7()

	m := &storage.Mission{
		ID:               id,
		Kind:             storage.KindMission,
		Lane:             "default",
		MissionName:      "test",
		Status:           status,
		Input:            []byte("{}"),
		InputFingerprint: "fp",
		TimeCreatedMs:    time.Now().UnixMilli(),
	}
	if err := storage.InsertMission(context.Background(), db, m); err != nil {
		t.Fatalf("insert: %v", err)
	}
	shard, _ := ids.ShardPath(id)
	parentDir := filepath.Join(dataDir, "output", shard)
	if err := os.MkdirAll(parentDir, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	return &handlers.OutputHandler{DataDir: dataDir, DB: db, PollEvery: 25 * time.Millisecond}, id, parentDir
}

func writeStreamFile(t *testing.T, parentDir, id, suffix, content string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(parentDir, id+suffix), []byte(content), 0o644); err != nil {
		t.Fatalf("write %s: %v", suffix, err)
	}
}

func doOutputReq(h *handlers.OutputHandler, id, stream string, follow bool) *httptest.ResponseRecorder {
	return doOutputReqScoped(h, id, stream, follow, middleware.ScopeDispatch)
}

func doOutputReqScoped(h *handlers.OutputHandler, id, stream string, follow bool, scope middleware.Scope) *httptest.ResponseRecorder {
	url := "/v1/missions/" + id + "/output?stream=" + stream
	if follow {
		url += "&follow=true"
	}
	r := withScope(httptest.NewRequest("GET", url, nil), scope)
	r.SetPathValue("id", id)
	w := httptest.NewRecorder()
	h.Stream(w, r)
	return w
}

func TestOutputStreamUnknownReturns400(t *testing.T) {
	h, id, _ := setupOutputTest(t, storage.StatusDone)
	w := doOutputReq(h, id, "weird", false)
	if w.Code != 400 {
		t.Errorf("status=%d, want 400", w.Code)
	}
}

func TestOutputMissionNotFoundReturns404(t *testing.T) {
	h, _, _ := setupOutputTest(t, storage.StatusDone)
	bogus := ids.NewUUIDv7()
	w := doOutputReq(h, bogus, "stdout", false)
	if w.Code != 404 {
		t.Errorf("status=%d, want 404", w.Code)
	}
}

func TestOutputMissionDeletingReturns410(t *testing.T) {
	h, id, _ := setupOutputTest(t, storage.StatusDeleting)
	w := doOutputReq(h, id, "stdout", false)
	if w.Code != 410 {
		t.Errorf("status=%d, want 410", w.Code)
	}
}

func TestOutputInvalidIDReturns400(t *testing.T) {
	h, _, _ := setupOutputTest(t, storage.StatusDone)
	w := doOutputReq(h, "not-a-uuid", "stdout", false)
	if w.Code != 400 {
		t.Errorf("status=%d, want 400", w.Code)
	}
}

func TestOutputStdoutReturnsContent(t *testing.T) {
	h, id, parentDir := setupOutputTest(t, storage.StatusDone)
	writeStreamFile(t, parentDir, id, "-stdout", "hello stdout\n")
	w := doOutputReq(h, id, "stdout", false)
	if w.Code != 200 {
		t.Fatalf("status=%d", w.Code)
	}
	if got := w.Header().Get("Content-Type"); got != "application/octet-stream" {
		t.Errorf("Content-Type=%q", got)
	}
	if w.Body.String() != "hello stdout\n" {
		t.Errorf("body=%q", w.Body.String())
	}
}

func TestOutputStderrReturnsContent(t *testing.T) {
	h, id, parentDir := setupOutputTest(t, storage.StatusDone)
	writeStreamFile(t, parentDir, id, "-stderr", "boom!")
	w := doOutputReq(h, id, "stderr", false)
	if w.Code != 200 || w.Body.String() != "boom!" {
		t.Errorf("status=%d body=%q", w.Code, w.Body.String())
	}
}

func TestOutputCombinedNDJSONContentType(t *testing.T) {
	h, id, parentDir := setupOutputTest(t, storage.StatusDone)
	writeStreamFile(t, parentDir, id, "-combined", `{"t":1,"stream":"stdout","data":"hi"}`+"\n")
	w := doOutputReq(h, id, "combined", false)
	if w.Code != 200 {
		t.Fatalf("status=%d", w.Code)
	}
	if got := w.Header().Get("Content-Type"); got != "application/x-ndjson" {
		t.Errorf("Content-Type=%q", got)
	}
	if !strings.HasPrefix(w.Body.String(), `{"t":1,"stream":"stdout"`) {
		t.Errorf("body=%q", w.Body.String())
	}
}

func TestOutputMissingFileReturnsEmptyBody(t *testing.T) {
	h, id, _ := setupOutputTest(t, storage.StatusDone)
	w := doOutputReq(h, id, "stdout", false)
	if w.Code != 200 {
		t.Errorf("status=%d", w.Code)
	}
	if w.Body.Len() != 0 {
		t.Errorf("body=%q, want empty", w.Body.String())
	}
}

func TestOutputFollowDeliversAppendedDataAndStopsWhenDone(t *testing.T) {
	h, id, parentDir := setupOutputTest(t, storage.StatusRunning)
	path := filepath.Join(parentDir, id+"-stdout")
	if err := os.WriteFile(path, []byte("first\n"), 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}

	r := withScope(httptest.NewRequest("GET", "/v1/missions/"+id+"/output?stream=stdout&follow=true", nil), middleware.ScopeDispatch)
	r.SetPathValue("id", id)
	w := httptest.NewRecorder()

	done := make(chan struct{})
	go func() {
		h.Stream(w, r)
		close(done)
	}()

	// Append more data while follow=true.
	time.Sleep(60 * time.Millisecond)
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	if _, err := f.Write([]byte("second\n")); err != nil {
		t.Fatalf("append: %v", err)
	}
	_ = f.Close()

	// Mark mission done so follow loop exits.
	time.Sleep(60 * time.Millisecond)
	_, err = h.DB.ExecContext(context.Background(),
		`UPDATE missions SET status='done' WHERE mission_id=?`, id)
	if err != nil {
		t.Fatalf("mark done: %v", err)
	}

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Stream didn't return after mission done")
	}
	body := w.Body.String()
	if !strings.Contains(body, "first") || !strings.Contains(body, "second") {
		t.Errorf("body missing data: %q", body)
	}
}

// Regression: when follow=true is requested before the mission has created
// its output file, the daemon used to return 200 with an empty body and
// close immediately — so any client tail goroutine (like the one in
// `letts run`) would silently exit before the mission actually started
// writing. With the fix, follow=true polls for the file to appear, then
// streams normally; if the mission ends without creating the file, the
// follow loop exits cleanly with whatever was written (possibly nothing).
func TestOutputFollowWaitsForMissingFileThenStreams(t *testing.T) {
	h, id, parentDir := setupOutputTest(t, storage.StatusRunning)
	path := filepath.Join(parentDir, id+"-stdout")

	r := withScope(httptest.NewRequest("GET", "/v1/missions/"+id+"/output?stream=stdout&follow=true", nil), middleware.ScopeDispatch)
	r.SetPathValue("id", id)
	w := httptest.NewRecorder()

	done := make(chan struct{})
	go func() {
		h.Stream(w, r)
		close(done)
	}()

	// File doesn't exist yet. Wait past a couple of poll intervals to prove
	// the handler is blocking, not returning early.
	time.Sleep(80 * time.Millisecond)

	// Now create the file with the mission's first bytes.
	if err := os.WriteFile(path, []byte("hello\n"), 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}

	// Append more so the streaming loop has something to do after the
	// initial open.
	time.Sleep(60 * time.Millisecond)
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	if _, err := f.Write([]byte("world\n")); err != nil {
		t.Fatalf("append: %v", err)
	}
	_ = f.Close()

	// Mark done so the streaming loop drains and exits.
	time.Sleep(60 * time.Millisecond)
	if _, err := h.DB.ExecContext(context.Background(),
		`UPDATE missions SET status='done' WHERE mission_id=?`, id); err != nil {
		t.Fatalf("mark done: %v", err)
	}

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Stream didn't return after mission done")
	}
	body := w.Body.String()
	if !strings.Contains(body, "hello") || !strings.Contains(body, "world") {
		t.Errorf("body missing data: %q", body)
	}
}

func TestOutputFollowReturnsOnContextCancel(t *testing.T) {
	h, id, parentDir := setupOutputTest(t, storage.StatusRunning)
	writeStreamFile(t, parentDir, id, "-stdout", "x")
	ctx, cancel := context.WithCancel(context.Background())
	r := withScope(httptest.NewRequest("GET", "/v1/missions/"+id+"/output?stream=stdout&follow=true", nil).WithContext(ctx), middleware.ScopeDispatch)
	r.SetPathValue("id", id)
	w := httptest.NewRecorder()
	done := make(chan struct{})
	go func() {
		h.Stream(w, r)
		close(done)
	}()
	time.Sleep(60 * time.Millisecond)
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Stream didn't return after ctx cancel")
	}
}

// TestOutputFollowOpensFileEvenIfMissionDoneOnFirstPoll is a regression
// for the fast-exec race: a sub-100ms mission (e.g. `letts exec -- uptime`)
// can finish and flip status=done in the window between the client's GET
// arriving and the handler's first poll tick. The runtime opens the
// per-stream output files up front, so the file exists by the time the
// lane runner picks the row up — even if the spawn and wait complete in
// the same poll tick. Before the fix the handler would observe
// status=done in the file-wait loop and return without ever opening the
// file, silently dropping the output. After the fix, the open is
// attempted before bailing out on the terminal status.
func TestOutputFollowOpensFileEvenIfMissionDoneOnFirstPoll(t *testing.T) {
	h, id, parentDir := setupOutputTest(t, storage.StatusRunning)
	path := filepath.Join(parentDir, id+"-stdout")

	r := withScope(httptest.NewRequest("GET", "/v1/missions/"+id+"/output?stream=stdout&follow=true", nil), middleware.ScopeDispatch)
	r.SetPathValue("id", id)
	w := httptest.NewRecorder()

	done := make(chan struct{})
	go func() {
		h.Stream(w, r)
		close(done)
	}()

	// Simulate the fast exec: 10ms after the GET arrives, write some
	// output AND flip status=done. The handler is mid-poll-loop and
	// must see the file (which now exists) rather than bailing on the
	// status=done check first.
	time.Sleep(10 * time.Millisecond)
	if err := os.WriteFile(path, []byte("fast-exec output\n"), 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}
	if _, err := h.DB.ExecContext(context.Background(),
		`UPDATE missions SET status='done' WHERE mission_id=?`, id); err != nil {
		t.Fatalf("mark done: %v", err)
	}

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Stream didn't return after mission done")
	}
	body := w.Body.String()
	if !strings.Contains(body, "fast-exec output") {
		t.Errorf("body missing fast-exec output: %q", body)
	}
}

func TestOutputContentTypeNoSniff(t *testing.T) {
	h, id, parentDir := setupOutputTest(t, storage.StatusDone)
	writeStreamFile(t, parentDir, id, "-stdout", "x")
	w := doOutputReq(h, id, "stdout", false)
	if w.Header().Get("X-Content-Type-Options") != "nosniff" {
		t.Errorf("missing nosniff header")
	}
}

// TestOutputStreamKindGated covers kind gating: dispatch tokens hitting an exec
// mission and exec tokens hitting a normal mission both get 403 forbidden_kind;
// admin sees both.
func TestOutputStreamKindGated(t *testing.T) {
	db := setupDB(t)
	dataDir := t.TempDir()

	// kind='mission'
	mid := ids.NewUUIDv7()
	if err := storage.InsertMission(context.Background(), db, &storage.Mission{
		ID: mid, Kind: storage.KindMission, Lane: "x", MissionName: "m",
		Status: storage.StatusDone, Input: []byte("{}"), InputFingerprint: "fp",
		TimeCreatedMs: time.Now().UnixMilli(),
	}); err != nil {
		t.Fatalf("insert mission: %v", err)
	}
	// kind='exec'
	xid := ids.NewUUIDv7()
	if err := storage.InsertMission(context.Background(), db, &storage.Mission{
		ID: xid, Kind: storage.KindExec, Lane: "x", MissionName: "x",
		Status: storage.StatusDone, Input: []byte("{}"), InputFingerprint: "fp",
		TimeCreatedMs: time.Now().UnixMilli(),
	}); err != nil {
		t.Fatalf("insert exec: %v", err)
	}
	// Create empty stdout files so reads don't surface other errors.
	for _, id := range []string{mid, xid} {
		shard, _ := ids.ShardPath(id)
		dir := filepath.Join(dataDir, "output", shard)
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatalf("mkdir: %v", err)
		}
		if err := os.WriteFile(filepath.Join(dir, id+"-stdout"), []byte("ok"), 0o644); err != nil {
			t.Fatalf("write stdout: %v", err)
		}
	}

	h := &handlers.OutputHandler{DataDir: dataDir, DB: db, PollEvery: 25 * time.Millisecond}

	// dispatch on exec → 403
	w := doOutputReqScoped(h, xid, "stdout", false, middleware.ScopeDispatch)
	if w.Code != 403 {
		t.Errorf("dispatch on exec: code=%d, want 403; body=%s", w.Code, w.Body.String())
	}
	if !strings.Contains(w.Body.String(), "forbidden_kind") {
		t.Errorf("body missing forbidden_kind: %q", w.Body.String())
	}

	// exec on mission → 403
	w = doOutputReqScoped(h, mid, "stdout", false, middleware.ScopeExec)
	if w.Code != 403 {
		t.Errorf("exec on mission: code=%d, want 403", w.Code)
	}

	// admin on either → 200
	if w := doOutputReqScoped(h, mid, "stdout", false, middleware.ScopeAdmin); w.Code != 200 {
		t.Errorf("admin on mission: code=%d, want 200", w.Code)
	}
	if w := doOutputReqScoped(h, xid, "stdout", false, middleware.ScopeAdmin); w.Code != 200 {
		t.Errorf("admin on exec: code=%d, want 200", w.Code)
	}
}
