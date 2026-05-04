package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"letts/internal/ids"
	"letts/pkg/lettsconfig"
)

// dispatchStub builds an httptest server that handles staging HEAD/PUT and
// /v1/dispatch. capturedBody is filled with the last dispatch body received.
type dispatchStub struct {
	srv             *httptest.Server
	dispatchCalls   atomic.Int64
	stagingHeadHits atomic.Int64
	stagingPutHits  atomic.Int64
	idempotencyKey  atomic.Value // string
	lastBody        atomic.Value // []byte
	respStatus      atomic.Value // string ("queued" by default)
	respMissionID   atomic.Value // string
}

func newDispatchStub(t *testing.T) *dispatchStub {
	t.Helper()
	d := &dispatchStub{}
	d.respStatus.Store("queued")
	d.respMissionID.Store("") // empty → echo incoming Idempotency-Key
	d.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == "POST" && r.URL.Path == "/v1/dispatch":
			d.dispatchCalls.Add(1)
			d.idempotencyKey.Store(r.Header.Get("Idempotency-Key"))
			b, _ := io.ReadAll(r.Body)
			d.lastBody.Store(b)
			mid, _ := d.respMissionID.Load().(string)
			if mid == "" {
				mid = r.Header.Get("Idempotency-Key")
			}
			st, _ := d.respStatus.Load().(string)
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusAccepted)
			_, _ = io.WriteString(w, `{"mission_id":"`+mid+`","status":"`+st+`"}`)
		case r.Method == "HEAD" && strings.HasPrefix(r.URL.Path, "/v1/staging/"):
			d.stagingHeadHits.Add(1)
			w.WriteHeader(http.StatusNotFound)
		case r.Method == "PUT" && strings.HasPrefix(r.URL.Path, "/v1/staging/"):
			d.stagingPutHits.Add(1)
			_, _ = io.Copy(io.Discard, r.Body)
			w.WriteHeader(http.StatusCreated)
		default:
			t.Errorf("unexpected request: %s %s", r.Method, r.URL.Path)
			w.WriteHeader(http.StatusBadRequest)
		}
	}))
	return d
}

func (d *dispatchStub) close() { d.srv.Close() }

func (d *dispatchStub) body() map[string]any {
	raw, _ := d.lastBody.Load().([]byte)
	var v map[string]any
	_ = json.Unmarshal(raw, &v)
	return v
}

// stubDispatchAppCtx builds an *appCtx pointing one dugdale at d.srv.
func stubDispatchAppCtx(t *testing.T, d *dispatchStub) *appCtx {
	t.Helper()
	return &appCtx{
		Config: &lettsconfig.Config{
			Dugdales: []lettsconfig.Dugdale{
				{ID: "s1", Host: "ignored", Token: "tok",
					Lanes: map[string]lettsconfig.LaneCfg{
						"normal": {Concurrency: 1},
					}},
			},
			Routes: map[string]lettsconfig.Route{
				"normal": {Host: "s1", Lane: "normal"},
			},
		},
		Getenv:       func(k string) (string, bool) { return "", false },
		BaseURLForID: map[string]string{"s1": d.srv.URL},
		clients:      map[clientKey]*hostClient{},
	}
}

func TestDispatchMissingMissionReturnsBadUsage(t *testing.T) {
	d := newDispatchStub(t)
	defer d.close()
	ac := stubDispatchAppCtx(t, d)

	cmd := newDispatchCmd()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetIn(bytes.NewReader(nil))

	err := runDispatch(cmd, ac, &dispatchFlags{route: "normal"}, FormatText)
	if err == nil {
		t.Fatal("expected error for missing --mission")
	}
	var bue *BadUsageError
	if !errors.As(err, &bue) {
		t.Errorf("got %T %v, want BadUsageError", err, err)
	}
}

func TestDispatchWithRoute(t *testing.T) {
	d := newDispatchStub(t)
	defer d.close()
	ac := stubDispatchAppCtx(t, d)

	cmd := newDispatchCmd()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetIn(bytes.NewReader(nil))

	df := &dispatchFlags{
		route:   "normal",
		mission: "Smoke",
	}
	if err := runDispatch(cmd, ac, df, FormatText); err != nil {
		t.Fatalf("runDispatch: %v", err)
	}
	if d.dispatchCalls.Load() != 1 {
		t.Errorf("dispatch calls = %d, want 1", d.dispatchCalls.Load())
	}
	body := d.body()
	if body["mission"] != "Smoke" {
		t.Errorf("body.mission = %v, want Smoke", body["mission"])
	}
	if body["lane"] != "normal" {
		t.Errorf("body.lane = %v, want normal", body["lane"])
	}
	got := strings.TrimSpace(out.String())
	parts := strings.Split(got, "\t")
	if len(parts) != 2 {
		t.Fatalf("text output should be id\\tstatus, got %q", got)
	}
	if !ids.ValidateUUIDv7(parts[0]) {
		t.Errorf("first column not a UUIDv7: %q", parts[0])
	}
	if parts[1] != "queued" {
		t.Errorf("status = %q, want queued", parts[1])
	}
}

func TestDispatchWithHostAndLane(t *testing.T) {
	d := newDispatchStub(t)
	defer d.close()
	ac := stubDispatchAppCtx(t, d)

	cmd := newDispatchCmd()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetIn(bytes.NewReader(nil))

	df := &dispatchFlags{
		host:    "s1",
		lane:    "normal",
		mission: "M",
	}
	if err := runDispatch(cmd, ac, df, FormatText); err != nil {
		t.Fatalf("runDispatch: %v", err)
	}
	if d.dispatchCalls.Load() != 1 {
		t.Errorf("dispatch calls = %d", d.dispatchCalls.Load())
	}
	body := d.body()
	if body["lane"] != "normal" {
		t.Errorf("body.lane=%v", body["lane"])
	}
}

func TestDispatchHostWithoutLaneIsBadUsage(t *testing.T) {
	d := newDispatchStub(t)
	defer d.close()
	ac := stubDispatchAppCtx(t, d)

	cmd := newDispatchCmd()
	cmd.SetOut(io.Discard)
	cmd.SetIn(bytes.NewReader(nil))

	err := runDispatch(cmd, ac, &dispatchFlags{host: "s1", mission: "M"}, FormatText)
	if err == nil {
		t.Fatal("expected error")
	}
	var bue *BadUsageError
	if !errors.As(err, &bue) {
		t.Errorf("got %T", err)
	}
}

func TestDispatchAutoSelect(t *testing.T) {
	d := newDispatchStub(t)
	defer d.close()
	ac := stubDispatchAppCtx(t, d)
	// Only --lane → must pick s1 via auto-select.

	cmd := newDispatchCmd()
	cmd.SetOut(io.Discard)
	cmd.SetIn(bytes.NewReader(nil))

	df := &dispatchFlags{lane: "normal", mission: "M"}
	if err := runDispatch(cmd, ac, df, FormatText); err != nil {
		t.Fatalf("runDispatch: %v", err)
	}
	if d.dispatchCalls.Load() != 1 {
		t.Errorf("dispatch calls = %d", d.dispatchCalls.Load())
	}
}

func TestDispatchInputLiteralIsForwarded(t *testing.T) {
	d := newDispatchStub(t)
	defer d.close()
	ac := stubDispatchAppCtx(t, d)

	cmd := newDispatchCmd()
	cmd.SetOut(io.Discard)
	cmd.SetIn(bytes.NewReader(nil))

	df := &dispatchFlags{
		route:   "normal",
		mission: "M",
		input:   `{"k":1}`,
	}
	if err := runDispatch(cmd, ac, df, FormatText); err != nil {
		t.Fatalf("runDispatch: %v", err)
	}
	body := d.body()
	inp, ok := body["input"].(map[string]any)
	if !ok {
		t.Fatalf("body.input = %v (%T)", body["input"], body["input"])
	}
	if v, _ := inp["k"].(float64); v != 1 {
		t.Errorf("body.input.k = %v, want 1", inp["k"])
	}
}

func TestDispatchInputFileStdin(t *testing.T) {
	d := newDispatchStub(t)
	defer d.close()
	ac := stubDispatchAppCtx(t, d)

	cmd := newDispatchCmd()
	cmd.SetOut(io.Discard)
	cmd.SetIn(strings.NewReader(`{"from":"stdin"}`))

	df := &dispatchFlags{
		route:     "normal",
		mission:   "M",
		inputFile: "-",
	}
	if err := runDispatch(cmd, ac, df, FormatText); err != nil {
		t.Fatalf("runDispatch: %v", err)
	}
	body := d.body()
	inp, _ := body["input"].(map[string]any)
	if inp["from"] != "stdin" {
		t.Errorf("body.input.from = %v, want stdin", inp["from"])
	}
}

func TestDispatchInputFileOnDisk(t *testing.T) {
	d := newDispatchStub(t)
	defer d.close()
	ac := stubDispatchAppCtx(t, d)

	tmp := t.TempDir()
	p := filepath.Join(tmp, "in.json")
	if err := os.WriteFile(p, []byte(`{"src":"disk"}`), 0o600); err != nil {
		t.Fatal(err)
	}

	cmd := newDispatchCmd()
	cmd.SetOut(io.Discard)
	cmd.SetIn(bytes.NewReader(nil))

	df := &dispatchFlags{
		route:     "normal",
		mission:   "M",
		inputFile: p,
	}
	if err := runDispatch(cmd, ac, df, FormatText); err != nil {
		t.Fatalf("runDispatch: %v", err)
	}
	body := d.body()
	inp, _ := body["input"].(map[string]any)
	if inp["src"] != "disk" {
		t.Errorf("body.input.src = %v, want disk", inp["src"])
	}
}

func TestDispatchInputAndInputFileMutuallyExclusive(t *testing.T) {
	d := newDispatchStub(t)
	defer d.close()
	ac := stubDispatchAppCtx(t, d)

	cmd := newDispatchCmd()
	cmd.SetOut(io.Discard)
	cmd.SetIn(bytes.NewReader(nil))

	err := runDispatch(cmd, ac, &dispatchFlags{
		route:     "normal",
		mission:   "M",
		input:     `{}`,
		inputFile: "x",
	}, FormatText)
	if err == nil {
		t.Fatal("expected error")
	}
	var bue *BadUsageError
	if !errors.As(err, &bue) {
		t.Errorf("got %T", err)
	}
}

func TestDispatchInputInvalidJSONReturnsError(t *testing.T) {
	d := newDispatchStub(t)
	defer d.close()
	ac := stubDispatchAppCtx(t, d)

	cmd := newDispatchCmd()
	cmd.SetOut(io.Discard)
	cmd.SetIn(bytes.NewReader(nil))

	err := runDispatch(cmd, ac, &dispatchFlags{
		route:   "normal",
		mission: "M",
		input:   `{not json}`,
	}, FormatText)
	if err == nil {
		t.Fatal("expected error for invalid JSON")
	}
}

func TestDispatchFileUploads(t *testing.T) {
	d := newDispatchStub(t)
	defer d.close()
	ac := stubDispatchAppCtx(t, d)

	tmp := t.TempDir()
	p := filepath.Join(tmp, "a.bin")
	content := []byte("hello world")
	if err := os.WriteFile(p, content, 0o600); err != nil {
		t.Fatal(err)
	}

	cmd := newDispatchCmd()
	cmd.SetOut(io.Discard)
	cmd.SetIn(bytes.NewReader(nil))

	df := &dispatchFlags{
		route:   "normal",
		mission: "M",
		files:   []string{"config=" + p},
	}
	if err := runDispatch(cmd, ac, df, FormatText); err != nil {
		t.Fatalf("runDispatch: %v", err)
	}
	if d.stagingHeadHits.Load() != 1 {
		t.Errorf("stagingHead calls = %d, want 1", d.stagingHeadHits.Load())
	}
	if d.stagingPutHits.Load() != 1 {
		t.Errorf("stagingPut calls = %d, want 1", d.stagingPutHits.Load())
	}
	body := d.body()
	files, ok := body["files"].([]any)
	if !ok || len(files) != 1 {
		t.Fatalf("body.files = %v (%T)", body["files"], body["files"])
	}
	first := files[0].(map[string]any)
	if first["role"] != "config" {
		t.Errorf("files[0].role = %v, want config", first["role"])
	}
	sid, _ := first["staging_id"].(string)
	if !ids.ValidateUUIDv7(sid) {
		t.Errorf("files[0].staging_id = %q, not UUIDv7", sid)
	}
}

func TestDispatchFileBadFormat(t *testing.T) {
	d := newDispatchStub(t)
	defer d.close()
	ac := stubDispatchAppCtx(t, d)

	cmd := newDispatchCmd()
	cmd.SetOut(io.Discard)
	cmd.SetIn(bytes.NewReader(nil))

	err := runDispatch(cmd, ac, &dispatchFlags{
		route:   "normal",
		mission: "M",
		files:   []string{"=missingrole"},
	}, FormatText)
	if err == nil {
		t.Fatal("expected error")
	}
	var bue *BadUsageError
	if !errors.As(err, &bue) {
		t.Errorf("got %T", err)
	}
}

func TestDispatchMissionIDOverride(t *testing.T) {
	d := newDispatchStub(t)
	defer d.close()
	ac := stubDispatchAppCtx(t, d)

	override := "01900000-0000-7000-8000-00000000abcd"
	cmd := newDispatchCmd()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetIn(bytes.NewReader(nil))

	df := &dispatchFlags{
		route:     "normal",
		mission:   "M",
		missionID: override,
	}
	if err := runDispatch(cmd, ac, df, FormatText); err != nil {
		t.Fatalf("runDispatch: %v", err)
	}
	if got, _ := d.idempotencyKey.Load().(string); got != override {
		t.Errorf("Idempotency-Key = %q, want %q", got, override)
	}
	if !strings.HasPrefix(strings.TrimSpace(out.String()), override) {
		t.Errorf("text output should start with %q, got %q", override, out.String())
	}
}

func TestDispatchJSONOutput(t *testing.T) {
	d := newDispatchStub(t)
	defer d.close()
	ac := stubDispatchAppCtx(t, d)

	cmd := newDispatchCmd()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetIn(bytes.NewReader(nil))

	df := &dispatchFlags{
		route:   "normal",
		mission: "M",
	}
	if err := runDispatch(cmd, ac, df, FormatJSON); err != nil {
		t.Fatalf("runDispatch: %v", err)
	}
	var got map[string]any
	if err := json.Unmarshal(out.Bytes(), &got); err != nil {
		t.Fatalf("json parse: %v body=%s", err, out.String())
	}
	if got["status"] != "queued" {
		t.Errorf("status = %v", got["status"])
	}
	if !ids.ValidateUUIDv7(got["mission_id"].(string)) {
		t.Errorf("mission_id = %v", got["mission_id"])
	}
}

func TestDispatchTimeoutForwarded(t *testing.T) {
	d := newDispatchStub(t)
	defer d.close()
	ac := stubDispatchAppCtx(t, d)

	cmd := newDispatchCmd()
	cmd.SetOut(io.Discard)
	cmd.SetIn(bytes.NewReader(nil))

	df := &dispatchFlags{
		route:   "normal",
		mission: "M",
		timeout: "5m",
	}
	if err := runDispatch(cmd, ac, df, FormatText); err != nil {
		t.Fatalf("runDispatch: %v", err)
	}
	body := d.body()
	if body["timeout"] != "5m" {
		t.Errorf("body.timeout = %v, want 5m", body["timeout"])
	}
}

func TestDispatchNoTargetIsBadUsage(t *testing.T) {
	d := newDispatchStub(t)
	defer d.close()
	ac := stubDispatchAppCtx(t, d)

	cmd := newDispatchCmd()
	cmd.SetOut(io.Discard)
	cmd.SetIn(bytes.NewReader(nil))

	err := runDispatch(cmd, ac, &dispatchFlags{mission: "M"}, FormatText)
	if err == nil {
		t.Fatal("expected error")
	}
	var bue *BadUsageError
	if !errors.As(err, &bue) {
		t.Errorf("got %T %v", err, err)
	}
}

// runDispatch should default to "{}" input when none is provided.
func TestDispatchDefaultEmptyInput(t *testing.T) {
	d := newDispatchStub(t)
	defer d.close()
	ac := stubDispatchAppCtx(t, d)

	cmd := newDispatchCmd()
	cmd.SetOut(io.Discard)
	cmd.SetIn(bytes.NewReader(nil))

	df := &dispatchFlags{route: "normal", mission: "M"}
	if err := runDispatch(cmd, ac, df, FormatText); err != nil {
		t.Fatalf("runDispatch: %v", err)
	}
	body := d.body()
	// Empty object input means {} — but omitempty on RawMessage retains "{}".
	inp, ok := body["input"].(map[string]any)
	if !ok {
		t.Fatalf("body.input expected, got %v", body["input"])
	}
	if len(inp) != 0 {
		t.Errorf("body.input should be empty object, got %v", inp)
	}
}
