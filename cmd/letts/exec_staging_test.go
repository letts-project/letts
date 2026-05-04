package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/spf13/cobra"

	"letts/internal/ids"
	"letts/pkg/lettsclient"
)

// TestDownloadAllAtomicRollsBackOnSecondFailure verifies the all-or-none contract:
// when the second of two downloads fails AFTER the first finished
// streaming bytes to a sidecar tmp, neither final path is left on disk.
// Without the all-or-none coordinator a single-host `letts exec --out
// a=x.png --out b=y.png` would leave x.png behind when y.png's download
// hit a transport error mid-stream.
func TestDownloadAllAtomicRollsBackOnSecondFailure(t *testing.T) {
	bytesA := []byte("AAAAAAAA")
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.Contains(r.URL.Path, "/staging-a"):
			w.WriteHeader(200)
			_, _ = w.Write(bytesA)
		case strings.Contains(r.URL.Path, "/staging-b"):
			w.WriteHeader(500)
			_, _ = w.Write([]byte(`{"error":"internal"}`))
		default:
			w.WriteHeader(404)
		}
	}))
	defer srv.Close()
	c, _ := lettsclient.New(lettsclient.Options{BaseURL: srv.URL, Token: "t"})

	dir := t.TempDir()
	pathA := filepath.Join(dir, "a.bin")
	pathB := filepath.Join(dir, "b.bin")

	err := downloadAllAtomic([]atomicDownload{
		{Client: c, StagingID: "staging-a", FinalPath: pathA},
		{Client: c, StagingID: "staging-b", FinalPath: pathB},
	})
	if err == nil {
		t.Fatal("expected error from second download failing")
	}
	// All-or-none: A should NOT be on disk because B failed.
	if _, sErr := os.Stat(pathA); sErr == nil {
		t.Errorf("file %s exists after rollback; want absent", pathA)
	}
	if _, sErr := os.Stat(pathB); sErr == nil {
		t.Errorf("file %s exists; want absent (download failed)", pathB)
	}
}

// TestDownloadAllAtomicAllSucceed verifies the happy path: every file
// ends up on disk with the right contents and no leftover tmp sidecars.
func TestDownloadAllAtomicAllSucceed(t *testing.T) {
	bytesA := []byte("AAA")
	bytesB := []byte("BBBBB")
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.Contains(r.URL.Path, "/staging-a"):
			w.WriteHeader(200)
			_, _ = w.Write(bytesA)
		case strings.Contains(r.URL.Path, "/staging-b"):
			w.WriteHeader(200)
			_, _ = w.Write(bytesB)
		default:
			w.WriteHeader(404)
		}
	}))
	defer srv.Close()
	c, _ := lettsclient.New(lettsclient.Options{BaseURL: srv.URL, Token: "t"})

	dir := t.TempDir()
	pathA := filepath.Join(dir, "a.bin")
	pathB := filepath.Join(dir, "b.bin")

	if err := downloadAllAtomic([]atomicDownload{
		{Client: c, StagingID: "staging-a", FinalPath: pathA},
		{Client: c, StagingID: "staging-b", FinalPath: pathB},
	}); err != nil {
		t.Fatalf("downloadAllAtomic: %v", err)
	}
	gotA, _ := os.ReadFile(pathA)
	if string(gotA) != string(bytesA) {
		t.Errorf("A contents: got %q want %q", gotA, bytesA)
	}
	gotB, _ := os.ReadFile(pathB)
	if string(gotB) != string(bytesB) {
		t.Errorf("B contents: got %q want %q", gotB, bytesB)
	}
	// No leftover tmp sidecars (filenames begin with the final base + ".tmp.").
	matches, _ := filepath.Glob(filepath.Join(dir, "*.tmp.*"))
	if len(matches) != 0 {
		t.Errorf("leftover tmp sidecars: %v", matches)
	}
}

// TestDownloadAllAtomicPreCheckRefusesExistingFinal verifies the pre-
// flight check: if any final path is occupied BEFORE any download
// starts, the operation returns a BadUsageError without touching disk.
func TestDownloadAllAtomicPreCheckRefusesExistingFinal(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		_, _ = w.Write([]byte("never read"))
	}))
	defer srv.Close()
	c, _ := lettsclient.New(lettsclient.Options{BaseURL: srv.URL, Token: "t"})

	dir := t.TempDir()
	pathA := filepath.Join(dir, "a.bin")
	pathB := filepath.Join(dir, "b.bin")
	if err := os.WriteFile(pathB, []byte("pre-existing"), 0o600); err != nil {
		t.Fatal(err)
	}
	err := downloadAllAtomic([]atomicDownload{
		{Client: c, StagingID: "staging-a", FinalPath: pathA},
		{Client: c, StagingID: "staging-b", FinalPath: pathB},
	})
	if err == nil {
		t.Fatal("expected error for existing final path")
	}
	var bue *BadUsageError
	if !errorsAs(err, &bue) {
		t.Errorf("expected BadUsageError, got %T: %v", err, err)
	}
	if _, sErr := os.Stat(pathA); sErr == nil {
		t.Errorf("file %s was written despite pre-check failure", pathA)
	}
}

// errorsAs is the local stand-in for errors.As (avoid importing errors
// just for this one assertion).
func errorsAs(err error, target **BadUsageError) bool {
	for e := err; e != nil; {
		if bu, ok := e.(*BadUsageError); ok {
			*target = bu
			return true
		}
		type unwrapper interface{ Unwrap() error }
		if u, ok := e.(unwrapper); ok {
			e = u.Unwrap()
			continue
		}
		break
	}
	return false
}

func TestUploadOrReuseHit(t *testing.T) {
	hitID := "0192aaaa-0000-7000-8000-000000000000"
	var puts int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == "GET" && strings.HasPrefix(r.URL.Path, "/v1/staging/by-content/"):
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"staging_id":"` + hitID + `"}`))
		case r.Method == "PUT" && strings.HasPrefix(r.URL.Path, "/v1/staging/"):
			atomic.AddInt32(&puts, 1)
			w.WriteHeader(201)
		default:
			w.WriteHeader(404)
		}
	}))
	defer srv.Close()
	c, _ := lettsclient.New(lettsclient.Options{BaseURL: srv.URL, Token: "t"})

	path := writeTempFile(t, "hello")
	defer func() { _ = os.Remove(path) }()
	id, err := uploadOrReuse(c, path)
	if err != nil {
		t.Fatal(err)
	}
	if id != hitID {
		t.Errorf("id=%q, want %q", id, hitID)
	}
	if atomic.LoadInt32(&puts) != 0 {
		t.Errorf("PUT count=%d, want 0", puts)
	}
}

func TestUploadOrReuseMiss(t *testing.T) {
	var puts int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == "GET" && strings.HasPrefix(r.URL.Path, "/v1/staging/by-content/"):
			w.WriteHeader(404)
			_, _ = w.Write([]byte(`{"error":"not_found"}`))
		case r.Method == "HEAD" && strings.HasPrefix(r.URL.Path, "/v1/staging/"):
			w.WriteHeader(404)
		case r.Method == "PUT" && strings.HasPrefix(r.URL.Path, "/v1/staging/"):
			atomic.AddInt32(&puts, 1)
			w.WriteHeader(201)
		default:
			w.WriteHeader(500)
		}
	}))
	defer srv.Close()
	c, _ := lettsclient.New(lettsclient.Options{BaseURL: srv.URL, Token: "t"})

	path := writeTempFile(t, "fresh-bytes")
	defer func() { _ = os.Remove(path) }()
	id, err := uploadOrReuse(c, path)
	if err != nil {
		t.Fatal(err)
	}
	if id == "" {
		t.Error("id empty")
	}
	if !ids.ValidateUUIDv7(id) {
		t.Errorf("id=%q, want UUIDv7", id)
	}
	if atomic.LoadInt32(&puts) != 1 {
		t.Errorf("PUT count=%d, want 1", puts)
	}
}

func TestUploadOrReuseBytesHit(t *testing.T) {
	hitID := "0192bbbb-0000-7000-8000-000000000000"
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"staging_id":"` + hitID + `"}`))
	}))
	defer srv.Close()
	c, _ := lettsclient.New(lettsclient.Options{BaseURL: srv.URL, Token: "t"})
	id, err := uploadOrReuseBytes(c, []byte("hello-bytes"))
	if err != nil {
		t.Fatal(err)
	}
	if id != hitID {
		t.Errorf("id=%q", id)
	}
}

func writeTempFile(t *testing.T, content string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "x")
	if err := os.WriteFile(p, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	return p
}

// TestSuffixBeforeExt exercises the per-host filename disambiguator used by
// the multi-host fan-out coordinator (E5). Single-host (E4) keeps paths
// verbatim. Covers four shapes: ext, no ext, double-ext (only last split),
// and no leading path.
func TestSuffixBeforeExt(t *testing.T) {
	cases := []struct{ Path, Suffix, Want string }{
		{"/p/result.png", "-s1", "/p/result-s1.png"},
		{"/p/out", "-s1", "/p/out-s1"},
		{"/p/x.tar.gz", "-s1", "/p/x.tar-s1.gz"},
		{"x.png", "-s1", "x-s1.png"},
	}
	for _, c := range cases {
		got := suffixBeforeExt(c.Path, c.Suffix)
		if got != c.Want {
			t.Errorf("suffixBeforeExt(%q, %q) = %q, want %q", c.Path, c.Suffix, got, c.Want)
		}
	}
}

// TestExecOutDownloadSingleHost is the E4 happy path: --out png=<dest>
// causes the CLI to download the role's bytes after a successful done
// event and write them atomically to dest. Verifies the bytes round-trip
// correctly through GetStaging → sidecar tmp → rename.
func TestExecOutDownloadSingleHost(t *testing.T) {
	dest := filepath.Join(t.TempDir(), "result.png")
	hs := newExecHostStubWithStagingAndOutputs(t, "s1",
		map[string][]byte{"png": []byte("PNGBYTES")},
		execHostPlan{doneOutcome: "success"})
	defer hs.close()
	ef := &execFlags{
		lane: "light", host: "s1",
		argv:      []string{"x"},
		out:       []string{"png=" + dest},
		outputFmt: "raw",
	}
	_, _, err := runExecForTest(t, hs, ef)
	if got := mapExecErr(t, err); got != 0 {
		t.Fatalf("exit=%d, want 0; err=%v", got, err)
	}
	got, readErr := os.ReadFile(dest)
	if readErr != nil {
		t.Fatalf("readfile: %v", readErr)
	}
	if string(got) != "PNGBYTES" {
		t.Errorf("downloaded bytes=%q, want %q", got, "PNGBYTES")
	}
}

// TestExecOutDownloadRefusesOverwrite pins the refuse-overwrite contract:
// an existing file at the --out target must produce BadUsage (exit 2)
// BEFORE the network download happens (fail-fast pre-check) and BEFORE
// any partial bytes touch disk. The pre-existing content stays untouched.
func TestExecOutDownloadRefusesOverwrite(t *testing.T) {
	dest := writeTempFile(t, "existing-content")
	hs := newExecHostStubWithStagingAndOutputs(t, "s1",
		map[string][]byte{"png": []byte("NEWBYTES")},
		execHostPlan{doneOutcome: "success"})
	defer hs.close()
	ef := &execFlags{
		lane: "light", host: "s1",
		argv:      []string{"x"},
		out:       []string{"png=" + dest},
		outputFmt: "raw",
	}
	_, _, err := runExecForTest(t, hs, ef)
	if got := mapExecErr(t, err); got != 2 {
		t.Errorf("exit=%d, want 2 (output_exists badusage)", got)
	}
	// Original bytes survive untouched.
	got, _ := os.ReadFile(dest)
	if string(got) != "existing-content" {
		t.Errorf("dest mutated: %q", got)
	}
}

// TestExecOutDownloadEmptyFileAllowed: 0-byte outputs are valid (a
// `touch sentinel.flag`-style usage pattern is legitimate). After the
// download the destination must exist and have size 0.
func TestExecOutDownloadEmptyFileAllowed(t *testing.T) {
	dest := filepath.Join(t.TempDir(), "sentinel.flag")
	hs := newExecHostStubWithStagingAndOutputs(t, "s1",
		map[string][]byte{"flag": {}},
		execHostPlan{doneOutcome: "success"})
	defer hs.close()
	ef := &execFlags{
		lane: "light", host: "s1",
		argv:      []string{"x"},
		out:       []string{"flag=" + dest},
		outputFmt: "raw",
	}
	_, _, err := runExecForTest(t, hs, ef)
	if got := mapExecErr(t, err); got != 0 {
		t.Fatalf("exit=%d, want 0; err=%v", got, err)
	}
	info, statErr := os.Stat(dest)
	if statErr != nil {
		t.Fatalf("stat: %v", statErr)
	}
	if info.Size() != 0 {
		t.Errorf("size=%d, want 0", info.Size())
	}
}

// TestExecOutDownloadMultiHostSuffix is the multi-host happy path: a
// single --out png=<dest> against two hosts produces two files with the
// per-host suffix inserted BEFORE the extension: r.png → r-s1.png and
// r-s2.png. Each holds the bytes the respective host served. Routes
// through the fan-out coordinator (downloadFanOutOutputs) after both
// hosts reach a successful done event.
func TestExecOutDownloadMultiHostSuffix(t *testing.T) {
	destPath := filepath.Join(t.TempDir(), "r.png")
	hs1 := newExecHostStubWithStagingAndOutputs(t, "s1",
		map[string][]byte{"png": []byte("s1bytes")},
		execHostPlan{doneOutcome: "success"})
	defer hs1.close()
	hs2 := newExecHostStubWithStagingAndOutputs(t, "s2",
		map[string][]byte{"png": []byte("s2bytes")},
		execHostPlan{doneOutcome: "success"})
	defer hs2.close()
	ac := stubExecMultiAppCtx(t, []*execHostStub{hs1, hs2})
	ef := &execFlags{
		lane: "light", host: "s1,s2",
		argv:      []string{"x"},
		out:       []string{"png=" + destPath},
		outputFmt: "prefix",
	}
	cmd := &cobra.Command{}
	cmd.SetContext(context.Background())
	cmd.SetOut(&strings.Builder{})
	cmd.SetErr(&strings.Builder{})
	_ = runExec(cmd, ac, ef, FormatText)

	g1, err1 := os.ReadFile(filepath.Join(filepath.Dir(destPath), "r-s1.png"))
	if err1 != nil {
		t.Fatalf("read r-s1.png: %v", err1)
	}
	g2, err2 := os.ReadFile(filepath.Join(filepath.Dir(destPath), "r-s2.png"))
	if err2 != nil {
		t.Fatalf("read r-s2.png: %v", err2)
	}
	if string(g1) != "s1bytes" || string(g2) != "s2bytes" {
		t.Errorf("s1=%q s2=%q, want s1bytes/s2bytes", g1, g2)
	}
}

// TestExecOutDownloadFailureNoPartial pins the all-or-none invariant:
// when one host's download fails mid-flight the coordinator must roll
// back ALL writes — no per-host final file from the successful host
// either. Either every host's output lands or none.
// s2's GET /v1/staging/{id} returns 500; the coordinator must clean up
// s1's already-written tmp before returning the transport error.
func TestExecOutDownloadFailureNoPartial(t *testing.T) {
	destPath := filepath.Join(t.TempDir(), "r.png")
	hs1 := newExecHostStubWithStagingAndOutputs(t, "s1",
		map[string][]byte{"png": []byte("s1ok")},
		execHostPlan{doneOutcome: "success"})
	defer hs1.close()
	hs2 := newExecHostStubFailingDownload(t, "s2", "png",
		execHostPlan{doneOutcome: "success"})
	defer hs2.close()
	ac := stubExecMultiAppCtx(t, []*execHostStub{hs1, hs2})
	ef := &execFlags{
		lane: "light", host: "s1,s2",
		argv:      []string{"x"},
		out:       []string{"png=" + destPath},
		outputFmt: "prefix",
	}
	cmd := &cobra.Command{}
	cmd.SetContext(context.Background())
	cmd.SetOut(&strings.Builder{})
	cmd.SetErr(&strings.Builder{})
	err := runExec(cmd, ac, ef, FormatText)
	if got := mapErrorToExit(err); got != 255 {
		t.Errorf("exit=%d, want 255 (transport)", got)
	}
	if _, err := os.Stat(filepath.Join(filepath.Dir(destPath), "r-s1.png")); err == nil {
		t.Errorf("s1 file should not exist after coordinator rollback")
	}
	if _, err := os.Stat(filepath.Join(filepath.Dir(destPath), "r-s2.png")); err == nil {
		t.Errorf("s2 file should not exist after coordinator rollback")
	}
}
