package lettsclient

import (
	"crypto/sha256"
	"encoding/hex"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
)

func writeFile(t *testing.T, content []byte) string {
	t.Helper()
	tmp := t.TempDir()
	p := filepath.Join(tmp, "src")
	if err := os.WriteFile(p, content, 0644); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestUploadFileNewArtifact(t *testing.T) {
	content := []byte("hello world")
	path := writeFile(t, content)
	sum := sha256.Sum256(content)
	hexsum := hex.EncodeToString(sum[:])

	var headCalls, putCalls atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case "HEAD":
			headCalls.Add(1)
			w.WriteHeader(404)
		case "PUT":
			putCalls.Add(1)
			body, _ := io.ReadAll(r.Body)
			if string(body) != "hello world" {
				t.Errorf("body = %q", body)
			}
			w.WriteHeader(201)
		}
	}))
	defer srv.Close()

	c, _ := New(Options{BaseURL: srv.URL, Token: "t"})
	stagingID, sumOut, sizeOut, err := UploadFile(c, "abc-staging-id", path)
	if err != nil {
		t.Fatal(err)
	}
	if stagingID != "abc-staging-id" || sumOut != hexsum || sizeOut != int64(len(content)) {
		t.Errorf("ret=%q sum=%q size=%d", stagingID, sumOut, sizeOut)
	}
	if headCalls.Load() != 1 || putCalls.Load() != 1 {
		t.Errorf("head=%d put=%d", headCalls.Load(), putCalls.Load())
	}
}

func TestUploadFileAlreadyCompleteSkipsPUT(t *testing.T) {
	content := []byte("xyz")
	path := writeFile(t, content)
	sum := sha256.Sum256(content)
	hexsum := hex.EncodeToString(sum[:])
	var putCalls atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case "HEAD":
			w.Header().Set("X-Letts-Upload-Status", "complete")
			w.Header().Set("X-Letts-Sha256", hexsum) // matching sha
			w.WriteHeader(200)
		case "PUT":
			putCalls.Add(1)
			w.WriteHeader(201)
		}
	}))
	defer srv.Close()

	c, _ := New(Options{BaseURL: srv.URL, Token: "t"})
	_, _, _, err := UploadFile(c, "id", path)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if putCalls.Load() != 0 {
		t.Errorf("PUT should not be called when HEAD reports complete with matching sha (put count=%d)", putCalls.Load())
	}
}

func TestUploadFileResume(t *testing.T) {
	content := []byte("0123456789")
	path := writeFile(t, content)
	sum := sha256.Sum256(content)
	hexsum := hex.EncodeToString(sum[:])

	var sawRange string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case "HEAD":
			w.Header().Set("X-Letts-Upload-Status", "incomplete")
			w.Header().Set("X-Letts-Sha256", hexsum)
			w.Header().Set("X-Letts-Bytes-Received", "4")
			w.Header().Set("X-Letts-Total-Size", "10")
			w.WriteHeader(200)
		case "PUT":
			sawRange = r.Header.Get("Content-Range")
			b, _ := io.ReadAll(r.Body)
			if string(b) != "456789" {
				t.Errorf("resume body = %q", b)
			}
			w.WriteHeader(201)
		}
	}))
	defer srv.Close()

	c, _ := New(Options{BaseURL: srv.URL, Token: "t"})
	if _, _, _, err := UploadFile(c, "id", path); err != nil {
		t.Fatal(err)
	}
	if sawRange != "bytes 4-9/10" {
		t.Errorf("range = %q", sawRange)
	}
	_ = strings.TrimSpace
}
