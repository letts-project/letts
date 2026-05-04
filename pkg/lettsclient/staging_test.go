package lettsclient

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// --- HEAD ----------------------------------------------------------------

func TestHeadStagingNotFound(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "HEAD" {
			t.Errorf("method=%q", r.Method)
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer srv.Close()

	c, _ := New(Options{BaseURL: srv.URL, Token: "t"})
	got, err := HeadStaging(c, "01900000-0000-7000-8000-000000000001")
	if err != nil {
		t.Fatalf("HeadStaging: %v", err)
	}
	if got.Status != StagingNotFound {
		t.Errorf("status=%v, want StagingNotFound", got.Status)
	}
}

func TestHeadStagingComplete(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Letts-Sha256", "deadbeef")
		w.Header().Set("X-Letts-Upload-Status", "complete")
		w.Header().Set("Content-Length", "1024")
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	c, _ := New(Options{BaseURL: srv.URL, Token: "t"})
	got, err := HeadStaging(c, "01900000-0000-7000-8000-000000000001")
	if err != nil {
		t.Fatalf("HeadStaging: %v", err)
	}
	if got.Status != StagingComplete {
		t.Errorf("status=%v, want StagingComplete", got.Status)
	}
	if got.SHA256 != "deadbeef" {
		t.Errorf("sha=%q", got.SHA256)
	}
	if got.TotalSize != 1024 {
		t.Errorf("TotalSize=%d, want 1024", got.TotalSize)
	}
}

func TestHeadStagingIncomplete(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Letts-Sha256", "abcdef")
		w.Header().Set("X-Letts-Upload-Status", "incomplete")
		w.Header().Set("X-Letts-Bytes-Received", "512")
		w.Header().Set("X-Letts-Total-Size", "2048")
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	c, _ := New(Options{BaseURL: srv.URL, Token: "t"})
	got, err := HeadStaging(c, "01900000-0000-7000-8000-000000000002")
	if err != nil {
		t.Fatalf("HeadStaging: %v", err)
	}
	if got.Status != StagingIncomplete {
		t.Errorf("status=%v, want StagingIncomplete", got.Status)
	}
	if got.SHA256 != "abcdef" {
		t.Errorf("sha=%q", got.SHA256)
	}
	if got.BytesReceived != 512 {
		t.Errorf("BytesReceived=%d, want 512", got.BytesReceived)
	}
	if got.TotalSize != 2048 {
		t.Errorf("TotalSize=%d, want 2048", got.TotalSize)
	}
}

func TestHeadStagingNon200ErrorReturnsHTTPError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()
	c, _ := New(Options{BaseURL: srv.URL, Token: "t"})
	_, err := HeadStaging(c, "01900000-0000-7000-8000-000000000003")
	if err == nil {
		t.Fatal("expected error for 500")
	}
	he, ok := err.(*HTTPError)
	if !ok {
		t.Fatalf("err is not *HTTPError: %T %v", err, err)
	}
	if he.Status != 500 {
		t.Errorf("Status=%d", he.Status)
	}
}

// --- PUT initial ---------------------------------------------------------

func TestPutStagingInitialBodyAndHeaders(t *testing.T) {
	var gotMethod, gotPath, gotSha, gotCT, gotCR string
	var gotCL int64
	var gotBody []byte
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod = r.Method
		gotPath = r.URL.Path
		gotSha = r.Header.Get("X-Letts-Sha256")
		gotCT = r.Header.Get("Content-Type")
		gotCR = r.Header.Get("Content-Range")
		gotCL = r.ContentLength
		gotBody, _ = io.ReadAll(r.Body)
		w.WriteHeader(http.StatusCreated)
		_, _ = io.WriteString(w, `{"staging_id":"01900000-0000-7000-8000-000000000001","sha256":"abc","size":5,"ttl_seconds":3600}`)
	}))
	defer srv.Close()

	c, _ := New(Options{BaseURL: srv.URL, Token: "t"})
	body := bytes.NewReader([]byte("hello"))
	err := PutStagingInitial(c, "01900000-0000-7000-8000-000000000001", "abc", 5, body)
	if err != nil {
		t.Fatalf("PutStagingInitial: %v", err)
	}
	if gotMethod != "PUT" {
		t.Errorf("method=%q", gotMethod)
	}
	if gotPath != "/v1/staging/01900000-0000-7000-8000-000000000001" {
		t.Errorf("path=%q", gotPath)
	}
	if gotSha != "abc" {
		t.Errorf("sha=%q", gotSha)
	}
	if gotCT != "application/octet-stream" {
		t.Errorf("Content-Type=%q", gotCT)
	}
	if gotCR != "" {
		t.Errorf("Content-Range should be empty on initial PUT, got %q", gotCR)
	}
	if gotCL != 5 {
		t.Errorf("Content-Length=%d, want 5", gotCL)
	}
	if string(gotBody) != "hello" {
		t.Errorf("body=%q", gotBody)
	}
}

func TestPutStagingInitialSurfacesHTTPError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		w.WriteHeader(http.StatusConflict)
		_, _ = io.WriteString(w, `{"error":"content_mismatch","message":"sha differs"}`)
	}))
	defer srv.Close()
	c, _ := New(Options{BaseURL: srv.URL, Token: "t"})
	err := PutStagingInitial(c, "01900000-0000-7000-8000-000000000001", "abc", 5, bytes.NewReader([]byte("hello")))
	if err == nil {
		t.Fatal("expected error on 409")
	}
	he, ok := err.(*HTTPError)
	if !ok {
		t.Fatalf("err is not *HTTPError: %T %v", err, err)
	}
	if he.Code != "content_mismatch" {
		t.Errorf("code=%q", he.Code)
	}
}

// --- PUT resume ----------------------------------------------------------

func TestPutStagingResumeContentRangeBytes4To9Of10(t *testing.T) {
	var gotMethod, gotSha, gotCR string
	var gotCL int64
	var gotBody []byte
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod = r.Method
		gotSha = r.Header.Get("X-Letts-Sha256")
		gotCR = r.Header.Get("Content-Range")
		gotCL = r.ContentLength
		gotBody, _ = io.ReadAll(r.Body)
		w.WriteHeader(http.StatusCreated)
		_, _ = io.WriteString(w, `{"staging_id":"x","sha256":"abc","size":10,"ttl_seconds":3600}`)
	}))
	defer srv.Close()

	c, _ := New(Options{BaseURL: srv.URL, Token: "t"})
	// total=10, offset=4 → suffix is 6 bytes
	err := PutStagingResume(c, "01900000-0000-7000-8000-000000000002", "abc", 10, 4, bytes.NewReader([]byte("567890")))
	if err != nil {
		t.Fatalf("PutStagingResume: %v", err)
	}
	if gotMethod != "PUT" {
		t.Errorf("method=%q", gotMethod)
	}
	if gotSha != "abc" {
		t.Errorf("sha=%q", gotSha)
	}
	if gotCR != "bytes 4-9/10" {
		t.Errorf("Content-Range=%q, want %q", gotCR, "bytes 4-9/10")
	}
	if gotCL != 6 {
		t.Errorf("Content-Length=%d, want 6", gotCL)
	}
	if string(gotBody) != "567890" {
		t.Errorf("body=%q", gotBody)
	}
}

func TestPutStagingResumeRejectsOffsetGTETotal(t *testing.T) {
	c, _ := New(Options{BaseURL: "http://localhost:9", Token: "t"})
	err := PutStagingResume(c, "x", "abc", 10, 10, bytes.NewReader(nil))
	if err == nil {
		t.Fatal("expected error for offset>=total")
	}
	if !strings.Contains(err.Error(), "offset") {
		t.Errorf("err=%v", err)
	}
}

// --- GET -----------------------------------------------------------------

func TestGetStagingFullDownload(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "GET" {
			t.Errorf("method=%q", r.Method)
		}
		if r.Header.Get("Range") != "" {
			t.Errorf("Range should be empty, got %q", r.Header.Get("Range"))
		}
		w.Header().Set("Content-Type", "application/octet-stream")
		w.Header().Set("Content-Length", "11")
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, "hello world")
	}))
	defer srv.Close()

	c, _ := New(Options{BaseURL: srv.URL, Token: "t"})
	rc, size, err := GetStaging(c, "01900000-0000-7000-8000-000000000001", "")
	if err != nil {
		t.Fatalf("GetStaging: %v", err)
	}
	defer func() { _ = rc.Close() }()
	if size != 11 {
		t.Errorf("size=%d, want 11", size)
	}
	got, _ := io.ReadAll(rc)
	if string(got) != "hello world" {
		t.Errorf("body=%q", got)
	}
}

func TestGetStagingRangeRequestForwardsHeader(t *testing.T) {
	var gotRange string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotRange = r.Header.Get("Range")
		w.Header().Set("Content-Length", "100")
		w.WriteHeader(http.StatusPartialContent)
		_, _ = w.Write(bytes.Repeat([]byte("x"), 100))
	}))
	defer srv.Close()

	c, _ := New(Options{BaseURL: srv.URL, Token: "t"})
	rc, _, err := GetStaging(c, "01900000-0000-7000-8000-000000000002", "bytes=0-99")
	if err != nil {
		t.Fatalf("GetStaging: %v", err)
	}
	defer func() { _ = rc.Close() }()
	if gotRange != "bytes=0-99" {
		t.Errorf("Range=%q", gotRange)
	}
}

func TestGetStagingNon2xxReturnsHTTPError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		_, _ = io.WriteString(w, `{"error":"not_found","message":"missing"}`)
	}))
	defer srv.Close()
	c, _ := New(Options{BaseURL: srv.URL, Token: "t"})
	_, _, err := GetStaging(c, "01900000-0000-7000-8000-000000000003", "")
	if err == nil {
		t.Fatal("expected error")
	}
	he, ok := err.(*HTTPError)
	if !ok {
		t.Fatalf("err is not *HTTPError: %T", err)
	}
	if he.Status != 404 || he.Code != "not_found" {
		t.Errorf("got %+v", he)
	}
}

// --- DELETE --------------------------------------------------------------

func TestDeleteStagingWithoutForce(t *testing.T) {
	var gotPath, gotQuery string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "DELETE" {
			t.Errorf("method=%q", r.Method)
		}
		gotPath = r.URL.Path
		gotQuery = r.URL.RawQuery
		w.WriteHeader(http.StatusAccepted)
		_, _ = io.WriteString(w, `{"status":"deletion_pending"}`)
	}))
	defer srv.Close()

	c, _ := New(Options{BaseURL: srv.URL, Token: "t"})
	err := DeleteStaging(c, "01900000-0000-7000-8000-000000000001", false)
	if err != nil {
		t.Fatalf("DeleteStaging: %v", err)
	}
	if gotPath != "/v1/staging/01900000-0000-7000-8000-000000000001" {
		t.Errorf("path=%q", gotPath)
	}
	if gotQuery != "" {
		t.Errorf("query=%q, want empty", gotQuery)
	}
}

func TestDeleteStagingWithForce(t *testing.T) {
	var gotQuery string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotQuery = r.URL.RawQuery
		w.WriteHeader(http.StatusAccepted)
		_, _ = io.WriteString(w, `{"status":"deletion_pending"}`)
	}))
	defer srv.Close()

	c, _ := New(Options{BaseURL: srv.URL, Token: "t"})
	err := DeleteStaging(c, "01900000-0000-7000-8000-000000000001", true)
	if err != nil {
		t.Fatalf("DeleteStaging: %v", err)
	}
	if gotQuery != "force=true" {
		t.Errorf("query=%q, want force=true", gotQuery)
	}
}

func TestDeleteStagingSurfacesConflict(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusConflict)
		_, _ = io.WriteString(w, `{"error":"staging_in_use","message":"refs"}`)
	}))
	defer srv.Close()
	c, _ := New(Options{BaseURL: srv.URL, Token: "t"})
	err := DeleteStaging(c, "01900000-0000-7000-8000-000000000001", false)
	if err == nil {
		t.Fatal("expected 409 error")
	}
	he, ok := err.(*HTTPError)
	if !ok {
		t.Fatalf("err is not *HTTPError: %T", err)
	}
	if he.Code != "staging_in_use" {
		t.Errorf("code=%q", he.Code)
	}
}

// --- ListStaging ---------------------------------------------------------

func TestListStagingMissionIDQuery(t *testing.T) {
	var gotPath string
	var gotMission, gotRefKind, gotCursor, gotLimit string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		gotMission = r.URL.Query().Get("mission_id")
		gotRefKind = r.URL.Query().Get("ref_kind")
		gotCursor = r.URL.Query().Get("cursor")
		gotLimit = r.URL.Query().Get("limit")
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, `{"staging":[{"staging_id":"sid1","sha256":"sha1","state":"complete","ref_kind":"input","role":"input1","size":100,"bytes_received":100,"time_created":1000,"time_expires":2000}],"next_cursor":"next-cur"}`)
	}))
	defer srv.Close()

	c, _ := New(Options{BaseURL: srv.URL, Token: "t"})
	out, err := ListStaging(c, ListStagingOpts{
		MissionID: "01900000-0000-7000-8000-000000000001",
		RefKind:   "input",
		Cursor:    "abc",
		Limit:     50,
	})
	if err != nil {
		t.Fatalf("ListStaging: %v", err)
	}
	if gotPath != "/v1/staging" {
		t.Errorf("path=%q", gotPath)
	}
	if gotMission != "01900000-0000-7000-8000-000000000001" {
		t.Errorf("mission_id=%q", gotMission)
	}
	if gotRefKind != "input" {
		t.Errorf("ref_kind=%q", gotRefKind)
	}
	if gotCursor != "abc" {
		t.Errorf("cursor=%q", gotCursor)
	}
	if gotLimit != "50" {
		t.Errorf("limit=%q", gotLimit)
	}
	if len(out.Staging) != 1 {
		t.Fatalf("len=%d", len(out.Staging))
	}
	if out.Staging[0].StagingID != "sid1" {
		t.Errorf("staging_id=%q", out.Staging[0].StagingID)
	}
	if out.Staging[0].SHA256 != "sha1" {
		t.Errorf("sha256=%q", out.Staging[0].SHA256)
	}
	if out.Staging[0].State != "complete" {
		t.Errorf("state=%q", out.Staging[0].State)
	}
	if out.Staging[0].RefKind != "input" {
		t.Errorf("ref_kind=%q", out.Staging[0].RefKind)
	}
	if out.Staging[0].Role != "input1" {
		t.Errorf("role=%q", out.Staging[0].Role)
	}
	if out.Staging[0].Size != 100 {
		t.Errorf("size=%d", out.Staging[0].Size)
	}
	if out.Staging[0].BytesReceived != 100 {
		t.Errorf("bytes_received=%d", out.Staging[0].BytesReceived)
	}
	if out.Staging[0].TimeCreated != 1000 {
		t.Errorf("time_created=%d", out.Staging[0].TimeCreated)
	}
	if out.Staging[0].TimeExpires == nil || *out.Staging[0].TimeExpires != 2000 {
		t.Errorf("time_expires=%v", out.Staging[0].TimeExpires)
	}
	if out.NextCursor != "next-cur" {
		t.Errorf("next_cursor=%q", out.NextCursor)
	}
}

func TestListStagingOmitsEmptyOptionalParams(t *testing.T) {
	var gotRefKind, gotCursor, gotLimit string
	var hasRefKind, hasCursor, hasLimit bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, hasRefKind = r.URL.Query()["ref_kind"]
		_, hasCursor = r.URL.Query()["cursor"]
		_, hasLimit = r.URL.Query()["limit"]
		gotRefKind = r.URL.Query().Get("ref_kind")
		gotCursor = r.URL.Query().Get("cursor")
		gotLimit = r.URL.Query().Get("limit")
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, `{"staging":[]}`)
	}))
	defer srv.Close()

	c, _ := New(Options{BaseURL: srv.URL, Token: "t"})
	_, err := ListStaging(c, ListStagingOpts{MissionID: "01900000-0000-7000-8000-000000000001"})
	if err != nil {
		t.Fatalf("ListStaging: %v", err)
	}
	if hasRefKind || gotRefKind != "" {
		t.Errorf("ref_kind should be omitted, got %q", gotRefKind)
	}
	if hasCursor || gotCursor != "" {
		t.Errorf("cursor should be omitted, got %q", gotCursor)
	}
	if hasLimit || gotLimit != "" {
		t.Errorf("limit should be omitted, got %q", gotLimit)
	}
}

func TestListStagingEmptyResponse(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, `{"staging":[]}`)
	}))
	defer srv.Close()

	c, _ := New(Options{BaseURL: srv.URL, Token: "t"})
	out, err := ListStaging(c, ListStagingOpts{MissionID: "01900000-0000-7000-8000-000000000001"})
	if err != nil {
		t.Fatalf("ListStaging: %v", err)
	}
	if len(out.Staging) != 0 {
		t.Errorf("len=%d", len(out.Staging))
	}
	if out.NextCursor != "" {
		t.Errorf("next_cursor=%q", out.NextCursor)
	}
}

// --- StagingByContent ----------------------------------------------------
// Tests for StagingByContent live in staging_bycontent_test.go (sentinel
// triple contract: (id, true, nil) / ("", false, nil) / ("", false, err)).
