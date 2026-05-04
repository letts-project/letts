package storage

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	"letts/internal/ids"
)

func newTestStaging(id string) *StagingFile {
	return &StagingFile{
		StagingID:     id,
		State:         StagingUploading,
		Sha256:        "abc123",
		Size:          1024,
		BytesReceived: 0,
		Path:          "staging/ab/c1/23",
		TimeCreatedMs: 1000,
		TimeUpdatedMs: 1000,
		TimeExpiresMs: 5000,
	}
}

func TestInsertGetStaging(t *testing.T) {
	db := setupDB(t)
	id := ids.NewUUIDv7()
	s := newTestStaging(id)
	if err := InsertStaging(context.Background(), db, s); err != nil {
		t.Fatal(err)
	}
	got, err := GetStaging(context.Background(), db, id)
	if err != nil {
		t.Fatal(err)
	}
	if got.State != StagingUploading {
		t.Errorf("got state %q", got.State)
	}
	if got.Sha256 != "abc123" {
		t.Errorf("got sha256 %q", got.Sha256)
	}
}

func TestGetStagingNotFound(t *testing.T) {
	db := setupDB(t)
	_, err := GetStaging(context.Background(), db, "nonexistent")
	if err != ErrNotFound {
		t.Errorf("expected ErrNotFound, got %v", err)
	}
}

func TestUpdateStagingProgress(t *testing.T) {
	db := setupDB(t)
	id := ids.NewUUIDv7()
	_ = InsertStaging(context.Background(), db, newTestStaging(id))

	if err := UpdateStagingProgress(context.Background(), db, id, 512, 2000, 6000); err != nil {
		t.Fatal(err)
	}
	got, _ := GetStaging(context.Background(), db, id)
	if got.BytesReceived != 512 {
		t.Errorf("expected 512 bytes, got %d", got.BytesReceived)
	}
	if got.TimeExpiresMs != 6000 {
		t.Errorf("expected 6000 expires, got %d", got.TimeExpiresMs)
	}
}

func TestUpdateStagingProgressWrongState(t *testing.T) {
	db := setupDB(t)
	id := ids.NewUUIDv7()
	s := newTestStaging(id)
	s.State = StagingComplete
	_ = InsertStaging(context.Background(), db, s)

	// UpdateStagingProgress requires state='uploading'
	err := UpdateStagingProgress(context.Background(), db, id, 512, 2000, 6000)
	if err != ErrNotFound {
		t.Errorf("expected ErrNotFound for non-uploading state, got %v", err)
	}
}

func TestMarkStagingComplete(t *testing.T) {
	db := setupDB(t)
	id := ids.NewUUIDv7()
	_ = InsertStaging(context.Background(), db, newTestStaging(id))

	if err := MarkStagingComplete(context.Background(), db, id, "verified-sha", 1024, 3000, 9000); err != nil {
		t.Fatal(err)
	}
	got, _ := GetStaging(context.Background(), db, id)
	if got.State != StagingComplete {
		t.Errorf("expected complete, got %q", got.State)
	}
	if got.Sha256 != "verified-sha" {
		t.Errorf("expected verified sha, got %q", got.Sha256)
	}
}

func TestMarkStagingDeleting(t *testing.T) {
	db := setupDB(t)
	id := ids.NewUUIDv7()
	_ = InsertStaging(context.Background(), db, newTestStaging(id))
	if err := MarkStagingDeleting(context.Background(), db, id); err != nil {
		t.Fatal(err)
	}
	got, _ := GetStaging(context.Background(), db, id)
	if got.State != StagingDeleting {
		t.Errorf("expected deleting, got %q", got.State)
	}
}

// TestMarkStagingDeletingPreservesPhaseB enforces the invariant:
// admin force-delete arriving while a row is mid-Phase-B (state=
// 'pending_output' or 'committing') must NOT transition the row to
// 'deleting'. Phase B owns those states; flipping them out from under
// finalize causes file-vs-row inconsistency (file in tombstone yet row
// stays 'complete' after Phase B step 4 races back).
func TestMarkStagingDeletingPreservesPhaseB(t *testing.T) {
	cases := []StagingState{StagingPendingOutput, StagingCommitting}
	for _, st := range cases {
		t.Run(string(st), func(t *testing.T) {
			db := setupDB(t)
			id := ids.NewUUIDv7()
			s := newTestStaging(id)
			s.State = st
			_ = InsertStaging(context.Background(), db, s)

			err := MarkStagingDeleting(context.Background(), db, id)
			if !errors.Is(err, ErrStagingFinalizing) {
				t.Errorf("got %v, want ErrStagingFinalizing for state=%q", err, st)
			}
			// Row state must be untouched.
			got, _ := GetStaging(context.Background(), db, id)
			if got.State != st {
				t.Errorf("state mutated: got %q, want %q", got.State, st)
			}
		})
	}
}

// TestMarkStagingDeletingIdempotent verifies the already-deleting case
// returns nil (no error) rather than ErrNotFound — callers like the
// upload-completion content-mismatch path should be safe to call repeatedly.
func TestMarkStagingDeletingIdempotent(t *testing.T) {
	db := setupDB(t)
	id := ids.NewUUIDv7()
	s := newTestStaging(id)
	s.State = StagingDeleting
	_ = InsertStaging(context.Background(), db, s)

	if err := MarkStagingDeleting(context.Background(), db, id); err != nil {
		t.Errorf("got %v, want nil for already-deleting", err)
	}
}

// TestMarkStagingDeletingIfExpiredSkipsPromotedRow verifies a row
// that was expired at SELECT time but got promoted to live (recalc to
// MaxInt64) by a concurrent dispatch must NOT be flipped to deleting.
// Without the time_expires guard the GC reaped a row referenced by a
// fresh queued mission.
func TestMarkStagingDeletingIfExpiredSkipsPromotedRow(t *testing.T) {
	db := setupDB(t)
	id := ids.NewUUIDv7()
	s := newTestStaging(id)
	s.State = StagingComplete
	s.TimeExpiresMs = 1000 // expired at "now"
	_ = InsertStaging(context.Background(), db, s)

	// Simulate concurrent dispatch promoting the row to live.
	if _, err := db.Exec(`UPDATE staging_files SET time_expires=? WHERE staging_id=?`,
		int64(1<<62), id); err != nil {
		t.Fatal(err)
	}

	// GC observes the original expiry but tries to flip now — should skip.
	if err := MarkStagingDeletingIfExpired(context.Background(), db, id, 5000); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	got, _ := GetStaging(context.Background(), db, id)
	if got.State != StagingComplete {
		t.Errorf("state=%q, want complete (promoted row reaped)", got.State)
	}
}

// TestMarkStagingDeletingIfExpiredFlipsExpiredRow exercises the
// happy-path: still-expired row goes to deleting.
func TestMarkStagingDeletingIfExpiredFlipsExpiredRow(t *testing.T) {
	db := setupDB(t)
	id := ids.NewUUIDv7()
	s := newTestStaging(id)
	s.State = StagingComplete
	s.TimeExpiresMs = 1000
	_ = InsertStaging(context.Background(), db, s)

	if err := MarkStagingDeletingIfExpired(context.Background(), db, id, 5000); err != nil {
		t.Fatal(err)
	}
	got, _ := GetStaging(context.Background(), db, id)
	if got.State != StagingDeleting {
		t.Errorf("state=%q, want deleting", got.State)
	}
}

func TestLookupStagingByContent(t *testing.T) {
	db := setupDB(t)
	id := ids.NewUUIDv7()
	s := newTestStaging(id)
	_ = InsertStaging(context.Background(), db, s)

	// Not findable while uploading
	_, err := LookupStagingByContent(context.Background(), db, "abc123", 1024)
	if err != ErrNotFound {
		t.Errorf("expected ErrNotFound for uploading, got %v", err)
	}

	// Complete it, then it should be findable
	_ = MarkStagingComplete(context.Background(), db, id, "abc123", 1024, 3000, 9000)
	got, err := LookupStagingByContent(context.Background(), db, "abc123", 1024)
	if err != nil {
		t.Fatal(err)
	}
	if got.StagingID != id {
		t.Errorf("got id %q, want %q", got.StagingID, id)
	}
}

func TestListExpiredStaging(t *testing.T) {
	db := setupDB(t)

	// Insert 2 expired complete, 1 not expired, 1 uploading
	for i := 0; i < 2; i++ {
		id := ids.NewUUIDv7()
		s := newTestStaging(id)
		_ = InsertStaging(context.Background(), db, s)
		_ = MarkStagingComplete(context.Background(), db, id, "sha"+ids.NewUUIDv7(), 1024, 1000, 500) // expires=500
	}
	{
		id := ids.NewUUIDv7()
		s := newTestStaging(id)
		_ = InsertStaging(context.Background(), db, s)
		_ = MarkStagingComplete(context.Background(), db, id, "sha"+ids.NewUUIDv7(), 1024, 1000, 99999) // far future
	}
	{
		id := ids.NewUUIDv7()
		s := newTestStaging(id)
		s.TimeExpiresMs = 500
		_ = InsertStaging(context.Background(), db, s) // uploading, expired
	}

	// List expired complete only
	expired, err := ListExpiredStaging(context.Background(), db, 1000, []StagingState{StagingComplete}, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(expired) != 2 {
		t.Errorf("expected 2 expired complete, got %d", len(expired))
	}

	// List expired uploading
	expiredUploading, _ := ListExpiredStaging(context.Background(), db, 1000, []StagingState{StagingUploading}, 100)
	if len(expiredUploading) != 1 {
		t.Errorf("expected 1 expired uploading, got %d", len(expiredUploading))
	}

	// List with downloaded_at set (testing sql.NullInt64)
	id2 := ids.NewUUIDv7()
	s2 := newTestStaging(id2)
	s2.DownloadedAt = sql.NullInt64{Int64: 999, Valid: true}
	_ = InsertStaging(context.Background(), db, s2)
	got, _ := GetStaging(context.Background(), db, id2)
	if !got.DownloadedAt.Valid || got.DownloadedAt.Int64 != 999 {
		t.Errorf("expected DownloadedAt=999, got %v", got.DownloadedAt)
	}
}
