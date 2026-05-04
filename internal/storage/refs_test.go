package storage

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"letts/internal/ids"
)

func TestInsertRefAndGetByMission(t *testing.T) {
	db := setupDB(t)
	mID := ids.NewUUIDv7()
	sID := ids.NewUUIDv7()

	_ = InsertMission(context.Background(), db, &Mission{
		ID: mID, Kind: KindMission, Lane: "x", MissionName: "m",
		Status: StatusQueued, Input: []byte("{}"), InputFingerprint: "f",
		TimeCreatedMs: 1,
	})
	_ = InsertStaging(context.Background(), db, &StagingFile{
		StagingID: sID, State: StagingComplete, Sha256: "sha1", Size: 100,
		Path: "p", TimeCreatedMs: 1, TimeUpdatedMs: 1, TimeExpiresMs: 9999,
	})

	ref := StagingRef{MissionID: mID, StagingID: sID, RefKind: RefInput, Role: "input1"}
	if err := InsertRef(context.Background(), db, ref); err != nil {
		t.Fatal(err)
	}

	refs, err := RefsByMission(context.Background(), db, mID)
	if err != nil {
		t.Fatal(err)
	}
	if len(refs) != 1 {
		t.Fatalf("expected 1 ref, got %d", len(refs))
	}
	if refs[0].RefKind != RefInput {
		t.Errorf("got RefKind %q", refs[0].RefKind)
	}
}

func TestRefsByStaging(t *testing.T) {
	db := setupDB(t)
	mID1 := ids.NewUUIDv7()
	mID2 := ids.NewUUIDv7()
	sID := ids.NewUUIDv7()

	for _, mID := range []string{mID1, mID2} {
		_ = InsertMission(context.Background(), db, &Mission{
			ID: mID, Kind: KindMission, Lane: "x", MissionName: "m",
			Status: StatusQueued, Input: []byte("{}"), InputFingerprint: "f",
			TimeCreatedMs: 1,
		})
	}
	_ = InsertStaging(context.Background(), db, &StagingFile{
		StagingID: sID, State: StagingComplete, Sha256: "sha1", Size: 100,
		Path: "p", TimeCreatedMs: 1, TimeUpdatedMs: 1, TimeExpiresMs: 9999,
	})

	_ = InsertRef(context.Background(), db, StagingRef{MissionID: mID1, StagingID: sID, RefKind: RefInput, Role: ""})
	_ = InsertRef(context.Background(), db, StagingRef{MissionID: mID2, StagingID: sID, RefKind: RefScript, Role: ""})

	refs, err := RefsByStaging(context.Background(), db, sID)
	if err != nil {
		t.Fatal(err)
	}
	if len(refs) != 2 {
		t.Errorf("expected 2 refs, got %d", len(refs))
	}
}

func TestRefUniqueRoleConstraint(t *testing.T) {
	db := setupDB(t)
	mID := ids.NewUUIDv7()
	sID1 := ids.NewUUIDv7()
	sID2 := ids.NewUUIDv7()

	_ = InsertMission(context.Background(), db, &Mission{
		ID: mID, Kind: KindMission, Lane: "x", MissionName: "m",
		Status: StatusQueued, Input: []byte("{}"), InputFingerprint: "f",
		TimeCreatedMs: 1,
	})
	for _, sID := range []string{sID1, sID2} {
		_ = InsertStaging(context.Background(), db, &StagingFile{
			StagingID: sID, State: StagingComplete, Sha256: sID, Size: 100,
			Path: "p", TimeCreatedMs: 1, TimeUpdatedMs: 1, TimeExpiresMs: 9999,
		})
	}

	_ = InsertRef(context.Background(), db, StagingRef{MissionID: mID, StagingID: sID1, RefKind: RefInput, Role: "cover"})
	// Same mission+ref_kind+role but different staging_id → violates msr_unique_role
	err := InsertRef(context.Background(), db, StagingRef{MissionID: mID, StagingID: sID2, RefKind: RefInput, Role: "cover"})
	if err == nil {
		t.Errorf("expected unique constraint error for duplicate role")
	}
}

func TestRecalcStagingTTLRunningMission(t *testing.T) {
	db := setupDB(t)
	mID := ids.NewUUIDv7()
	sID := ids.NewUUIDv7()

	_ = InsertMission(context.Background(), db, &Mission{
		ID: mID, Kind: KindMission, Lane: "x", MissionName: "m",
		Status: StatusRunning, Input: []byte("{}"), InputFingerprint: "f",
		TimeCreatedMs: 1000,
	})
	_ = InsertStaging(context.Background(), db, &StagingFile{
		StagingID: sID, State: StagingComplete, Sha256: "sha1", Size: 100,
		Path: "p", TimeCreatedMs: 1000, TimeUpdatedMs: 1000, TimeExpiresMs: 9999,
	})
	_ = InsertRef(context.Background(), db, StagingRef{MissionID: mID, StagingID: sID, RefKind: RefInput, Role: ""})

	policy := TTLPolicy{
		MissionSuccess: 24 * time.Hour,
		MissionFailed:  1 * time.Hour,
		ExecSuccess:    12 * time.Hour,
		ExecFailed:     30 * time.Minute,
		StagingTTL:     1 * time.Hour,
		DownloadGrace:  15 * time.Minute,
	}
	exp, err := RecalcStagingTTL(context.Background(), db, sID, policy, 2000)
	if err != nil {
		t.Fatal(err)
	}
	// Mission is running → infinity
	if exp != maxInt64 {
		t.Errorf("expected infinity (%d), got %d", maxInt64, exp)
	}
}

func TestRecalcStagingTTLDoneMission(t *testing.T) {
	db := setupDB(t)
	mID := ids.NewUUIDv7()
	sID := ids.NewUUIDv7()

	finishedMs := int64(5000)
	_ = InsertMission(context.Background(), db, &Mission{
		ID: mID, Kind: KindMission, Lane: "x", MissionName: "m",
		Status: StatusDone, Outcome: sql.NullString{String: "success", Valid: true},
		Input: []byte("{}"), InputFingerprint: "f",
		TimeCreatedMs:  1000,
		TimeFinishedMs: sql.NullInt64{Int64: finishedMs, Valid: true},
	})
	_ = InsertStaging(context.Background(), db, &StagingFile{
		StagingID: sID, State: StagingComplete, Sha256: "sha1", Size: 100,
		Path: "p", TimeCreatedMs: 1000, TimeUpdatedMs: 1000, TimeExpiresMs: 9999,
	})
	_ = InsertRef(context.Background(), db, StagingRef{MissionID: mID, StagingID: sID, RefKind: RefOutput, Role: "result"})

	successTTL := 24 * time.Hour
	policy := TTLPolicy{
		MissionSuccess: successTTL,
		MissionFailed:  1 * time.Hour,
		ExecSuccess:    12 * time.Hour,
		ExecFailed:     30 * time.Minute,
		StagingTTL:     1 * time.Hour,
		DownloadGrace:  15 * time.Minute,
	}
	exp, err := RecalcStagingTTL(context.Background(), db, sID, policy, 6000)
	if err != nil {
		t.Fatal(err)
	}
	expected := finishedMs + successTTL.Milliseconds()
	if exp != expected {
		t.Errorf("expected %d, got %d", expected, exp)
	}
}

func TestRecalcStagingTTLNoRefs(t *testing.T) {
	db := setupDB(t)
	sID := ids.NewUUIDv7()
	createdMs := int64(1000)
	_ = InsertStaging(context.Background(), db, &StagingFile{
		StagingID: sID, State: StagingComplete, Sha256: "sha1", Size: 100,
		Path: "p", TimeCreatedMs: createdMs, TimeUpdatedMs: createdMs, TimeExpiresMs: 9999,
	})

	stagingTTL := 2 * time.Hour
	policy := TTLPolicy{
		MissionSuccess: 24 * time.Hour,
		MissionFailed:  1 * time.Hour,
		ExecSuccess:    12 * time.Hour,
		ExecFailed:     30 * time.Minute,
		StagingTTL:     stagingTTL,
		DownloadGrace:  15 * time.Minute,
	}
	exp, err := RecalcStagingTTL(context.Background(), db, sID, policy, 2000)
	if err != nil {
		t.Fatal(err)
	}
	expected := createdMs + stagingTTL.Milliseconds()
	if exp != expected {
		t.Errorf("expected %d, got %d", expected, exp)
	}
}

func TestTriggerSetsTimeExpiresZeroOnRefDelete(t *testing.T) {
	db := setupDB(t)
	mID := ids.NewUUIDv7()
	sID := ids.NewUUIDv7()

	_ = InsertMission(context.Background(), db, &Mission{
		ID: mID, Kind: KindMission, Lane: "x", MissionName: "m",
		Status: StatusDone, Outcome: sql.NullString{String: "success", Valid: true},
		Input: []byte("{}"), InputFingerprint: "f",
		TimeCreatedMs: 1,
	})
	_ = InsertStaging(context.Background(), db, &StagingFile{
		StagingID: sID, State: StagingComplete, Sha256: "sha1", Size: 100,
		Path: "p", TimeCreatedMs: 1, TimeUpdatedMs: 1, TimeExpiresMs: 9999,
	})
	_ = InsertRef(context.Background(), db, StagingRef{MissionID: mID, StagingID: sID, RefKind: RefInput, Role: ""})

	// Verify time_expires is nonzero initially
	s, _ := GetStaging(context.Background(), db, sID)
	if s.TimeExpiresMs == 0 {
		t.Fatal("expected non-zero time_expires before delete")
	}

	// Delete mission → CASCADE deletes ref → trigger zeros time_expires
	_, _ = db.ExecContext(context.Background(), "DELETE FROM missions WHERE mission_id=?", mID)

	// time_expires should now be 0 (set by trigger)
	s2, err := GetStaging(context.Background(), db, sID)
	if err != nil {
		t.Fatal(err)
	}
	if s2.TimeExpiresMs != 0 {
		t.Errorf("expected time_expires=0 after CASCADE delete, got %d", s2.TimeExpiresMs)
	}
}

func TestFindStagingNeedingRecalc(t *testing.T) {
	db := setupDB(t)

	// Insert staging with time_expires=0
	for i := 0; i < 3; i++ {
		sID := ids.NewUUIDv7()
		_ = InsertStaging(context.Background(), db, &StagingFile{
			StagingID: sID, State: StagingComplete, Sha256: sID, Size: 100,
			Path: "p", TimeCreatedMs: 1, TimeUpdatedMs: 1, TimeExpiresMs: 0,
		})
	}
	// One with nonzero
	sID := ids.NewUUIDv7()
	_ = InsertStaging(context.Background(), db, &StagingFile{
		StagingID: sID, State: StagingComplete, Sha256: sID + "x", Size: 100,
		Path: "p", TimeCreatedMs: 1, TimeUpdatedMs: 1, TimeExpiresMs: 9999,
	})

	ids2, err := FindStagingNeedingRecalc(context.Background(), db, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(ids2) != 3 {
		t.Errorf("expected 3 needing recalc, got %d", len(ids2))
	}
}
