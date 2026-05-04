package storage

import (
	"context"
	"testing"

	"letts/internal/ids"
)

func TestFinalizeIntentLifecycle(t *testing.T) {
	db := setupDB(t)
	id := ids.NewUUIDv7()
	// Need a missions row first (FK).
	_ = InsertMission(context.Background(), db, &Mission{
		ID: id, Kind: KindMission, Lane: "x", MissionName: "x", Status: StatusRunning,
		Input: []byte("{}"), InputFingerprint: "f", TimeCreatedMs: 1,
	})
	intent := FinalizeIntent{
		MissionID: id, Phase: PhasePrepared, Outcome: string(OutcomeSuccess),
		Outputs: []byte(`[]`), DoneSeq: 5, DoneEvent: `{"seq":5}`, TimeCreatedMs: 1,
	}
	if err := InsertFinalizeIntent(context.Background(), db, &intent); err != nil {
		t.Fatal(err)
	}
	if err := UpdateFinalizePhase(context.Background(), db, id, PhaseCommitting); err != nil {
		t.Fatal(err)
	}
	prepared, err := ListFinalizeIntents(context.Background(), db, PhasePrepared)
	if err != nil {
		t.Fatal(err)
	}
	if len(prepared) != 0 {
		t.Errorf("after Update should be 0 prepared; got %d", len(prepared))
	}
	committing, _ := ListFinalizeIntents(context.Background(), db, PhaseCommitting)
	if len(committing) != 1 {
		t.Errorf("got %d committing", len(committing))
	}
	if err := DeleteFinalizeIntent(context.Background(), db, id); err != nil {
		t.Fatal(err)
	}
	// After delete, list should be empty
	all, _ := ListFinalizeIntents(context.Background(), db, PhaseCommitting)
	if len(all) != 0 {
		t.Errorf("expected 0 after delete, got %d", len(all))
	}
}

func TestGetFinalizeIntent(t *testing.T) {
	db := setupDB(t)
	id := ids.NewUUIDv7()
	_ = InsertMission(context.Background(), db, &Mission{
		ID: id, Kind: KindMission, Lane: "x", MissionName: "x", Status: StatusRunning,
		Input: []byte("{}"), InputFingerprint: "f", TimeCreatedMs: 1,
	})
	intent := FinalizeIntent{
		MissionID: id, Phase: PhasePrepared, Outcome: "failed",
		Outputs: []byte(`[]`), DoneSeq: 3, DoneEvent: `{"seq":3}`, TimeCreatedMs: 100,
	}
	_ = InsertFinalizeIntent(context.Background(), db, &intent)

	got, err := GetFinalizeIntent(context.Background(), db, id)
	if err != nil {
		t.Fatal(err)
	}
	if got.Outcome != "failed" {
		t.Errorf("got outcome %q", got.Outcome)
	}
	if got.DoneSeq != 3 {
		t.Errorf("got DoneSeq %d", got.DoneSeq)
	}
}

func TestGetFinalizeIntentNotFound(t *testing.T) {
	db := setupDB(t)
	_, err := GetFinalizeIntent(context.Background(), db, "nonexistent")
	if err != ErrNotFound {
		t.Errorf("expected ErrNotFound, got %v", err)
	}
}

func TestQueryAllFinalizeIntents(t *testing.T) {
	db := setupDB(t)
	for i := 0; i < 3; i++ {
		id := ids.NewUUIDv7()
		_ = InsertMission(context.Background(), db, &Mission{
			ID: id, Kind: KindMission, Lane: "x", MissionName: "x", Status: StatusRunning,
			Input: []byte("{}"), InputFingerprint: "f", TimeCreatedMs: int64(i + 1),
		})
		_ = InsertFinalizeIntent(context.Background(), db, &FinalizeIntent{
			MissionID: id, Phase: PhasePrepared, Outcome: "success",
			Outputs: []byte(`[]`), DoneSeq: int64(i), DoneEvent: `{}`, TimeCreatedMs: int64(i + 1),
		})
	}
	all, err := QueryAllFinalizeIntents(context.Background(), db)
	if err != nil {
		t.Fatal(err)
	}
	if len(all) != 3 {
		t.Errorf("expected 3, got %d", len(all))
	}
}
