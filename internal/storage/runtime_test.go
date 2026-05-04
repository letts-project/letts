package storage

import (
	"context"
	"testing"

	"letts/internal/ids"
)

func TestRuntimeRoundTrip(t *testing.T) {
	db := setupDB(t)
	id := ids.NewUUIDv7()
	_ = InsertMission(context.Background(), db, &Mission{
		ID: id, Kind: KindMission, Lane: "x", MissionName: "m",
		Status: StatusQueued, Input: []byte("{}"), InputFingerprint: "f",
		TimeCreatedMs: 1,
	})
	r := &MissionRuntime{
		MissionID:           id,
		MissionDir:          "/var/missions",
		CommandTemplate:     `["php", "{mission}"]`,
		MissionPathTemplate: "{mission}.php",
		ValidateMissionFile: true,
	}
	if err := InsertRuntime(context.Background(), db, r); err != nil {
		t.Fatal(err)
	}
	got, err := GetRuntime(context.Background(), db, id)
	if err != nil {
		t.Fatal(err)
	}
	if got.MissionDir != "/var/missions" {
		t.Errorf("got MissionDir %q", got.MissionDir)
	}
	if got.CommandTemplate != `["php", "{mission}"]` {
		t.Errorf("got CommandTemplate %q", got.CommandTemplate)
	}
	if !got.ValidateMissionFile {
		t.Errorf("expected ValidateMissionFile=true")
	}
}

func TestGetRuntimeNotFound(t *testing.T) {
	db := setupDB(t)
	_, err := GetRuntime(context.Background(), db, "nonexistent")
	if err != ErrNotFound {
		t.Errorf("expected ErrNotFound, got %v", err)
	}
}
