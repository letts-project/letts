package storage

import (
	"context"
	"database/sql"
	"testing"
)

func TestSetGetAppliedConfig(t *testing.T) {
	db := setupDB(t)

	cfg := AppliedConfig{
		Data:      []byte(`{"mission_dir":"/var/missions","lanes":{"normal":{}}}`),
		AppliedAt: 1700000000000,
		Source:    sql.NullString{String: "dugdale.yaml", Valid: true},
	}
	if err := SetAppliedConfig(context.Background(), db, cfg); err != nil {
		t.Fatal(err)
	}
	got, err := GetAppliedConfig(context.Background(), db)
	if err != nil {
		t.Fatal(err)
	}
	if string(got.Data) != string(cfg.Data) {
		t.Errorf("got data %q, want %q", got.Data, cfg.Data)
	}
	if got.AppliedAt != cfg.AppliedAt {
		t.Errorf("got applied_at %d, want %d", got.AppliedAt, cfg.AppliedAt)
	}
	if !got.Source.Valid || got.Source.String != "dugdale.yaml" {
		t.Errorf("got source %v", got.Source)
	}
}

func TestGetAppliedConfigNotFound(t *testing.T) {
	db := setupDB(t)
	_, err := GetAppliedConfig(context.Background(), db)
	if err != ErrNotFound {
		t.Errorf("expected ErrNotFound on empty config, got %v", err)
	}
}

func TestSetAppliedConfigIsIdempotent(t *testing.T) {
	db := setupDB(t)

	cfg1 := AppliedConfig{
		Data:      []byte(`{"version":1}`),
		AppliedAt: 1000,
	}
	cfg2 := AppliedConfig{
		Data:      []byte(`{"version":2}`),
		AppliedAt: 2000,
	}
	if err := SetAppliedConfig(context.Background(), db, cfg1); err != nil {
		t.Fatal(err)
	}
	if err := SetAppliedConfig(context.Background(), db, cfg2); err != nil {
		t.Fatal(err)
	}
	got, err := GetAppliedConfig(context.Background(), db)
	if err != nil {
		t.Fatal(err)
	}
	// Should have cfg2 (latest)
	if string(got.Data) != `{"version":2}` {
		t.Errorf("expected version 2, got %q", got.Data)
	}
	if got.AppliedAt != 2000 {
		t.Errorf("expected applied_at=2000, got %d", got.AppliedAt)
	}
}
