package mission

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"letts/internal/eventfile"
	"letts/internal/ids"
	"letts/internal/storage"
)

func TestKillQueuedMessages(t *testing.T) {
	cases := []struct {
		reason  string
		wantMsg string
	}{
		{"killed_by_api", "killed via the kill API before it started"},
		{"lane_removed", `killed because lane "reaped" was removed from the config before it started`},
	}
	for _, tc := range cases {
		t.Run(tc.reason, func(t *testing.T) {
			db := openTestDB(t)
			dataDir := t.TempDir()
			m := &storage.Mission{
				ID:               ids.NewUUIDv7(),
				Kind:             storage.KindMission,
				Lane:             "reaped",
				MissionName:      "Queued",
				Status:           storage.StatusQueued,
				Input:            []byte(`{}`),
				InputFingerprint: "fp",
				TimeCreatedMs:    time.Now().UnixMilli(),
			}
			if err := storage.InsertMission(context.Background(), db, m); err != nil {
				t.Fatalf("insert mission: %v", err)
			}
			shard, _ := ids.ShardPath(m.ID)
			parentDir := filepath.Join(dataDir, "output", shard)
			if err := os.MkdirAll(parentDir, 0o755); err != nil {
				t.Fatalf("mkdir: %v", err)
			}
			w, err := eventfile.Create(parentDir, m.ID)
			if err != nil {
				t.Fatalf("eventfile create: %v", err)
			}
			if _, err := w.Append(eventfile.KindQueued, map[string]any{"time_created": m.TimeCreatedMs}, true); err != nil {
				t.Fatalf("append queued: %v", err)
			}
			_ = w.Close()

			if err := KillQueued(context.Background(), dataDir, db, m, tc.reason); err != nil {
				t.Fatalf("KillQueued: %v", err)
			}

			got := loadMission(t, db, m.ID)
			if got.Outcome.String != "killed" || got.FailReason.String != tc.reason {
				t.Errorf("outcome=%q reason=%q, want killed/%s", got.Outcome.String, got.FailReason.String, tc.reason)
			}
			if got.FailMessage.String != tc.wantMsg {
				t.Errorf("FailMessage=%q, want %q", got.FailMessage.String, tc.wantMsg)
			}
			events := loadEvents(t, dataDir, m.ID)
			done := events[len(events)-1]
			if done["event"] != "done" || done["fail_reason"] != tc.reason || done["fail_message"] != tc.wantMsg {
				t.Errorf("done event=%v, want fail_reason=%s fail_message=%q", done, tc.reason, tc.wantMsg)
			}
		})
	}
}
