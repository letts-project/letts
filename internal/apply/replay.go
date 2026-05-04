package apply

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"

	"letts/internal/lane"
	"letts/internal/storage"
)

// ReplayFromDB reads the stored AppliedState (if any) and reconciles the
// lane manager unconditionally. Used at startup to recreate lane runners
// after a restart. Returns nil and a no-op if no config is stored yet.
func ReplayFromDB(ctx context.Context, db *sql.DB, mgr *lane.Manager) error {
	existing, err := storage.GetAppliedConfig(ctx, db)
	if errors.Is(err, storage.ErrNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	if existing == nil {
		return nil
	}
	var state AppliedState
	if err := json.Unmarshal(existing.Data, &state); err != nil {
		return err
	}
	specs := make([]lane.LaneSpec, 0, len(state.Lanes))
	for name, c := range state.Lanes {
		specs = append(specs, lane.LaneSpec{
			Name: name, Concurrency: c.Concurrency, Paused: c.Paused,
		})
	}
	mgr.Apply(specs)
	return nil
}
