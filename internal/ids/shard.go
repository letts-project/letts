package ids

import (
	"path/filepath"
)

// ShardPath returns the two-level shard prefix "<sh1>/<sh2>" for a UUIDv7.
// Used to fan out output/, staging/, and work/ directories under data_dir.
// Returns ErrInvalidUUIDv7 when id is not a valid UUIDv7.
func ShardPath(id string) (string, error) {
	if !ValidateUUIDv7(id) {
		return "", ErrInvalidUUIDv7
	}
	return filepath.Join(id[0:2], id[2:4]), nil
}
