package lettsclient

import (
	"encoding/base64"
	"encoding/json"
)

// ListCursor mirrors the dugdale's GET /v1/missions cursor wire format
// (internal/storage.Cursor and internal/server/handlers encodeCursor): compact
// JSON, base64url no-pad. The active time field depends on the listing order
// — TimeCreatedMs for default/created order, TimeFinishedMs for order=finished.
// MissionID is the tiebreaker and is always present.
//
// arby needs this to build a *per-host* cursor pointing at the last item it
// accepted during a k-way merge (the host's own returned next_cursor points
// past items arby dropped from the merged top-N, so it cannot be reused).
type ListCursor struct {
	TimeCreatedMs  int64  `json:"time_created,omitempty"`
	TimeFinishedMs int64  `json:"time_finished,omitempty"`
	MissionID      string `json:"mission_id"`
}

// EncodeListCursor produces the opaque cursor string the dugdale accepts as
// ?cursor=. Matches the daemon's base64.RawURLEncoding(json.Marshal(...)).
func EncodeListCursor(c ListCursor) string {
	b, _ := json.Marshal(c)
	return base64.RawURLEncoding.EncodeToString(b)
}

// DecodeListCursor parses a cursor string. Empty string → zero ListCursor, nil
// error (means "from the beginning").
func DecodeListCursor(s string) (ListCursor, error) {
	if s == "" {
		return ListCursor{}, nil
	}
	raw, err := base64.RawURLEncoding.DecodeString(s)
	if err != nil {
		return ListCursor{}, err
	}
	var c ListCursor
	if err := json.Unmarshal(raw, &c); err != nil {
		return ListCursor{}, err
	}
	return c, nil
}
