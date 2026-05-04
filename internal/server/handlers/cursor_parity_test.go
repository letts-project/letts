package handlers

import (
	"testing"

	"letts/internal/storage"
	"letts/pkg/lettsclient"
)

// TestCursorWireParityWithClient locks the dugdale's list-cursor encoding
// (encodeCursor over storage.Cursor) byte-identical to the public client codec
// (lettsclient.EncodeListCursor over ListCursor) that arby uses to build
// per-host merge cursors. If either side drifts (a tag rename, StdEncoding vs
// RawURLEncoding, field reorder), arby pagination silently breaks — this test
// fails loudly instead.
func TestCursorWireParityWithClient(t *testing.T) {
	cases := []struct {
		name   string
		server *storage.Cursor
		client lettsclient.ListCursor
	}{
		{
			"created",
			&storage.Cursor{TimeCreatedMs: 1714600000123, MissionID: "0192a8b3-d2c1-7abc-bad0-1234567890ab"},
			lettsclient.ListCursor{TimeCreatedMs: 1714600000123, MissionID: "0192a8b3-d2c1-7abc-bad0-1234567890ab"},
		},
		{
			"finished",
			&storage.Cursor{TimeFinishedMs: 1714600099999, MissionID: "0192a8b4-d2c1-7abc-bad0-1234567890ac"},
			lettsclient.ListCursor{TimeFinishedMs: 1714600099999, MissionID: "0192a8b4-d2c1-7abc-bad0-1234567890ac"},
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := encodeCursor(c.server)
			want := lettsclient.EncodeListCursor(c.client)
			if got != want {
				t.Errorf("cursor wire mismatch:\n handler encodeCursor   = %q\n client EncodeListCursor = %q", got, want)
			}
		})
	}
}
