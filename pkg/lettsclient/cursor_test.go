package lettsclient

import (
	b64 "encoding/base64"
	"strings"
	"testing"
)

func TestListCursorRoundTrip(t *testing.T) {
	c := ListCursor{TimeCreatedMs: 1714600000123, MissionID: "0192a8b3-d2c1-7abc-bad0-1234567890ab"}
	s := EncodeListCursor(c)
	got, err := DecodeListCursor(s)
	if err != nil {
		t.Fatal(err)
	}
	if got != c {
		t.Errorf("created round-trip: got %+v want %+v", got, c)
	}

	f := ListCursor{TimeFinishedMs: 1714600099999, MissionID: "0192a8b4-d2c1-7abc-bad0-1234567890ac"}
	if got, _ := DecodeListCursor(EncodeListCursor(f)); got != f {
		t.Errorf("finished round-trip: got %+v want %+v", got, f)
	}

	if got, err := DecodeListCursor(""); err != nil || got != (ListCursor{}) {
		t.Errorf("empty decode: got %+v err %v", got, err)
	}

	raw, err := b64.RawURLEncoding.DecodeString(s)
	if err != nil {
		t.Fatal(err)
	}
	wire := string(raw)
	if strings.Contains(wire, "time_finished") {
		t.Errorf("created cursor must omit time_finished, wire=%s", wire)
	}
	if !strings.Contains(wire, "time_created") || !strings.Contains(wire, "mission_id") {
		t.Errorf("created cursor missing fields, wire=%s", wire)
	}
}
