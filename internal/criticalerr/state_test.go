package criticalerr

import "testing"

func TestTripFirstWins(t *testing.T) {
	Reset()
	t.Cleanup(Reset)

	Trip(Detail{Kind: "k1", MissionID: "m1", Op: "first"})
	Trip(Detail{Kind: "k2", MissionID: "m2", Op: "second"})

	d, ok := Get()
	if !ok {
		t.Fatal("expected tripped")
	}
	if d.MissionID != "m1" || d.Op != "first" {
		t.Errorf("first-trip not preserved: got %+v", d)
	}
}

func TestGetReturnsFalseUntilTripped(t *testing.T) {
	Reset()
	t.Cleanup(Reset)

	if _, ok := Get(); ok {
		t.Errorf("Get should return false before any Trip")
	}
	Trip(Detail{Kind: "x", MissionID: "m"})
	if _, ok := Get(); !ok {
		t.Errorf("Get should return true after Trip")
	}
}
