package lettsconfig

import (
	"errors"
	"math/rand/v2"
	"testing"
)

func TestSelectByLaneOnly(t *testing.T) {
	c := &Config{
		Dugdales: []Dugdale{
			{ID: "s1", Host: "h", Lanes: map[string]LaneCfg{"normal": {Concurrency: 1}}},
			{ID: "s2", Host: "h", Lanes: map[string]LaneCfg{"other": {Concurrency: 1}}},
		},
	}
	cands := Candidates(c, "normal", nil)
	if len(cands) != 1 || cands[0].ID != "s1" {
		t.Fatalf("got %+v", cands)
	}
}

func TestSelectByLaneAndMatchAND(t *testing.T) {
	c := &Config{
		Dugdales: []Dugdale{
			{ID: "s1", Host: "h", Labels: []string{"prod"}, Lanes: map[string]LaneCfg{"normal": {}}},
			{ID: "s2", Host: "h", Labels: []string{"prod", "k"}, Lanes: map[string]LaneCfg{"normal": {}}},
			{ID: "s3", Host: "h", Labels: []string{"k"}, Lanes: map[string]LaneCfg{"normal": {}}},
		},
	}
	cands := Candidates(c, "normal", []string{"prod", "k"})
	if len(cands) != 1 || cands[0].ID != "s2" {
		t.Fatalf("got %+v, want only s2", cands)
	}
}

func TestSelectNoMatchReturnsAll(t *testing.T) {
	c := &Config{
		Dugdales: []Dugdale{
			{ID: "s1", Host: "h", Lanes: map[string]LaneCfg{"normal": {}}},
			{ID: "s2", Host: "h", Lanes: map[string]LaneCfg{"normal": {}}},
		},
	}
	cands := Candidates(c, "normal", nil)
	if len(cands) != 2 {
		t.Fatalf("got %d cands, want 2", len(cands))
	}
}

func TestSelectOnePickRandomDeterministic(t *testing.T) {
	c := &Config{
		Dugdales: []Dugdale{
			{ID: "s1", Host: "h", Lanes: map[string]LaneCfg{"normal": {}}},
			{ID: "s2", Host: "h", Lanes: map[string]LaneCfg{"normal": {}}},
			{ID: "s3", Host: "h", Lanes: map[string]LaneCfg{"normal": {}}},
		},
	}
	rng := rand.New(rand.NewPCG(42, 99))
	d, err := PickOne(c, "normal", nil, rng)
	if err != nil {
		t.Fatal(err)
	}
	picks := map[string]int{}
	for i := 0; i < 100; i++ {
		dd, _ := PickOne(c, "normal", nil, rng)
		picks[dd.ID]++
	}
	if len(picks) < 2 {
		t.Errorf("PickOne not random enough: %v (first pick was %s)", picks, d.ID)
	}
}

func TestSelectNoCandidatesErrors(t *testing.T) {
	c := &Config{Dugdales: []Dugdale{{ID: "s", Host: "h", Lanes: map[string]LaneCfg{"a": {}}}}}
	_, err := PickOne(c, "missing", nil, nil)
	var nc *NoCandidatesError
	if !errors.As(err, &nc) {
		t.Fatalf("expected NoCandidatesError, got %v", err)
	}
}
