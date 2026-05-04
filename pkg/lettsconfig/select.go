package lettsconfig

import (
	"fmt"
	"math/rand/v2"
)

// NoCandidatesError signals that no dugdale matched the filter.
type NoCandidatesError struct {
	Lane  string
	Match []string
}

func (e *NoCandidatesError) Error() string {
	if len(e.Match) == 0 {
		return fmt.Sprintf("no dugdale exposes lane %q", e.Lane)
	}
	return fmt.Sprintf("no dugdale exposes lane %q with labels %v", e.Lane, e.Match)
}

// Candidates returns dugdales that expose the lane and (if match non-empty)
// carry ALL listed labels. Caller passes match as the effective filter:
// either the --match flag, or selector.match, or nil to skip filtering.
func Candidates(c *Config, lane string, match []string) []*Dugdale {
	out := make([]*Dugdale, 0, len(c.Dugdales))
	for i := range c.Dugdales {
		d := &c.Dugdales[i]
		if !d.HasLane(lane) {
			continue
		}
		if !hasAllLabels(d, match) {
			continue
		}
		out = append(out, d)
	}
	return out
}

func hasAllLabels(d *Dugdale, match []string) bool {
	for _, l := range match {
		if !d.HasLabel(l) {
			return false
		}
	}
	return true
}

// PickOne returns one random candidate or NoCandidatesError.
// Pass rng = nil to use the package-level default rand source.
func PickOne(c *Config, lane string, match []string, rng *rand.Rand) (*Dugdale, error) {
	cands := Candidates(c, lane, match)
	if len(cands) == 0 {
		return nil, &NoCandidatesError{Lane: lane, Match: match}
	}
	if len(cands) == 1 {
		return cands[0], nil
	}
	var idx int
	if rng == nil {
		idx = rand.IntN(len(cands))
	} else {
		idx = rng.IntN(len(cands))
	}
	return cands[idx], nil
}
