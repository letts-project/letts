package apply

import "reflect"

// Diff describes what changed between two AppliedState values.
type Diff struct {
	MissionDirChanged bool
	LabelsChanged     bool
	RuntimeChanged    bool
	LanesAdded        []string
	LanesRemoved      []string
	LanesResized      []string
}

// ComputeDiff returns the diff between current and desired states.
func ComputeDiff(current, desired AppliedState) Diff {
	var d Diff

	if current.MissionDir != desired.MissionDir {
		d.MissionDirChanged = true
	}
	if !reflect.DeepEqual(current.Labels, desired.Labels) {
		d.LabelsChanged = true
	}
	if !reflect.DeepEqual(current.Runtime, desired.Runtime) {
		d.RuntimeChanged = true
	}

	// Lanes added / removed / resized. Paused-only transitions count as
	// LanesResized too — paused is a "hot" property reconciled
	// without restart, so operators auditing the apply diff need to see
	// it land somewhere; LanesResized is the right bucket.
	for name, dc := range desired.Lanes {
		cc, ok := current.Lanes[name]
		if !ok {
			d.LanesAdded = append(d.LanesAdded, name)
			continue
		}
		if cc.Concurrency != dc.Concurrency || cc.Paused != dc.Paused {
			d.LanesResized = append(d.LanesResized, name)
		}
	}
	for name := range current.Lanes {
		if _, ok := desired.Lanes[name]; !ok {
			d.LanesRemoved = append(d.LanesRemoved, name)
		}
	}

	return d
}
