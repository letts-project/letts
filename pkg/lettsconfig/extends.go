package lettsconfig

import "fmt"

// ResolveExtends walks each Dugdale with a non-empty Extends and merges
// it with the named Template. Mutates the Config in place.
//
// After ResolveExtends each Dugdale carries the fully-resolved fields
// it inherits; further passes (env substitution, validation) can ignore
// templates and operate on Dugdales alone.
func ResolveExtends(c *Config) error {
	for i := range c.Dugdales {
		d := &c.Dugdales[i]
		if d.Extends == "" {
			continue
		}
		t, ok := c.Templates[d.Extends]
		if !ok {
			return fmt.Errorf("dugdales[%d] (%q): extends unknown template %q", i, d.ID, d.Extends)
		}
		mergeDugdaleWithTemplate(d, &t)
	}
	return nil
}

func mergeDugdaleWithTemplate(d *Dugdale, t *Template) {
	// Scalar inheritance: template wins only when dugdale's field is the zero value.
	if d.MissionDir == "" {
		d.MissionDir = t.MissionDir
	}
	if d.Token == "" {
		d.Token = t.Token
	}
	if d.AdminToken == "" {
		d.AdminToken = t.AdminToken
	}
	if d.ExecToken == "" {
		d.ExecToken = t.ExecToken
	}
	// Labels: dugdale replaces template if specified.
	if d.Labels == nil {
		d.Labels = append([]string(nil), t.Labels...)
	}
	// Runtime: deep-merge field-by-field.
	if d.Runtime.MissionPathTemplate == "" {
		d.Runtime.MissionPathTemplate = t.Runtime.MissionPathTemplate
	}
	if d.Runtime.CommandTemplate == nil {
		d.Runtime.CommandTemplate = append([]string(nil), t.Runtime.CommandTemplate...)
	}
	// ValidateMissionFile: bool zero-value is false; yaml.v3 can't tell
	// "user wrote false" from "user didn't write the key".
	// The load.go AST walk sets runtimeValidateMissionFileSet whenever
	// the YAML carried the key for this dugdale, so we can:
	//   - dugdale explicit (true or false) → wins
	//   - dugdale unset → inherit template's value
	if !d.runtimeValidateMissionFileSet {
		d.Runtime.ValidateMissionFile = t.Runtime.ValidateMissionFile
	}
	// Lanes: deep-merge. dugdale wins on collision; absent keys inherit
	// FROM the template unless the dugdale's YAML explicitly nullified
	// the name (`lanes: <name>: null`) — that suppresses the inheritance
	// without redefining the rest of the template's lanes.
	if d.Lanes == nil {
		d.Lanes = map[string]LaneCfg{}
	}
	nullified := make(map[string]struct{}, len(d.nullifiedLanes))
	for _, name := range d.nullifiedLanes {
		nullified[name] = struct{}{}
	}
	for name, lc := range t.Lanes {
		if _, ok := d.Lanes[name]; ok {
			continue
		}
		if _, deleted := nullified[name]; deleted {
			continue
		}
		d.Lanes[name] = lc
	}
}
