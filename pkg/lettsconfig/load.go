package lettsconfig

import (
	"bytes"
	"fmt"

	"gopkg.in/yaml.v3"
)

// Load parses raw YAML bytes into a Config. Does not apply extends,
// env substitution, or validation — call LoadAndResolve for that pipeline.
func Load(b []byte) (*Config, error) {
	var c Config
	dec := yaml.NewDecoder(bytes.NewReader(b))
	dec.KnownFields(true)
	if err := dec.Decode(&c); err != nil {
		return nil, fmt.Errorf("yaml decode: %w", err)
	}
	// Second-pass scan: record dugdales[].lanes.<name>=null entries as
	// nullifiedLanes sentinels so ResolveExtends can drop the matching
	// inherited template lane. The struct-decode
	// above already populated everything else; KnownFields(true) ran on
	// it, so this scan only needs to recognise the null pattern. Also
	// records whether runtime.validate_mission_file was explicitly
	// present so the bool-zero ambiguity can be resolved.
	if err := extractNullifiedLanes(&c, b); err != nil {
		return nil, err
	}
	if err := extractRuntimeOverrides(&c, b); err != nil {
		return nil, err
	}
	if err := extractNullifiedProxy(&c, b); err != nil {
		return nil, err
	}
	return &c, nil
}

// extractNullifiedProxy walks the raw YAML once more and flags each Dugdale
// whose `proxy:` key was set explicitly to null. yaml.v3 decodes that null to
// the empty string (the zero value), indistinguishable from an absent proxy, so
// the AST is the only place to tell "delete the inherited proxy" (connect
// directly) from "inherit the template's proxy".
func extractNullifiedProxy(c *Config, b []byte) error {
	var root yaml.Node
	if err := yaml.Unmarshal(b, &root); err != nil {
		return nil // already errored above
	}
	if root.Kind != yaml.DocumentNode || len(root.Content) == 0 {
		return nil
	}
	top := root.Content[0]
	if top.Kind != yaml.MappingNode {
		return nil
	}
	var dugdalesNode *yaml.Node
	for i := 0; i+1 < len(top.Content); i += 2 {
		if top.Content[i].Value == "dugdales" {
			dugdalesNode = top.Content[i+1]
			break
		}
	}
	if dugdalesNode == nil || dugdalesNode.Kind != yaml.SequenceNode {
		return nil
	}
	for i, dnode := range dugdalesNode.Content {
		if i >= len(c.Dugdales) || dnode.Kind != yaml.MappingNode {
			continue
		}
		for j := 0; j+1 < len(dnode.Content); j += 2 {
			k, v := dnode.Content[j], dnode.Content[j+1]
			if k.Value != "proxy" {
				continue
			}
			if v.Tag == "!!null" || (v.Kind == yaml.ScalarNode && v.Value == "" && v.Tag == "") {
				c.Dugdales[i].proxyNullified = true
			}
			break
		}
	}
	return nil
}

// extractRuntimeOverrides walks the AST once more to flag each Dugdale
// where runtime.validate_mission_file was explicitly set in YAML.
// Without this, the bool zero-value (false) is indistinguishable from
// "user didn't write the key", so the extends merge can't honor an
// explicit false from dugdale over an inherited true from template.
func extractRuntimeOverrides(c *Config, b []byte) error {
	var root yaml.Node
	if err := yaml.Unmarshal(b, &root); err != nil {
		return nil
	}
	if root.Kind != yaml.DocumentNode || len(root.Content) == 0 {
		return nil
	}
	top := root.Content[0]
	if top.Kind != yaml.MappingNode {
		return nil
	}
	var dugdalesNode *yaml.Node
	for i := 0; i+1 < len(top.Content); i += 2 {
		if top.Content[i].Value == "dugdales" {
			dugdalesNode = top.Content[i+1]
			break
		}
	}
	if dugdalesNode == nil || dugdalesNode.Kind != yaml.SequenceNode {
		return nil
	}
	for i, dnode := range dugdalesNode.Content {
		if i >= len(c.Dugdales) || dnode.Kind != yaml.MappingNode {
			continue
		}
		var runtimeNode *yaml.Node
		for j := 0; j+1 < len(dnode.Content); j += 2 {
			if dnode.Content[j].Value == "runtime" {
				runtimeNode = dnode.Content[j+1]
				break
			}
		}
		if runtimeNode == nil || runtimeNode.Kind != yaml.MappingNode {
			continue
		}
		for k := 0; k+1 < len(runtimeNode.Content); k += 2 {
			if runtimeNode.Content[k].Value == "validate_mission_file" {
				c.Dugdales[i].runtimeValidateMissionFileSet = true
				break
			}
		}
	}
	return nil
}

// extractNullifiedLanes walks the raw YAML once more and populates the
// nullifiedLanes slice on each Dugdale for any `dugdales[i].lanes.<name>: null`
// entry. yaml.v3's struct-tag decode collapses null to zero-value LaneCfg{},
// so distinguishing "user typed null" from "user wrote concurrency: 0" needs
// a second look at the AST.
func extractNullifiedLanes(c *Config, b []byte) error {
	var root yaml.Node
	if err := yaml.Unmarshal(b, &root); err != nil {
		return nil // already errored above
	}
	if root.Kind != yaml.DocumentNode || len(root.Content) == 0 {
		return nil
	}
	top := root.Content[0]
	if top.Kind != yaml.MappingNode {
		return nil
	}
	// Find the "dugdales" key.
	var dugdalesNode *yaml.Node
	for i := 0; i+1 < len(top.Content); i += 2 {
		if top.Content[i].Value == "dugdales" {
			dugdalesNode = top.Content[i+1]
			break
		}
	}
	if dugdalesNode == nil || dugdalesNode.Kind != yaml.SequenceNode {
		return nil
	}
	for i, dnode := range dugdalesNode.Content {
		if i >= len(c.Dugdales) || dnode.Kind != yaml.MappingNode {
			continue
		}
		var lanesNode *yaml.Node
		for j := 0; j+1 < len(dnode.Content); j += 2 {
			if dnode.Content[j].Value == "lanes" {
				lanesNode = dnode.Content[j+1]
				break
			}
		}
		if lanesNode == nil || lanesNode.Kind != yaml.MappingNode {
			continue
		}
		for k := 0; k+1 < len(lanesNode.Content); k += 2 {
			lk, lv := lanesNode.Content[k], lanesNode.Content[k+1]
			isNull := lv.Tag == "!!null" ||
				(lv.Kind == yaml.ScalarNode && lv.Value == "" && lv.Tag == "")
			if !isNull {
				continue
			}
			c.Dugdales[i].nullifiedLanes = append(c.Dugdales[i].nullifiedLanes, lk.Value)
			// Also drop the placeholder zero-LaneCfg the struct decode
			// produced so downstream code sees "lane absent" rather
			// than "concurrency 0".
			delete(c.Dugdales[i].Lanes, lk.Value)
		}
	}
	return nil
}
