package main

import "letts/pkg/lettsconfig"

// MergeConfigs returns a new Config with right-overlay semantics:
//   - dugdales: union by id, right overrides duplicates.
//   - aliases/routes/templates: map union with right wins.
//   - selector/auth/defaults: scalar override if non-zero in right.
func MergeConfigs(base, overlay *lettsconfig.Config) *lettsconfig.Config {
	if base == nil {
		return overlay
	}
	if overlay == nil {
		return base
	}
	out := *base
	if overlay.Auth.Token != "" {
		out.Auth.Token = overlay.Auth.Token
	}
	if overlay.Auth.AdminToken != "" {
		out.Auth.AdminToken = overlay.Auth.AdminToken
	}
	if overlay.Auth.ExecToken != "" {
		out.Auth.ExecToken = overlay.Auth.ExecToken
	}
	if overlay.Defaults.Port != 0 {
		out.Defaults.Port = overlay.Defaults.Port
	}
	if len(overlay.Selector.Match) > 0 {
		out.Selector.Match = overlay.Selector.Match
	}
	out.Aliases = mergeStringMap(base.Aliases, overlay.Aliases)
	out.Routes = mergeRouteMap(base.Routes, overlay.Routes)
	out.Templates = mergeTemplateMap(base.Templates, overlay.Templates)
	out.Dugdales = mergeDugdaleSlice(base.Dugdales, overlay.Dugdales)
	return &out
}

func mergeStringMap(a, b map[string]string) map[string]string {
	out := map[string]string{}
	for k, v := range a {
		out[k] = v
	}
	for k, v := range b {
		out[k] = v
	}
	return out
}

func mergeRouteMap(a, b map[string]lettsconfig.Route) map[string]lettsconfig.Route {
	out := map[string]lettsconfig.Route{}
	for k, v := range a {
		out[k] = v
	}
	for k, v := range b {
		out[k] = v
	}
	return out
}

func mergeTemplateMap(a, b map[string]lettsconfig.Template) map[string]lettsconfig.Template {
	out := map[string]lettsconfig.Template{}
	for k, v := range a {
		out[k] = v
	}
	for k, v := range b {
		out[k] = v
	}
	return out
}

func mergeDugdaleSlice(a, b []lettsconfig.Dugdale) []lettsconfig.Dugdale {
	idx := map[string]int{}
	out := append([]lettsconfig.Dugdale(nil), a...)
	for i, d := range out {
		idx[d.ID] = i
	}
	for _, overlay := range b {
		if i, ok := idx[overlay.ID]; ok {
			// Field-level merge — overlay overrides base fields that
			// are explicitly set (non-zero). A wholesale `out[i] =
			// overlay` would wipe base's lanes/labels/runtime even
			// when overlay only carried e.g. `{id, port}`.
			out[i] = mergeDugdaleEntry(out[i], overlay)
		} else {
			idx[overlay.ID] = len(out)
			out = append(out, overlay)
		}
	}
	return out
}

// mergeDugdaleEntry overlays the right (overlay) onto the left (base):
//   - non-empty scalars in overlay win (Host, Port, URL, Extends,
//     MissionDir, Token, AdminToken, ExecToken).
//   - Runtime sub-struct is deep-merged: non-empty fields in overlay
//     win; ValidateMissionFile preserved unless overlay explicitly
//     differs (zero-value ambiguity for bool — best effort).
//   - Labels: overlay replaces if non-empty, else base preserved.
//   - Lanes: per-name union; overlay value wins on collision.
func mergeDugdaleEntry(base, overlay lettsconfig.Dugdale) lettsconfig.Dugdale {
	out := base
	if overlay.Host != "" {
		out.Host = overlay.Host
	}
	if overlay.Port != 0 {
		out.Port = overlay.Port
	}
	if overlay.URL != "" {
		out.URL = overlay.URL
	}
	// Proxy: an explicit `proxy: null` in the overlay deletes it (connect
	// directly), carried forward as a nullify sentinel so a post-merge
	// ResolveExtends also drops a template-inherited proxy; otherwise a
	// non-empty overlay proxy wins.
	if overlay.ProxyNullified() {
		out.Proxy = ""
		out.SetProxyNullified(true)
	} else if overlay.Proxy != "" {
		out.Proxy = overlay.Proxy
		out.SetProxyNullified(false)
	}
	if overlay.Extends != "" {
		out.Extends = overlay.Extends
	}
	if overlay.MissionDir != "" {
		out.MissionDir = overlay.MissionDir
	}
	if overlay.Token != "" {
		out.Token = overlay.Token
	}
	if overlay.AdminToken != "" {
		out.AdminToken = overlay.AdminToken
	}
	if overlay.ExecToken != "" {
		out.ExecToken = overlay.ExecToken
	}
	if overlay.Runtime.MissionPathTemplate != "" {
		out.Runtime.MissionPathTemplate = overlay.Runtime.MissionPathTemplate
	}
	if len(overlay.Runtime.CommandTemplate) > 0 {
		out.Runtime.CommandTemplate = overlay.Runtime.CommandTemplate
	}
	// Overlay can set ValidateMissionFile to false over a base
	// true only when its YAML carried an explicit value. That flag was
	// added for extends; thread it through the layered merge.
	if overlay.HasExplicitValidateMissionFile() {
		out.Runtime.ValidateMissionFile = overlay.Runtime.ValidateMissionFile
		out.SetExplicitValidateMissionFile(true)
	}
	if len(overlay.Labels) > 0 {
		out.Labels = overlay.Labels
	}
	if len(overlay.Lanes) > 0 || len(overlay.NullifiedLanes()) > 0 {
		if out.Lanes == nil {
			out.Lanes = map[string]lettsconfig.LaneCfg{}
		} else {
			cp := make(map[string]lettsconfig.LaneCfg, len(out.Lanes)+len(overlay.Lanes))
			for k, v := range out.Lanes {
				cp[k] = v
			}
			out.Lanes = cp
		}
		for k, v := range overlay.Lanes {
			out.Lanes[k] = v
		}
		// Overlay's `lanes: <name>: null` drops a lane already
		// materialised in out.Lanes from an earlier file.
		for _, name := range overlay.NullifiedLanes() {
			delete(out.Lanes, name)
		}
	}
	// Carry the nullification sentinel forward (union of base and
	// overlay) so the post-merge ResolveExtends also suppresses a lane that is
	// only inherited from a template via extends — it isn't in out.Lanes yet,
	// so the delete() above can't reach it.
	if len(out.NullifiedLanes()) > 0 || len(overlay.NullifiedLanes()) > 0 {
		nulls := append(append([]string(nil), out.NullifiedLanes()...), overlay.NullifiedLanes()...)
		out.SetNullifiedLanes(nulls)
	}
	return out
}
