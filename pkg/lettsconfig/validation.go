package lettsconfig

import (
	"fmt"
	"regexp"
	"sort"
	"strings"
)

var (
	// Regex table.
	reDugdaleID = regexp.MustCompile(`^[a-z][a-z0-9_-]{0,63}$`)
	reLaneName  = regexp.MustCompile(`^[a-z][a-z0-9_-]{0,31}$`)
	reLabel     = regexp.MustCompile(`^[a-z][a-z0-9_-]{0,31}$`)
	reRoute     = regexp.MustCompile(`^[a-z][a-z0-9_-]{0,63}$`)
	reTemplate  = regexp.MustCompile(`^[a-z][a-z0-9_-]{0,63}$`)
)

func ValidateDugdaleID(s string) error {
	if !reDugdaleID.MatchString(s) {
		return fmt.Errorf("invalid dugdale id %q (must match %s)", s, reDugdaleID)
	}
	return nil
}

func ValidateLaneName(s string) error {
	if !reLaneName.MatchString(s) {
		return fmt.Errorf("invalid lane name %q (must match %s)", s, reLaneName)
	}
	return nil
}

func ValidateLabel(s string) error {
	if !reLabel.MatchString(s) {
		return fmt.Errorf("invalid label %q (must match %s)", s, reLabel)
	}
	return nil
}

func ValidateRouteName(s string) error {
	if !reRoute.MatchString(s) {
		return fmt.Errorf("invalid route name %q (must match %s)", s, reRoute)
	}
	return nil
}

func ValidateTemplateName(s string) error {
	if !reTemplate.MatchString(s) {
		return fmt.Errorf("invalid template name %q (must match %s)", s, reTemplate)
	}
	return nil
}

// Validate runs the full validation: ValidateSyntax then ValidateStructure.
// Use this on a single, complete config (single-file load, or a merged config
// after extends resolution).
//
// Does NOT call extends merge, env substitution, or token resolution —
// those happen in separate stages and have their own validation passes.
func Validate(c *Config) error {
	if err := ValidateSyntax(c); err != nil {
		return err
	}
	return ValidateStructure(c)
}

// ValidateSyntax checks per-field name regexes and intra-config uniqueness —
// rules that are well-defined on a single config fragment. This is
// the only pass safe to run on each `-f` file BEFORE merge, because structural
// rules (host/url presence after extends, route-target resolution, alias-key
// collision) span the whole merged config and would wrongly reject a host-less
// delta overlay.
func ValidateSyntax(c *Config) error {
	// A port outside 1..65535 (other than 0 = "unset, use default")
	// would otherwise flow into "http://host:<port>" and fail at dial time
	// with an opaque wire error instead of a clear config error.
	if c.Defaults.Port < 0 || c.Defaults.Port > 65535 {
		return fmt.Errorf("defaults.port %d out of range (1..65535; 0 = use default)", c.Defaults.Port)
	}
	seenIDs := map[string]bool{}
	for i, d := range c.Dugdales {
		if err := ValidateDugdaleID(d.ID); err != nil {
			return fmt.Errorf("dugdales[%d]: %w", i, err)
		}
		if seenIDs[d.ID] {
			return fmt.Errorf("dugdales[%d]: duplicate id %q", i, d.ID)
		}
		seenIDs[d.ID] = true
		if d.Port < 0 || d.Port > 65535 {
			return fmt.Errorf("dugdales[%d] (%q): port %d out of range (1..65535; 0 = use default)", i, d.ID, d.Port)
		}
		for laneName := range d.Lanes {
			if err := ValidateLaneName(laneName); err != nil {
				return fmt.Errorf("dugdales[%d].lanes: %w", i, err)
			}
		}
		for _, l := range d.Labels {
			if err := ValidateLabel(l); err != nil {
				return fmt.Errorf("dugdales[%d].labels: %w", i, err)
			}
		}
	}
	for name, t := range c.Templates {
		if err := ValidateTemplateName(name); err != nil {
			return fmt.Errorf("templates[%q]: %w", name, err)
		}
		for laneName := range t.Lanes {
			if err := ValidateLaneName(laneName); err != nil {
				return fmt.Errorf("templates[%q].lanes: %w", name, err)
			}
		}
		for _, l := range t.Labels {
			if err := ValidateLabel(l); err != nil {
				return fmt.Errorf("templates[%q].labels: %w", name, err)
			}
		}
	}
	for name, r := range c.Routes {
		if err := ValidateRouteName(name); err != nil {
			return fmt.Errorf("routes[%q]: %w", name, err)
		}
		// The lane field of each route was unchecked — empty
		// or malformed lane names sailed through load only to fail at
		// dispatch time with a generic 400. Catch at config load.
		if r.Lane == "" {
			return fmt.Errorf("routes[%q]: lane is required", name)
		}
		if err := ValidateLaneName(r.Lane); err != nil {
			return fmt.Errorf("routes[%q].lane: %w", name, err)
		}
	}
	for aliasKey, aliasVal := range c.Aliases {
		if err := ValidateDugdaleID(aliasKey); err != nil {
			return fmt.Errorf("aliases[%q]: %w", aliasKey, err)
		}
		if aliasVal == "" {
			return fmt.Errorf("aliases[%q]: empty value", aliasKey)
		}
		// Values may contain ${VAR} that resolve at runtime — skip regex
		// check on those. Pure literals must satisfy the dugdale-id regex.
		if !strings.Contains(aliasVal, "${") {
			if err := ValidateDugdaleID(aliasVal); err != nil {
				return fmt.Errorf("aliases[%q] value: %w", aliasKey, err)
			}
		}
	}
	return nil
}

// ValidateStructure checks rules that require the fully-merged (and ideally
// extends-resolved) config: host/url presence, alias-key↔dugdale-id collision,
// alias cycles, and route-target resolution. Run once on the merged config,
// after ResolveExtends.
func ValidateStructure(c *Config) error {
	seenIDs := map[string]bool{}
	for _, d := range c.Dugdales {
		seenIDs[d.ID] = true
	}
	for aliasKey := range c.Aliases {
		if seenIDs[aliasKey] {
			return fmt.Errorf("aliases[%q]: collides with existing dugdales[].id (would mask the dugdale on --host=%s)", aliasKey, aliasKey)
		}
	}

	// Detect alias cycles at load time, not just on dispatch.
	// Only walk chains of pure literals — ${VAR} resolutions are runtime
	// and may legitimately differ between hosts.
	if err := checkAliasCycles(c); err != nil {
		return err
	}

	// Every dugdale entry MUST set either host or url (or both
	// via templates). Without this validation a misconfigured entry
	// produces malformed URLs like "http://:7180" on the first dispatch.
	for i, d := range c.Dugdales {
		if d.Host == "" && d.URL == "" {
			return fmt.Errorf("dugdales[%d] (%q): one of host or url is required", i, d.ID)
		}
	}

	// Route targets must resolve to a known dugdale id or
	// alias at load time. Skip targets that contain ${VAR} — those are
	// runtime-substituted and may legitimately differ between hosts.
	for name, r := range c.Routes {
		if r.Host == "" || strings.Contains(r.Host, "${") {
			continue
		}
		if findDugdale(c, r.Host) == nil {
			if _, isAlias := c.Aliases[r.Host]; !isAlias {
				return fmt.Errorf("routes[%q]: host %q is not a known dugdale id or alias", name, r.Host)
			}
		}
	}
	return nil
}

// PlainTokenLocations returns a list of human-readable "where" strings
// identifying every place in c where a plain (non-${VAR}) token is
// configured. letts apply prints a warning for each
// such location so operators get a visible nudge toward env substitution.
//
// Empty slice = no plain tokens; returned in stable order for deterministic
// CLI output.
func PlainTokenLocations(c *Config) []string {
	var out []string
	if c.Auth.Token != "" && IsPlainToken(c.Auth.Token) {
		out = append(out, "auth.token")
	}
	if c.Auth.AdminToken != "" && IsPlainToken(c.Auth.AdminToken) {
		out = append(out, "auth.admin_token")
	}
	if c.Auth.ExecToken != "" && IsPlainToken(c.Auth.ExecToken) {
		out = append(out, "auth.exec_token")
	}
	for i, d := range c.Dugdales {
		if d.Token != "" && IsPlainToken(d.Token) {
			out = append(out, fmt.Sprintf("dugdales[%d:%s].token", i, d.ID))
		}
		if d.AdminToken != "" && IsPlainToken(d.AdminToken) {
			out = append(out, fmt.Sprintf("dugdales[%d:%s].admin_token", i, d.ID))
		}
		if d.ExecToken != "" && IsPlainToken(d.ExecToken) {
			out = append(out, fmt.Sprintf("dugdales[%d:%s].exec_token", i, d.ID))
		}
	}
	// Templates can also carry plain tokens (extends merges
	// them into dugdales), and CheckPermissions fires on them too. The
	// warning needs to name the culprit so the operator knows which
	// template to fix.
	keys := make([]string, 0, len(c.Templates))
	for k := range c.Templates {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, name := range keys {
		t := c.Templates[name]
		if t.Token != "" && IsPlainToken(t.Token) {
			out = append(out, fmt.Sprintf("templates[%s].token", name))
		}
		if t.AdminToken != "" && IsPlainToken(t.AdminToken) {
			out = append(out, fmt.Sprintf("templates[%s].admin_token", name))
		}
		if t.ExecToken != "" && IsPlainToken(t.ExecToken) {
			out = append(out, fmt.Sprintf("templates[%s].exec_token", name))
		}
	}
	return out
}

// checkAliasCycles walks each alias's literal chain and reports cycle,
// self-reference, or max-depth-exceeded. Chains involving ${VAR} are
// skipped at the first env-substituted hop — those are deliberately
// runtime-resolved and may differ between hosts.
func checkAliasCycles(c *Config) error {
	for start := range c.Aliases {
		visited := map[string]bool{start: true}
		cur := start
		resolved := false
		for depth := 0; depth < aliasMaxDepth; depth++ {
			next, ok := c.Aliases[cur]
			if !ok {
				// cur isn't an alias key — but is it a real dugdale id?
				if findDugdale(c, cur) != nil {
					resolved = true
					break
				}
				// Dangling chain: alias points at an unknown id. Surface
				// as a load-time error so typos don't first appear on the
				// production dispatch path.
				if cur == start {
					return fmt.Errorf("aliases[%q]: value is not a known dugdale id or alias", start)
				}
				return fmt.Errorf("aliases[%q]: chain dead-ends at unknown id %q", start, cur)
			}
			if strings.Contains(next, "${") {
				// Runtime-resolved hop — bail out without diagnosing.
				resolved = true
				break
			}
			if next == cur {
				return fmt.Errorf("aliases[%q]: self-referential value %q", cur, next)
			}
			if visited[next] {
				return fmt.Errorf("aliases[%q]: cycle detected at %q", start, next)
			}
			visited[next] = true
			cur = next
		}
		if !resolved {
			return fmt.Errorf("aliases[%q]: chain exceeds max depth %d", start, aliasMaxDepth)
		}
	}
	return nil
}
