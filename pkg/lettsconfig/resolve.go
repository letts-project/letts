package lettsconfig

import (
	"fmt"
	"os"
)

// ResolveOpts tunes LoadAndResolveWithOpts behavior.
type ResolveOpts struct {
	// Insecure, when true, skips the plain-token permissions check.
	// CLI flag: --insecure-config-permissions.
	Insecure bool
}

// LoadAndResolve is the strict entry point: parses, validates, extends,
// re-validates, AND enforces 0600/0400 perms when the file carries plain
// tokens. Equivalent to LoadAndResolveWithOpts(path, ResolveOpts{}).
func LoadAndResolve(path string) (*Config, error) {
	return LoadAndResolveWithOpts(path, ResolveOpts{})
}

// LoadAndResolveWithOpts is the same pipeline as LoadAndResolve but lets
// the caller opt out of the permissions check via opts.Insecure. Used by
// CLI subcommands that surface a --insecure-config-permissions flag.
func LoadAndResolveWithOpts(path string, opts ResolveOpts) (*Config, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", path, err)
	}
	c, err := Load(b)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	// Pre-extends: syntax only — structural rules (host/url presence,
	// route/alias resolution) may depend on template-inherited fields that
	// ResolveExtends fills in below.
	if err := ValidateSyntax(c); err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	if err := ResolveExtends(c); err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	// Full validation AFTER extends — extends may have introduced bad data via
	// template (e.g. invalid lane name in a template inherited by dugdales),
	// and host/url/route resolution now sees the resolved fields.
	if err := Validate(c); err != nil {
		return nil, fmt.Errorf("%s (post-extends): %w", path, err)
	}
	if !opts.Insecure {
		if err := CheckPermissions(path, c); err != nil {
			return nil, err
		}
	}
	return c, nil
}
