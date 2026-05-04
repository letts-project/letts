package config

import (
	"fmt"
	"regexp"
	"strings"
)

var envRefRegex = regexp.MustCompile(`\$\{([A-Za-z_][A-Za-z0-9_]*)\}`)

// LookupFunc returns (value, ok) for an env-var name. Pass os.LookupEnv in
// production; tests can substitute a closure.
type LookupFunc func(string) (string, bool)

// ExpandEnv replaces ${VAR} occurrences using lookup. A missing var returns
// an error — config consumer decides how to handle that (lazy vs strict).
func ExpandEnv(s string, lookup LookupFunc) (string, error) {
	var missing []string
	out := envRefRegex.ReplaceAllStringFunc(s, func(match string) string {
		name := match[2 : len(match)-1]
		v, ok := lookup(name)
		if !ok {
			missing = append(missing, name)
			return match
		}
		return v
	})
	if len(missing) > 0 {
		return "", fmt.Errorf("undefined env vars: %s", strings.Join(missing, ", "))
	}
	return out, nil
}
