package lettsconfig

import (
	"fmt"
	"regexp"
	"strings"
)

// reEnvVar matches ${NAME} where NAME is [A-Za-z_][A-Za-z0-9_]*.
var reEnvVar = regexp.MustCompile(`\$\{([A-Za-z_][A-Za-z0-9_]*)\}`)

// MissingEnvError is returned by SubstituteEnv when an unresolved
// ${VAR} cannot be looked up.
type MissingEnvError struct {
	Name string
}

func (e *MissingEnvError) Error() string {
	return fmt.Sprintf("environment variable %q is not set", e.Name)
}

// EnvLookup mirrors os.LookupEnv (returns value, ok). Pass os.LookupEnv
// in production. Tests inject a closure for hermetic lookup.
type EnvLookup func(string) (string, bool)

// SubstituteEnv replaces every ${NAME} in s with the value from getenv.
// Returns *MissingEnvError if any ${NAME} cannot be resolved.
//
// Substitution is lazy and scope-aware: the loader stores
// raw "${LETTS_DISPATCH_TOKEN}" tokens and only resolves them when a
// command actually needs that scope's token.
func SubstituteEnv(s string, getenv EnvLookup) (string, error) {
	if !strings.Contains(s, "${") {
		return s, nil
	}
	if getenv == nil {
		matches := reEnvVar.FindStringSubmatch(s)
		if len(matches) > 1 {
			return "", &MissingEnvError{Name: matches[1]}
		}
		return s, nil
	}
	var missing string
	out := reEnvVar.ReplaceAllStringFunc(s, func(m string) string {
		name := reEnvVar.FindStringSubmatch(m)[1]
		v, ok := getenv(name)
		if !ok {
			if missing == "" {
				missing = name
			}
			return m
		}
		return v
	})
	if missing != "" {
		return "", &MissingEnvError{Name: missing}
	}
	return out, nil
}

// IsPlainToken returns true if s contains any literal (non-${VAR}) text,
// meaning it is partly or wholly committed in YAML and should trigger
// the permissions-check requirement.
//
// Pure ${VAR} strings (e.g. "${LETTS_DISPATCH_TOKEN}") are NOT plain —
// the secret material lives in the environment. Mixed strings like
// "prefix-${VAR}" ARE plain: literal bytes sit in the file.
//
// Empty string returns false (it is not a "token at all"). Partial
// literals like "partial-${FOO}-plain" count as plain, so we strip ${VAR}
// occurrences and check whether anything literal remains (a naive
// !reEnvVar.MatchString(s) would incorrectly accept them).
func IsPlainToken(s string) bool {
	if s == "" {
		return false
	}
	stripped := reEnvVar.ReplaceAllString(s, "")
	return stripped != ""
}
