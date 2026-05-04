package lettsconfig

import "fmt"

// Scope identifies which token to resolve for a dugdale.
type Scope int

const (
	ScopeDispatch Scope = iota
	ScopeAdmin
	ScopeExec
)

func (s Scope) String() string {
	switch s {
	case ScopeDispatch:
		return "dispatch"
	case ScopeAdmin:
		return "admin"
	case ScopeExec:
		return "exec"
	default:
		return "unknown"
	}
}

// ResolveToken finds the right token for (dugdaleID, scope), preferring
// dugdale-local over global Auth fallbacks, then performs env substitution.
//
// Resolution order is dugdale entry → global auth.
//
// Returns *MissingEnvError if the chosen value references an unset ${VAR}.
// Returns plain error if no token is configured for the scope at all.
func ResolveToken(c *Config, dugdaleID string, scope Scope, getenv EnvLookup) (string, error) {
	d := findDugdale(c, dugdaleID)
	if d == nil {
		return "", fmt.Errorf("dugdale %q not found in letts.yaml", dugdaleID)
	}
	var raw string
	switch scope {
	case ScopeDispatch:
		if d.Token != "" {
			raw = d.Token
		} else {
			raw = c.Auth.Token
		}
	case ScopeAdmin:
		if d.AdminToken != "" {
			raw = d.AdminToken
		} else {
			raw = c.Auth.AdminToken
		}
	case ScopeExec:
		if d.ExecToken != "" {
			raw = d.ExecToken
		} else {
			raw = c.Auth.ExecToken
		}
	default:
		return "", fmt.Errorf("unknown scope %d", scope)
	}
	if raw == "" {
		return "", fmt.Errorf("no %s token configured for dugdale %q", scope, dugdaleID)
	}
	out, err := SubstituteEnv(raw, getenv)
	if err != nil {
		return "", err
	}
	// A ${VAR} that resolves to an empty string (env set but empty) would
	// otherwise yield a silently-empty token: the client omits the
	// Authorization header and the request fails with a confusing 401. Treat
	// it as a hard config error, like a missing token.
	if out == "" {
		return "", fmt.Errorf("%s token for dugdale %q resolved to empty (check its ${ENV} value)", scope, dugdaleID)
	}
	return out, nil
}

func findDugdale(c *Config, id string) *Dugdale {
	for i := range c.Dugdales {
		if c.Dugdales[i].ID == id {
			return &c.Dugdales[i]
		}
	}
	return nil
}
