package lettsconfig

import "fmt"

const aliasMaxDepth = 8

// ResolveHost translates an alias chain or direct dugdale id into a real
// dugdale id (one present in c.Dugdales). Returns error on:
//   - unknown id (not in aliases and not in dugdales)
//   - alias cycle
//   - alias chain longer than aliasMaxDepth
//   - alias self-reference (`a: a`)
//   - unresolved ${VAR} in alias value
func ResolveHost(c *Config, host string, getenv EnvLookup) (string, error) {
	visited := map[string]bool{}
	cur := host
	// Caps alias chain depth at 8. The
	// previous `depth <= aliasMaxDepth` permitted 9 iterations
	// (0..8 inclusive). Switch to strict `<` so the loop body runs
	// exactly aliasMaxDepth times and the post-loop overflow error
	// fires on the 9th hop.
	for depth := 0; depth < aliasMaxDepth; depth++ {
		if findDugdale(c, cur) != nil {
			return cur, nil
		}
		next, ok := c.Aliases[cur]
		if !ok {
			return "", fmt.Errorf("host %q not found in aliases or dugdales[].id", cur)
		}
		resolved, err := SubstituteEnv(next, getenv)
		if err != nil {
			return "", fmt.Errorf("alias %q value: %w", cur, err)
		}
		if resolved == cur {
			return "", fmt.Errorf("alias %q is self-referential", cur)
		}
		if visited[cur] {
			return "", fmt.Errorf("alias cycle detected starting at %q", host)
		}
		visited[cur] = true
		cur = resolved
	}
	return "", fmt.Errorf("alias chain from %q exceeds max depth %d", host, aliasMaxDepth)
}

// ResolveRoute looks up a symbolic route and returns the resolved
// (dugdaleID, lane). Routes can target aliases, which ResolveHost expands.
func ResolveRoute(c *Config, route string, getenv EnvLookup) (host string, lane string, err error) {
	r, ok := c.Routes[route]
	if !ok {
		return "", "", fmt.Errorf("route %q not found in letts.yaml", route)
	}
	host, err = ResolveHost(c, r.Host, getenv)
	if err != nil {
		return "", "", fmt.Errorf("route %q: %w", route, err)
	}
	return host, r.Lane, nil
}
