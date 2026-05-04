package main

import (
	"regexp"
	"strings"
)

// execKeyRegex mirrors the server-side key syntax. Identifiers start with
// a letter or underscore, then up to 63 alphanumeric/underscore chars
// (64 total).
var execKeyRegex = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]{0,63}$`)

// execKV is a parsed --in/--out key=path pair.
type execKV struct {
	Key  string
	Path string
}

// parseExecKV parses --in/--out "key=path" pairs. Validates each key
// against execKeyRegex AND rejects the reserved `__` prefix (kept for
// internal roles like __stdin__). Returns a BadUsageError on the first
// violation, including duplicate keys.
func parseExecKV(pairs []string, flag string) ([]execKV, error) {
	out := make([]execKV, 0, len(pairs))
	seen := map[string]bool{}
	for _, p := range pairs {
		eq := strings.IndexByte(p, '=')
		if eq < 1 {
			return nil, NewBadUsageError(flag + " expects key=path, got: " + p)
		}
		key, path := p[:eq], p[eq+1:]
		if path == "" {
			return nil, NewBadUsageError(flag + " path empty: " + p)
		}
		if !execKeyRegex.MatchString(key) {
			return nil, NewBadUsageError(flag + " key invalid (regex ^[A-Za-z_][A-Za-z0-9_]{0,63}$): " + key)
		}
		if strings.HasPrefix(key, "__") {
			return nil, NewBadUsageError(flag + " key uses reserved __ prefix: " + key)
		}
		if seen[key] {
			return nil, NewBadUsageError(flag + " duplicate key: " + key)
		}
		seen[key] = true
		out = append(out, execKV{Key: key, Path: path})
	}
	return out, nil
}
