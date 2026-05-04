//go:build !linux

package repair

// platformKillPgid is a no-op on non-Linux platforms because the /proc
// identity check is unavailable. Production deployments target Linux; macOS
// is dev-only.
func platformKillPgid(_, _ int, _ int64) bool { return false }
