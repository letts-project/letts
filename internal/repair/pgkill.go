package repair

// BestEffortKillPgid sends SIGKILL to the process group whose leader is pid
// iff /proc/<pid>/stat's starttime matches expectedStarttime. The identity
// check guards against pid reuse: if the original mission process exited and
// the kernel handed the pid to an unrelated process, the starttime won't
// match and we won't kill anything.
//
// Returns true if the kill syscall was actually attempted (identity matched).
// Returns false on identity mismatch, missing /proc entry (pid gone), or
// non-Linux platforms (where /proc lookup is unavailable).
//
// This is best-effort: if the original group leader (pid) died but children
// in the same pgid are still alive, /proc/<pid> is gone, identity check
// fails, and the children leak. Strong containment requires per-mission
// cgroups (future).
func BestEffortKillPgid(pid, pgid int, expectedStarttime int64) bool {
	if pid <= 0 || pgid <= 0 || expectedStarttime == 0 {
		return false
	}
	return platformKillPgid(pid, pgid, expectedStarttime)
}
