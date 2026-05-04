//go:build darwin

package mission

import (
	"errors"
	"io"
	"os"

	"golang.org/x/sys/unix"
)

// copyFile materialises a staging artifact into the per-mission work-dir
// via clonefile() with a buffered-copy fallback.
//
// On APFS clonefile() is instantaneous CoW. On HFS+ / FAT / network
// shares it fails with ENOTSUP/EXDEV/EEXIST — fall through to buffered
// io.Copy. Other syscall errors (ENOENT, EACCES on src, ENOSPC) bubble up.
//
// O_EXCL on the destination is enforced even on the fallback path so a
// pre-existing file can't be overwritten by a buggy caller.
func copyFile(src, dst string) error {
	// Tier 1: clonefile — instant CoW on APFS, EEXIST guard built in.
	if err := unix.Clonefile(src, dst, 0); err == nil {
		// Successful clone — caller relies on fsync semantics, but
		// clonefile is metadata-only on APFS; opening and syncing would be
		// redundant. Best-effort sync the parent dir is done by callers
		// of copyFile (workdir.go) as needed.
		return nil
	} else if !isClonefileSoftFail(err) {
		return err
	}

	// Tier 2: buffered io.Copy. Replicates the original copy_other.go
	// behaviour with O_EXCL guard.
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer func() { _ = in.Close() }()
	out, err := os.OpenFile(dst, os.O_CREATE|os.O_WRONLY|os.O_EXCL, 0o644)
	if err != nil {
		return err
	}
	defer func() { _ = out.Close() }()
	if _, err := io.Copy(out, in); err != nil {
		return err
	}
	return out.Sync()
}

// isClonefileSoftFail reports whether the clonefile error indicates a
// fall-through to buffered copy rather than a hard problem. EEXIST is
// NOT a soft fail — the O_EXCL semantics of copyFile must not be
// silently bypassed by retrying with io.Copy onto an existing file
// (io.Copy would also EEXIST via O_EXCL, but no point trying).
func isClonefileSoftFail(err error) bool {
	switch {
	case errors.Is(err, unix.ENOTSUP):
		return true
	case errors.Is(err, unix.EOPNOTSUPP):
		return true
	case errors.Is(err, unix.EXDEV):
		return true
	case errors.Is(err, unix.EINVAL):
		return true
	}
	return false
}
