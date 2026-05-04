//go:build linux

package mission

import (
	"errors"
	"io"
	"os"

	"golang.org/x/sys/unix"
)

// copyFile materialises a staging artifact into the per-mission work-dir.
//
// Three-tier strategy:
//  1. FICLONE (ioctl(FICLONE) via unix.IoctlFileClone): instantaneous
//     CoW on btrfs / XFS-reflink / overlayfs that supports it. Returns
//     EOPNOTSUPP/EXDEV/EINVAL on non-CoW or cross-fs targets — fall through.
//  2. copy_file_range: kernel-side copy without userspace bounce; on
//     Linux 5.3+ this may itself reflink under the hood for CoW
//     filesystems, but is not guaranteed CoW like FICLONE.
//  3. buffered io.Copy: portable fallback for ENOSYS/EXDEV/EINVAL.
//
// O_EXCL on the destination is preserved across all three branches.
func copyFile(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()

	out, err := os.OpenFile(dst, os.O_CREATE|os.O_WRONLY|os.O_EXCL, 0o644)
	if err != nil {
		return err
	}
	defer out.Close()

	// Tier 1: FICLONE — guaranteed CoW or fail loudly.
	if err := unix.IoctlFileClone(int(out.Fd()), int(in.Fd())); err == nil {
		return out.Sync()
	} else if !isFICloneSoftFail(err) {
		return err
	}

	// Tier 2: copy_file_range.
	st, err := in.Stat()
	if err != nil {
		return err
	}
	remaining := st.Size()
	for remaining > 0 {
		n, err := unix.CopyFileRange(int(in.Fd()), nil, int(out.Fd()), nil, int(remaining), 0)
		if err != nil {
			if errors.Is(err, unix.ENOSYS) || errors.Is(err, unix.EXDEV) || errors.Is(err, unix.EINVAL) {
				// Tier 3: buffered io.Copy.
				if _, copyErr := io.Copy(out, in); copyErr != nil {
					return copyErr
				}
				return out.Sync()
			}
			return err
		}
		if n == 0 {
			break
		}
		remaining -= int64(n)
	}
	return out.Sync()
}

// isFICloneSoftFail reports whether err from IoctlFileClone is "fall
// through to next tier" rather than a hard failure. FICLONE fails
// "softly" when the filesystem doesn't support clones, when src and dst
// are on different mounts, or when permission semantics differ.
func isFICloneSoftFail(err error) bool {
	switch {
	case errors.Is(err, unix.EOPNOTSUPP):
		return true
	case errors.Is(err, unix.ENOTSUP):
		return true
	case errors.Is(err, unix.EXDEV):
		return true
	case errors.Is(err, unix.EINVAL):
		return true
	case errors.Is(err, unix.EACCES):
		return true
	case errors.Is(err, unix.ENOSYS):
		return true
	}
	return false
}
