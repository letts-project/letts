//go:build !linux && !darwin

package mission

import (
	"io"
	"os"
)

// copyFile is the FreeBSD / generic UNIX fallback: plain buffered
// io.Copy. No CoW primitive that we attempt;
// jail-based isolation is out of scope for the baseline letts.

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

	if _, err := io.Copy(out, in); err != nil {
		return err
	}
	return out.Sync()
}
