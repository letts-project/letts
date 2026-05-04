// Package lock provides exclusive flock for data_dir to prevent
// two dugdale processes from operating on the same data_dir.
package lock

import (
	"errors"
	"fmt"
	"os"
	"runtime"
	"sync"

	"golang.org/x/sys/unix"
)

// ErrLocked indicates another process holds the data_dir lock.
var ErrLocked = errors.New("data_dir is already locked by another dugdale process")

// Info is metadata written into the lock file for diagnostics.
type Info struct {
	Pid     int
	Host    string
	Version string
	Listen  string
}

// Lock represents an acquired flock; call Release on shutdown.
// Verify and Release coordinate through mu so a watchdog goroutine
// observing the file pointer doesn't race a concurrent shutdown.
type Lock struct {
	mu    sync.Mutex
	f     *os.File
	path  string
	inode uint64
}

// Acquire opens (creates if needed) path and takes a non-blocking exclusive
// flock. The file is truncated and metadata written for ops diagnostics.
// The Lock remembers the lockfile inode so Verify() can later detect a
// rm-and-replace by a second dugdale start.
func Acquire(path string, info Info) (*Lock, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open lock %s: %w", path, err)
	}
	if err := unix.Flock(int(f.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		_ = f.Close()
		if errors.Is(err, unix.EWOULDBLOCK) {
			return nil, ErrLocked
		}
		return nil, fmt.Errorf("flock %s: %w", path, err)
	}
	if err := f.Truncate(0); err == nil {
		_, _ = fmt.Fprintf(f, "pid=%d\nhost=%s\nversion=%s\nlisten=%s\nos=%s/%s\n",
			info.Pid, info.Host, info.Version, info.Listen, runtime.GOOS, runtime.GOARCH)
	}
	var st unix.Stat_t
	if err := unix.Fstat(int(f.Fd()), &st); err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("fstat lock fd: %w", err)
	}
	return &Lock{f: f, path: path, inode: st.Ino}, nil
}

// Release closes the lock file (kernel auto-releases the flock).
func (l *Lock) Release() error {
	if l == nil {
		return nil
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.f == nil {
		return nil
	}
	err := l.f.Close()
	l.f = nil
	return err
}

// ErrLockFileVanished is returned by Verify when the lockfile path no
// longer points at the inode we acquired — admin rm'd the file, or a
// second dugdale start re-created it at a fresh inode. Callers should
// treat this as a critical-shutdown signal: another daemon may be live
// against the same data_dir.
var ErrLockFileVanished = errors.New("lock file vanished or replaced by a different inode")

// Verify checks that path still resolves to the same inode this Lock
// was acquired on. Returns ErrLockFileVanished on rm or replace.
//
// flock is per-inode, so `rm <data_dir>/dugdale.lock` after acquire
// keeps the original kernel lock alive, but a second dugdale start
// re-creates the file at a new inode and takes its own flock — two
// daemons against the same data_dir. A watchdog calling Verify on a
// short cadence catches the divergence.
func (l *Lock) Verify() error {
	if l == nil {
		return errors.New("Verify on nil lock")
	}
	l.mu.Lock()
	path, inode, closed := l.path, l.inode, l.f == nil
	l.mu.Unlock()
	if closed {
		return errors.New("Verify on released lock")
	}
	var st unix.Stat_t
	if err := unix.Stat(path, &st); err != nil {
		if errors.Is(err, unix.ENOENT) {
			return fmt.Errorf("%w: %s removed", ErrLockFileVanished, path)
		}
		return fmt.Errorf("stat lock path: %w", err)
	}
	if st.Ino != inode {
		return fmt.Errorf("%w: %s now inode %d, was %d", ErrLockFileVanished, path, st.Ino, inode)
	}
	return nil
}
