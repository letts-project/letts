package config

import (
	"fmt"
	"os"
	"path/filepath"
)

// ValidateConfigPaths verifies the OS-level reachability of paths derived
// from a dugdale config: data_dir and log.output. Used by
// `dugdale --check-config` to catch typos and permission problems before
// the daemon tries to start.
//
// Rules:
//   - data_dir: writable if it exists; otherwise its parent must be a
//     writable directory (so MkdirAll succeeds at startup).
//   - log.output (if a file, not "stdout"/"stderr"/"-"): writable file or
//     a writable parent directory.
func ValidateConfigPaths(c *DugdaleConfig) error {
	if err := checkDirWritable(c.DataDir, "data_dir"); err != nil {
		return err
	}
	out := c.Log.Output
	if out != "" && out != "stdout" && out != "stderr" && out != "-" {
		if err := checkFileWritable(out, "log.output"); err != nil {
			return err
		}
	}
	return nil
}

// checkDirWritable returns nil iff path is a writable directory, or if
// it doesn't exist but its parent is a writable directory.
func checkDirWritable(path, label string) error {
	if path == "" {
		return nil
	}
	if info, err := os.Stat(path); err == nil {
		if !info.IsDir() {
			return fmt.Errorf("%s %q: not a directory", label, path)
		}
		return probeWrite(path, label)
	} else if !os.IsNotExist(err) {
		return fmt.Errorf("%s %q: %w", label, path, err)
	}
	parent := filepath.Dir(path)
	if pInfo, err := os.Stat(parent); err != nil {
		return fmt.Errorf("%s parent %q: %w", label, parent, err)
	} else if !pInfo.IsDir() {
		return fmt.Errorf("%s parent %q: not a directory", label, parent)
	}
	return probeWrite(parent, label+" parent")
}

// checkFileWritable returns nil iff path is a writable regular file, or
// path doesn't exist but its parent dir is writable.
func checkFileWritable(path, label string) error {
	if info, err := os.Stat(path); err == nil {
		if info.IsDir() {
			return fmt.Errorf("%s %q: expected file got directory", label, path)
		}
		f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0)
		if err != nil {
			return fmt.Errorf("%s %q: not writable: %w", label, path, err)
		}
		_ = f.Close()
		return nil
	} else if !os.IsNotExist(err) {
		return fmt.Errorf("%s %q: %w", label, path, err)
	}
	return checkDirWritable(filepath.Dir(path), label+" parent")
}

// probeWrite attempts to create-and-remove a probe file in dir to confirm
// real write access. Faster than parsing mode bits and side-steps NFS /
// FUSE backends where mode bits don't reflect runtime permissions.
func probeWrite(dir, label string) error {
	f, err := os.CreateTemp(dir, ".letts-probe-*")
	if err != nil {
		return fmt.Errorf("%s %q: not writable: %w", label, dir, err)
	}
	name := f.Name()
	_ = f.Close()
	_ = os.Remove(name)
	return nil
}
