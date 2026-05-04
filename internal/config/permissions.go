package config

import (
	"fmt"
	"os"
	"syscall"
)

// CheckConfigPermissions enforces the contract: configs with secrets
// require 0600/0400 mode and process-UID ownership. If insecure=true, bypass
// the check entirely (CI/dev escape hatch).
func CheckConfigPermissions(path string, c *DugdaleConfig, insecure bool) error {
	if insecure {
		return nil
	}
	if !configHasSecrets(c) {
		return nil
	}
	st, err := os.Stat(path)
	if err != nil {
		return fmt.Errorf("stat config: %w", err)
	}
	mode := st.Mode().Perm()
	if mode != 0o600 && mode != 0o400 {
		return fmt.Errorf("config %s has unsafe permissions %o; require 0600/0400 owned by process user", path, mode)
	}
	sys, ok := st.Sys().(*syscall.Stat_t)
	if !ok {
		return fmt.Errorf("cannot stat config %s on this platform", path)
	}
	if int(sys.Uid) != os.Getuid() {
		return fmt.Errorf("config %s owned by uid %d but process uid is %d", path, sys.Uid, os.Getuid())
	}
	return nil
}

func configHasSecrets(c *DugdaleConfig) bool {
	if len(c.Auth.Tokens) > 0 {
		return true
	}
	if len(c.Admin.Tokens) > 0 {
		return true
	}
	if len(c.Exec.Tokens) > 0 {
		return true
	}
	if len(c.MissionEnv.Set) > 0 {
		return true
	}
	return false
}
