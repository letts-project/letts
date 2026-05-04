package lettsconfig

import (
	"fmt"
	"os"
	"path/filepath"
)

// DiscoverOpts injects all filesystem dependencies so tests can build
// a hermetic discovery environment.
//
// In production, callers fill from os.Getenv, os.Getwd, os.UserHomeDir,
// XDG_CONFIG_HOME, "/etc/letts/letts.yaml".
type DiscoverOpts struct {
	Flag             string    // explicit config-flag value (empty if not given)
	FlagName         string    // the flag's name for error messages (default "--config"); arby passes "--letts-config"
	Getenv           EnvLookup // looks up LETTS_CONFIG, XDG_CONFIG_HOME
	Cwd              string    // working directory (for ./letts.yaml)
	HomeDir          string    // user home (for ~/.letts/letts.yaml)
	XDGConfigHome    string    // overrides XDG_CONFIG_HOME from Getenv if non-empty
	EtcLettsYamlPath string    // overrides "/etc/letts/letts.yaml" for tests; "" uses default
}

// Discover returns the first existing letts.yaml, by priority:
//  1. --config flag
//  2. $LETTS_CONFIG
//  3. ./letts.yaml
//  4. $XDG_CONFIG_HOME/letts/letts.yaml
//  5. ~/.letts/letts.yaml
//  6. /etc/letts/letts.yaml
func Discover(opts DiscoverOpts) (string, error) {
	flagName := opts.FlagName
	if flagName == "" {
		flagName = "--config"
	}
	if opts.Flag != "" {
		if !fileExists(opts.Flag) {
			return "", fmt.Errorf("config file %q does not exist (from %s)", opts.Flag, flagName)
		}
		return opts.Flag, nil
	}
	if v, ok := opts.Getenv("LETTS_CONFIG"); ok && v != "" {
		if !fileExists(v) {
			return "", fmt.Errorf("config file %q does not exist (from $LETTS_CONFIG)", v)
		}
		return v, nil
	}
	if opts.Cwd != "" {
		p := filepath.Join(opts.Cwd, "letts.yaml")
		if fileExists(p) {
			return p, nil
		}
	}
	xdg := opts.XDGConfigHome
	if xdg == "" {
		xdg, _ = opts.Getenv("XDG_CONFIG_HOME")
	}
	if xdg != "" {
		p := filepath.Join(xdg, "letts", "letts.yaml")
		if fileExists(p) {
			return p, nil
		}
	}
	if opts.HomeDir != "" {
		p := filepath.Join(opts.HomeDir, ".letts", "letts.yaml")
		if fileExists(p) {
			return p, nil
		}
	}
	etc := opts.EtcLettsYamlPath
	if etc == "" {
		etc = "/etc/letts/letts.yaml"
	}
	if fileExists(etc) {
		return etc, nil
	}
	return "", fmt.Errorf("letts.yaml not found in any of: %s, $LETTS_CONFIG, ./letts.yaml, $XDG_CONFIG_HOME/letts/letts.yaml, ~/.letts/letts.yaml, /etc/letts/letts.yaml", flagName)
}

func fileExists(p string) bool {
	st, err := os.Stat(p)
	return err == nil && !st.IsDir()
}
