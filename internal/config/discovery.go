package config

import (
	"errors"
	"os"
)

// ErrNoDugdaleConfig is returned by DiscoverDugdale when nothing matches.
var ErrNoDugdaleConfig = errors.New("no dugdale config found in any cascade location")

// DiscoverDugdale picks the first existing config from explicit flag → env →
// candidate cascade. The candidates parameter exists to make tests
// hermetic; production passes DefaultDugdaleCandidates().
func DiscoverDugdale(flagPath, envPath string, candidates []string) (string, error) {
	if flagPath != "" {
		return flagPath, nil
	}
	if envPath != "" {
		return envPath, nil
	}
	for _, c := range candidates {
		if _, err := os.Stat(c); err == nil {
			return c, nil
		}
	}
	return "", ErrNoDugdaleConfig
}

// DefaultDugdaleCandidates returns the production config cascade:
// ./dugdale.yaml (dev/cwd), then /etc/letts/dugdale/default.yaml (canonical
// daemon config location). Per-instance configs live at
// /etc/letts/dugdale/<name>.yaml and are passed explicitly via --config by the
// dugdale@<name>.service systemd template.
func DefaultDugdaleCandidates() []string {
	return []string{"./dugdale.yaml", "/etc/letts/dugdale/default.yaml"}
}
