package config

import (
	"fmt"
	"regexp"
)

var (
	dugdaleIDRegex = regexp.MustCompile(`^[a-z][a-z0-9_-]{0,63}$`)
	laneNameRegex  = regexp.MustCompile(`^[a-z][a-z0-9_-]{0,31}$`)
	labelRegex     = regexp.MustCompile(`^[a-z][a-z0-9_-]{0,31}$`)
	routeNameRegex = regexp.MustCompile(`^[a-z][a-z0-9_-]{0,63}$`)
	missionNameRe  = regexp.MustCompile(`^[A-Za-z0-9_][A-Za-z0-9_.-]*$`)
	roleKeyRegex   = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]{0,63}$`)
)

// ValidateDugdaleID validates a dugdale instance id.
func ValidateDugdaleID(s string) error {
	if !dugdaleIDRegex.MatchString(s) {
		return fmt.Errorf("invalid dugdale id %q (regex ^[a-z][a-z0-9_-]{0,63}$)", s)
	}
	return nil
}

// ValidateLaneName validates a lane name.
func ValidateLaneName(s string) error {
	if !laneNameRegex.MatchString(s) {
		return fmt.Errorf("invalid lane name %q (regex ^[a-z][a-z0-9_-]{0,31}$)", s)
	}
	return nil
}

// ValidateLabel validates a label value.
func ValidateLabel(s string) error {
	if !labelRegex.MatchString(s) {
		return fmt.Errorf("invalid label %q (regex ^[a-z][a-z0-9_-]{0,31}$)", s)
	}
	return nil
}

// ValidateRouteName validates a route name.
func ValidateRouteName(s string) error {
	if !routeNameRegex.MatchString(s) {
		return fmt.Errorf("invalid route name %q (regex ^[a-z][a-z0-9_-]{0,63}$)", s)
	}
	return nil
}

// ValidateMissionName validates a mission name (≤128 chars,
// regex ^[A-Za-z0-9_][A-Za-z0-9_.-]*$).
func ValidateMissionName(s string) error {
	if len(s) == 0 || len(s) > 128 || !missionNameRe.MatchString(s) {
		return fmt.Errorf("invalid mission name %q (regex ^[A-Za-z0-9_][A-Za-z0-9_.-]*$, ≤128)", s)
	}
	return nil
}

// ValidateRoleKey validates a role/env key.
// Regex ^[A-Za-z_][A-Za-z0-9_]{0,63}$ plus the __ prefix is reserved.
func ValidateRoleKey(s string) error {
	if !roleKeyRegex.MatchString(s) {
		return fmt.Errorf("invalid role/key %q (regex ^[A-Za-z_][A-Za-z0-9_]{0,63}$)", s)
	}
	if len(s) >= 2 && s[0] == '_' && s[1] == '_' {
		return fmt.Errorf("role/key %q uses reserved __ prefix", s)
	}
	return nil
}
