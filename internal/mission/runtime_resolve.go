// Package mission contains all the spawn/fd3/finalize logic.
package mission

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"letts/internal/fsutil"
	"letts/internal/storage"
)

// ErrMissionNotFound is returned when validate_mission_file=true and the
// resolved file does not exist.
var ErrMissionNotFound = errors.New("mission_not_found")

// ErrMissionNotInDir is returned when path containment fails (symlink escape).
var ErrMissionNotInDir = errors.New("mission_not_in_dir")

// ResolveCommand returns the argv to exec, with all template substitutions
// applied and path containment verified.
//
// Template rules:
//   - MissionPathTemplate defaults to "{mission}" if empty.
//   - {mission} in MissionPathTemplate is replaced with missionName.
//   - The resulting relative path is joined to rt.MissionDir.
//   - If ValidateMissionFile: Stat the resolved path and verify containment.
//   - CommandTemplate is a JSON array of strings; defaults to ["{mission_path}"].
//   - {mission} and {mission_path} are replaced in each argv element.
func ResolveCommand(rt *storage.MissionRuntime, missionName string) ([]string, error) {
	// Resolve mission path.
	pathTemplate := rt.MissionPathTemplate
	if pathTemplate == "" {
		pathTemplate = "{mission}"
	}
	missionPathRel := strings.ReplaceAll(pathTemplate, "{mission}", missionName)
	missionPathAbs := filepath.Join(rt.MissionDir, missionPathRel)

	if rt.ValidateMissionFile {
		if _, err := os.Stat(missionPathAbs); err != nil {
			if os.IsNotExist(err) {
				return nil, fmt.Errorf("%w: %s", ErrMissionNotFound, missionPathAbs)
			}
			return nil, err
		}
		// Containment check: resolved path must remain inside resolved mission_dir.
		if _, err := fsutil.ContainedPath(rt.MissionDir, missionPathAbs); err != nil {
			return nil, fmt.Errorf("%w: %v", ErrMissionNotInDir, err)
		}
	}

	// Decode command template.
	var argvTemplate []string
	ct := strings.TrimSpace(rt.CommandTemplate)
	if ct == "" || ct == "null" {
		argvTemplate = []string{"{mission_path}"}
	} else {
		if err := json.Unmarshal([]byte(ct), &argvTemplate); err != nil {
			return nil, fmt.Errorf("invalid command_template: %w", err)
		}
		if len(argvTemplate) == 0 {
			argvTemplate = []string{"{mission_path}"}
		}
	}

	// Substitute placeholders in each argv element.
	argv := make([]string, len(argvTemplate))
	for i, a := range argvTemplate {
		a = strings.ReplaceAll(a, "{mission}", missionName)
		a = strings.ReplaceAll(a, "{mission_path}", missionPathAbs)
		argv[i] = a
	}
	return argv, nil
}
