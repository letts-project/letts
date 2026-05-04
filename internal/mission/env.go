package mission

import (
	"fmt"
	"strings"

	"letts/internal/config"
)

// EnvInputs describes a per-mission file delivered to LETTS_IN_<role>.
type EnvInputs struct {
	Role   string
	Path   string
	Sha256 string
	Size   int64
}

// BaseVars carries per-mission identity used for env composition.
type BaseVars struct {
	MissionID string
	Kind      string
	Lane      string
	Workdir   string
}

// BuildEnv returns the env slice for exec.Cmd (KEY=VAL strings).
//
// dugdaleHome is the home of the dugdale unix user (used for HOME). cfg is
// the mission_env section. inputs are pre-resolved input files. vars carries
// per-mission identity fields. lookup resolves environment variable values
// for Inherit and Set expansion; pass os.LookupEnv in production.
//
// Always emits: PATH, HOME, TZ, LETTS_MISSION_ID, LETTS_KIND, LETTS_LANE,
// LETTS_WORKDIR, LETTS_TMPDIR.
// Per input: LETTS_IN_<role>, LETTS_IN_<role>__SHA256, LETTS_IN_<role>__SIZE.
// Inherit: passes through non-LETTS_ variables listed in cfg.Inherit.
// Set: applies ${ENV} expansion; rejects LETTS_* keys.
func BuildEnv(
	dugdaleHome string,
	cfg config.MissionEnvConfig,
	inputs []EnvInputs,
	vars BaseVars,
	lookup func(string) (string, bool),
) ([]string, error) {
	out := []string{
		"PATH=/usr/local/bin:/usr/bin:/bin",
		"HOME=" + dugdaleHome,
		"TZ=UTC",
		"LETTS_MISSION_ID=" + vars.MissionID,
		"LETTS_KIND=" + vars.Kind,
		"LETTS_LANE=" + vars.Lane,
		"LETTS_WORKDIR=" + vars.Workdir,
		"LETTS_TMPDIR=" + vars.Workdir + "/tmp",
	}

	for _, in := range inputs {
		out = append(out,
			"LETTS_IN_"+in.Role+"="+in.Path,
			"LETTS_IN_"+in.Role+"__SHA256="+in.Sha256,
			fmt.Sprintf("LETTS_IN_%s__SIZE=%d", in.Role, in.Size),
		)
	}

	for _, name := range cfg.Inherit {
		if strings.HasPrefix(name, "LETTS_") {
			continue // reserved namespace — skip silently
		}
		if v, ok := lookup(name); ok {
			out = append(out, name+"="+v)
		}
	}

	for k, v := range cfg.Set {
		if strings.HasPrefix(k, "LETTS_") {
			return nil, fmt.Errorf("mission_env.set may not override LETTS_* (%s)", k)
		}
		expanded, err := config.ExpandEnv(v, lookup)
		if err != nil {
			return nil, err
		}
		out = append(out, k+"="+expanded)
	}

	return out, nil
}
