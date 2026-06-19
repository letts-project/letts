// Package lettsconfig parses and resolves letts.yaml (client-side config).
// Distinct from internal/config which handles dugdale.yaml (daemon-side).
package lettsconfig

// Config is the top-level letts.yaml shape.
type Config struct {
	Auth      Auth                `yaml:"auth,omitempty"`
	Defaults  Defaults            `yaml:"defaults,omitempty"`
	Selector  Selector            `yaml:"selector,omitempty"`
	Routes    map[string]Route    `yaml:"routes,omitempty"`
	Aliases   map[string]string   `yaml:"aliases,omitempty"`
	Templates map[string]Template `yaml:"templates,omitempty"`
	Dugdales  []Dugdale           `yaml:"dugdales"`
}

// Auth holds the three global token fallbacks.
type Auth struct {
	Token      string `yaml:"token,omitempty"`       // dispatch
	AdminToken string `yaml:"admin_token,omitempty"` // admin
	ExecToken  string `yaml:"exec_token,omitempty"`  // exec
}

// Defaults carries optional cluster-wide defaults.
type Defaults struct {
	Port int `yaml:"port,omitempty"` // default port for dugdales[].port
}

// Selector configures auto-host-select default label filter.
type Selector struct {
	Match []string `yaml:"match,omitempty"`
}

// Route maps a symbolic route name to host+lane.
type Route struct {
	Host string `yaml:"host"`
	Lane string `yaml:"lane"`
}

// Template is a reusable cluster of fields referenced via Dugdale.Extends.
type Template struct {
	MissionDir string             `yaml:"mission_dir,omitempty"`
	Runtime    Runtime            `yaml:"runtime,omitempty"`
	Labels     []string           `yaml:"labels,omitempty"`
	Token      string             `yaml:"token,omitempty"`
	AdminToken string             `yaml:"admin_token,omitempty"`
	ExecToken  string             `yaml:"exec_token,omitempty"`
	Proxy      string             `yaml:"proxy,omitempty"`
	Lanes      map[string]LaneCfg `yaml:"lanes,omitempty"`
}

// Runtime mirrors apply.Runtime for serialization-compat.
type Runtime struct {
	MissionPathTemplate string   `yaml:"mission_path_template,omitempty"`
	CommandTemplate     []string `yaml:"command_template,omitempty"`
	ValidateMissionFile bool     `yaml:"validate_mission_file,omitempty"`
}

// LaneCfg mirrors apply.LaneCfg.
type LaneCfg struct {
	Concurrency int  `yaml:"concurrency"`
	Paused      bool `yaml:"paused,omitempty"`
}

// Dugdale is one server entry.
//
// One of Host (+optional Port) or URL must be present after resolution.
// Extends names a Template; deep-merge handled by extends.go.
//
// nullifiedLanes carries the names of lanes the YAML explicitly set to
// `null`, surfaced by the custom UnmarshalYAML. ResolveExtends drops
// those names from the inherited template lanes so operators can
// suppress an inherited lane without redefining the rest
// ("lanes: <name>: null" deletion semantic).
type Dugdale struct {
	ID             string             `yaml:"id"`
	Host           string             `yaml:"host,omitempty"`
	Port           int                `yaml:"port,omitempty"`
	URL            string             `yaml:"url,omitempty"`
	Proxy          string             `yaml:"proxy,omitempty"`
	Extends        string             `yaml:"extends,omitempty"`
	MissionDir     string             `yaml:"mission_dir,omitempty"`
	Runtime        Runtime            `yaml:"runtime,omitempty"`
	Labels         []string           `yaml:"labels,omitempty"`
	Token          string             `yaml:"token,omitempty"`
	AdminToken     string             `yaml:"admin_token,omitempty"`
	ExecToken      string             `yaml:"exec_token,omitempty"`
	Lanes          map[string]LaneCfg `yaml:"lanes,omitempty"`
	nullifiedLanes []string           `yaml:"-"`
	// runtimeValidateMissionFileSet records whether the YAML carried a
	// value for runtime.validate_mission_file on this dugdale. yaml.v3
	// can't tell `false` from `unset` on a bool field, so without this
	// sentinel the extends merge can't honor an explicit false from
	// dugdale over an inherited true from template.
	runtimeValidateMissionFileSet bool `yaml:"-"`
}

// HasExplicitValidateMissionFile reports whether the original YAML for
// this dugdale carried an explicit value for runtime.validate_mission_file
// (true OR false). Lets a layered-merge or extends step preserve the
// operator's intent across the yaml.v3 unset/false ambiguity.
func (d Dugdale) HasExplicitValidateMissionFile() bool {
	return d.runtimeValidateMissionFileSet
}

// SetExplicitValidateMissionFile is the writer side of the accessor: the
// merge step uses it to flag that the resulting Dugdale carries an
// explicit value too (because overlay supplied one). Public so apply_merge
// in cmd/letts can build correctly-flagged merged entries.
func (d *Dugdale) SetExplicitValidateMissionFile(set bool) {
	d.runtimeValidateMissionFileSet = set
}

// NullifiedLanes returns the lane names this dugdale's YAML explicitly
// set to `null` (instructing extends/merge to drop the inherited lane).
func (d Dugdale) NullifiedLanes() []string {
	return append([]string(nil), d.nullifiedLanes...)
}

// SetNullifiedLanes is the writer side of NullifiedLanes for the merge
// step in cmd/letts.
func (d *Dugdale) SetNullifiedLanes(names []string) {
	if len(names) == 0 {
		d.nullifiedLanes = nil
		return
	}
	d.nullifiedLanes = append(d.nullifiedLanes[:0], names...)
}

// HasLane returns true if the dugdale exposes the named lane.
func (d *Dugdale) HasLane(lane string) bool {
	_, ok := d.Lanes[lane]
	return ok
}

// HasLabel returns true if labels contain s.
func (d *Dugdale) HasLabel(s string) bool {
	for _, l := range d.Labels {
		if l == s {
			return true
		}
	}
	return false
}
