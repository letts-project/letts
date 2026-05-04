// Package version exposes build-time version metadata.
package version

// These are set via -ldflags at build time. Keep them as vars so the linker can override.
var (
	Version = "dev"
	Commit  = "unknown"
	BuiltAt = "unknown"
)
