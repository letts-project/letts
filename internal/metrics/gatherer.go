package metrics

import "github.com/prometheus/client_golang/prometheus"

// defaultGatherer wraps prometheus.DefaultGatherer for the test smoke check.
// Exported under an internal name so test code can ask "what's registered?"
// without depending directly on the global.
func defaultGatherer() prometheus.Gatherer { return prometheus.DefaultGatherer }
