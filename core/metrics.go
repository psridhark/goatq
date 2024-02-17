package core

import "github.com/go-kit/kit/metrics/prometheus"

// Counter Alias prometheus' counter, probably only need to use Inc() though.
type Counter = prometheus.Counter

// Metrics is used for tracking metrics.
type Metrics struct {
	RequestsHandled Counter
}
