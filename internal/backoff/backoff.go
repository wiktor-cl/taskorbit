// Package backoff computes exponential retry delays with a cap, used when
// a task run fails and still has attempts remaining.
package backoff

import (
	"math"
	"time"
)

type Config struct {
	Base       time.Duration
	Max        time.Duration
	Multiplier float64
}

// Default is a reasonable general-purpose backoff: 1s, 2s, 4s, 8s, ...
// capped at 5 minutes.
func Default() Config {
	return Config{Base: time.Second, Max: 5 * time.Minute, Multiplier: 2.0}
}

// Delay returns the delay before the next attempt. attempt is 1-indexed:
// attempt 1 is the delay scheduled after the first failure (before the
// second try), attempt 2 after the second failure, and so on. Values less
// than 1 are treated as 1.
func (c Config) Delay(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	d := float64(c.Base) * math.Pow(c.Multiplier, float64(attempt-1))
	if math.IsInf(d, 1) || d > float64(c.Max) {
		return c.Max
	}
	return time.Duration(d)
}
