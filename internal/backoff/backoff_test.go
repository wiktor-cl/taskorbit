package backoff_test

import (
	"testing"
	"time"

	"github.com/wiktor-cl/taskorbit/internal/backoff"
)

func TestDelay_ExponentialGrowth(t *testing.T) {
	c := backoff.Config{Base: time.Second, Max: time.Hour, Multiplier: 2.0}

	cases := []struct {
		attempt int
		want    time.Duration
	}{
		{1, 1 * time.Second},
		{2, 2 * time.Second},
		{3, 4 * time.Second},
		{4, 8 * time.Second},
		{5, 16 * time.Second},
	}
	for _, tc := range cases {
		got := c.Delay(tc.attempt)
		if got != tc.want {
			t.Errorf("Delay(%d) = %v, want %v", tc.attempt, got, tc.want)
		}
	}
}

func TestDelay_CapsAtMax(t *testing.T) {
	c := backoff.Config{Base: time.Second, Max: 30 * time.Second, Multiplier: 2.0}

	got := c.Delay(10) // 2^9 seconds uncapped, way past Max
	if got != 30*time.Second {
		t.Errorf("Delay(10) = %v, want capped at %v", got, 30*time.Second)
	}
}

func TestDelay_NonPositiveAttemptTreatedAsOne(t *testing.T) {
	c := backoff.Config{Base: time.Second, Max: time.Hour, Multiplier: 2.0}

	for _, attempt := range []int{0, -1, -100} {
		got := c.Delay(attempt)
		if got != time.Second {
			t.Errorf("Delay(%d) = %v, want %v (same as attempt 1)", attempt, got, time.Second)
		}
	}
}

func TestDefault_IsSane(t *testing.T) {
	c := backoff.Default()
	if c.Delay(1) != time.Second {
		t.Errorf("Default Delay(1) = %v, want 1s", c.Delay(1))
	}
	if c.Delay(20) != c.Max {
		t.Errorf("Default Delay(20) = %v, want capped at Max %v", c.Delay(20), c.Max)
	}
}
