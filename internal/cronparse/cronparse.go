// Package cronparse implements a minimal standard 5-field cron expression
// parser (minute hour day-of-month month day-of-week) and next-run
// calculation. It intentionally doesn't pull in a third-party cron
// library — the format is small and well-specified enough that a direct
// implementation is both simpler to audit and more instructive here than
// a dependency would be.
package cronparse

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

// Schedule is a parsed cron expression, represented as one bitmask per
// field for O(1) membership tests.
type Schedule struct {
	minute     uint64 // bits 0-59
	hour       uint64 // bits 0-23
	dayOfMonth uint64 // bits 1-31
	month      uint64 // bits 1-12
	dayOfWeek  uint64 // bits 0-6, 0 = Sunday

	domRestricted bool
	dowRestricted bool
}

// Parse parses a standard 5-field cron expression: "minute hour dom month dow".
func Parse(expr string) (*Schedule, error) {
	fields := strings.Fields(expr)
	if len(fields) != 5 {
		return nil, fmt.Errorf("cronparse: expected 5 fields (minute hour dom month dow), got %d in %q", len(fields), expr)
	}

	minute, err := parseField(fields[0], 0, 59)
	if err != nil {
		return nil, fmt.Errorf("cronparse: minute field: %w", err)
	}
	hour, err := parseField(fields[1], 0, 23)
	if err != nil {
		return nil, fmt.Errorf("cronparse: hour field: %w", err)
	}
	dom, err := parseField(fields[2], 1, 31)
	if err != nil {
		return nil, fmt.Errorf("cronparse: day-of-month field: %w", err)
	}
	month, err := parseField(fields[3], 1, 12)
	if err != nil {
		return nil, fmt.Errorf("cronparse: month field: %w", err)
	}
	dow, err := parseField(fields[4], 0, 6)
	if err != nil {
		return nil, fmt.Errorf("cronparse: day-of-week field: %w", err)
	}

	return &Schedule{
		minute:        minute,
		hour:          hour,
		dayOfMonth:    dom,
		month:         month,
		dayOfWeek:     dow,
		domRestricted: fields[2] != "*",
		dowRestricted: fields[4] != "*",
	}, nil
}

const maxLookaheadMinutes = 4 * 366 * 24 * 60 // ~4 years, a generous ceiling for any sane expression

// Next returns the first time strictly after `after`, truncated to the
// minute, that matches the schedule.
func (s *Schedule) Next(after time.Time) (time.Time, error) {
	t := after.Truncate(time.Minute).Add(time.Minute)

	for i := 0; i < maxLookaheadMinutes; i++ {
		if s.matches(t) {
			return t, nil
		}
		t = t.Add(time.Minute)
	}
	return time.Time{}, fmt.Errorf("cronparse: no matching run time found within lookahead window")
}

func (s *Schedule) matches(t time.Time) bool {
	if !bitSet(s.month, uint(t.Month())) {
		return false
	}
	if !bitSet(s.hour, uint(t.Hour())) {
		return false
	}
	if !bitSet(s.minute, uint(t.Minute())) {
		return false
	}

	domMatch := bitSet(s.dayOfMonth, uint(t.Day()))
	dowMatch := bitSet(s.dayOfWeek, uint(t.Weekday()))

	// Standard (Vixie) cron quirk: if BOTH day-of-month and day-of-week are
	// restricted (neither is "*"), a day matching either one is enough. If
	// only one is restricted, only that one has to match.
	switch {
	case s.domRestricted && s.dowRestricted:
		return domMatch || dowMatch
	case s.domRestricted:
		return domMatch
	case s.dowRestricted:
		return dowMatch
	default:
		return true
	}
}

func bitSet(mask uint64, n uint) bool {
	return mask&(1<<n) != 0
}

// parseField parses one comma-separated cron field (each part possibly a
// "*", a single number, a range "a-b", or any of those with a "/step").
func parseField(field string, min, max int) (uint64, error) {
	var mask uint64
	for _, part := range strings.Split(field, ",") {
		lo, hi, step, err := parseRangePart(part, min, max)
		if err != nil {
			return 0, err
		}
		for v := lo; v <= hi; v += step {
			mask |= 1 << uint(v)
		}
	}
	if mask == 0 {
		return 0, fmt.Errorf("field %q matches nothing", field)
	}
	return mask, nil
}

func parseRangePart(part string, min, max int) (lo, hi, step int, err error) {
	step = 1
	base := part
	if idx := strings.IndexByte(part, '/'); idx >= 0 {
		base = part[:idx]
		step, err = strconv.Atoi(part[idx+1:])
		if err != nil || step <= 0 {
			return 0, 0, 0, fmt.Errorf("invalid step in %q", part)
		}
	}

	switch {
	case base == "*":
		lo, hi = min, max
	case strings.Contains(base, "-"):
		bounds := strings.SplitN(base, "-", 2)
		lo, err = strconv.Atoi(bounds[0])
		if err != nil {
			return 0, 0, 0, fmt.Errorf("invalid range start in %q", part)
		}
		hi, err = strconv.Atoi(bounds[1])
		if err != nil {
			return 0, 0, 0, fmt.Errorf("invalid range end in %q", part)
		}
	default:
		v, convErr := strconv.Atoi(base)
		if convErr != nil {
			return 0, 0, 0, fmt.Errorf("invalid value %q", part)
		}
		lo, hi = v, v
	}

	if lo < min || hi > max || lo > hi {
		return 0, 0, 0, fmt.Errorf("value %q out of range [%d,%d]", part, min, max)
	}
	return lo, hi, step, nil
}
