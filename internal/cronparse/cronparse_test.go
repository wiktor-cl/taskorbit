package cronparse_test

import (
	"testing"
	"time"

	"github.com/wiktor-cl/taskorbit/internal/cronparse"
)

func mustParse(t *testing.T, expr string) *cronparse.Schedule {
	t.Helper()
	s, err := cronparse.Parse(expr)
	if err != nil {
		t.Fatalf("Parse(%q) failed: %v", expr, err)
	}
	return s
}

func at(s string) time.Time {
	t, err := time.Parse("2006-01-02 15:04", s)
	if err != nil {
		panic(err)
	}
	return t
}

func TestNext_EveryMinute(t *testing.T) {
	s := mustParse(t, "* * * * *")
	got, err := s.Next(at("2026-01-01 10:00"))
	if err != nil {
		t.Fatalf("Next: %v", err)
	}
	want := at("2026-01-01 10:01")
	if !got.Equal(want) {
		t.Fatalf("got %v, want %v", got, want)
	}
}

func TestNext_SpecificHourAndMinute(t *testing.T) {
	// Every day at 09:30.
	s := mustParse(t, "30 9 * * *")

	got, err := s.Next(at("2026-01-01 08:00"))
	if err != nil {
		t.Fatalf("Next: %v", err)
	}
	want := at("2026-01-01 09:30")
	if !got.Equal(want) {
		t.Fatalf("got %v, want %v", got, want)
	}

	// Asking after the window today should roll to tomorrow.
	got2, err := s.Next(at("2026-01-01 10:00"))
	if err != nil {
		t.Fatalf("Next: %v", err)
	}
	want2 := at("2026-01-02 09:30")
	if !got2.Equal(want2) {
		t.Fatalf("got %v, want %v", got2, want2)
	}
}

func TestNext_RangeAndStep(t *testing.T) {
	// Every 15 minutes, business hours (9-17), weekdays.
	s := mustParse(t, "*/15 9-17 * * 1-5")

	// 2026-01-05 is a Monday.
	got, err := s.Next(at("2026-01-05 09:00"))
	if err != nil {
		t.Fatalf("Next: %v", err)
	}
	want := at("2026-01-05 09:15")
	if !got.Equal(want) {
		t.Fatalf("got %v, want %v", got, want)
	}
}

func TestNext_RangeAndStep_RollsPastBusinessHoursToNextWeekday(t *testing.T) {
	s := mustParse(t, "*/15 9-17 * * 1-5")

	// 2026-01-09 is a Friday; after 17:00 should roll to Monday 2026-01-12 09:00.
	got, err := s.Next(at("2026-01-09 17:45"))
	if err != nil {
		t.Fatalf("Next: %v", err)
	}
	want := at("2026-01-12 09:00")
	if !got.Equal(want) {
		t.Fatalf("got %v, want %v", got, want)
	}
}

func TestNext_List(t *testing.T) {
	// At minute 0, on hours 6, 12, 18.
	s := mustParse(t, "0 6,12,18 * * *")

	got, err := s.Next(at("2026-01-01 06:30"))
	if err != nil {
		t.Fatalf("Next: %v", err)
	}
	want := at("2026-01-01 12:00")
	if !got.Equal(want) {
		t.Fatalf("got %v, want %v", got, want)
	}
}

func TestNext_DayOfMonthAndDayOfWeek_AreOred(t *testing.T) {
	// Standard cron quirk: when BOTH day-of-month and day-of-week are
	// restricted, a match on either is sufficient (not an AND).
	// "1st of the month OR a Monday", at midnight.
	s := mustParse(t, "0 0 1 * 1")

	// 2026-01-05 is a Monday (not the 1st) -> should match via day-of-week.
	got, err := s.Next(at("2026-01-04 00:00"))
	if err != nil {
		t.Fatalf("Next: %v", err)
	}
	want := at("2026-01-05 00:00")
	if !got.Equal(want) {
		t.Fatalf("got %v, want %v", got, want)
	}
}

func TestParse_InvalidExpressions(t *testing.T) {
	cases := []string{
		"",
		"* * * *",      // too few fields
		"* * * * * *",  // too many fields
		"60 * * * *",   // minute out of range
		"* 24 * * *",   // hour out of range
		"* * 0 * *",    // day-of-month out of range (min is 1)
		"* * * 13 *",   // month out of range
		"* * * * 7",    // day-of-week out of range (max is 6)
		"abc * * * *",  // not a number
		"1-60 * * * *", // range exceeds max
		"*/0 * * * *",  // zero step
	}
	for _, expr := range cases {
		if _, err := cronparse.Parse(expr); err == nil {
			t.Errorf("Parse(%q) expected an error, got nil", expr)
		}
	}
}

func TestParse_ValidExpressions(t *testing.T) {
	cases := []string{
		"* * * * *",
		"0 0 * * *",
		"*/5 * * * *",
		"0 9-17 * * 1-5",
		"0,15,30,45 * * * *",
		"0 0 1 1 *",
	}
	for _, expr := range cases {
		if _, err := cronparse.Parse(expr); err != nil {
			t.Errorf("Parse(%q) unexpected error: %v", expr, err)
		}
	}
}
