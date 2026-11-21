package calendar_test

import (
	"testing"
	"time"

	"kitewheel/internal/calendar"
)

func mustLoc(t *testing.T, r calendar.Rule) *time.Location {
	t.Helper()
	return time.FixedZone("cst", 8*3600)
}

func dueAt(t *testing.T, s string) int64 {
	t.Helper()
	tm, err := time.ParseInLocation("2006-01-02 15:04", s, time.FixedZone("cst", 8*3600))
	if err != nil {
		t.Fatalf("parse %q: %v", s, err)
	}
	return tm.UnixNano()
}

func datesOf(t *testing.T, r calendar.Rule, n int) []string {
	t.Helper()
	occs, err := r.Upcoming(n)
	if err != nil {
		t.Fatalf("upcoming: %v", err)
	}
	out := make([]string, len(occs))
	for i, o := range occs {
		out[i] = time.Unix(0, o.Due).In(time.FixedZone("cst", 8*3600)).Format("2006-01-02 15:04")
	}
	return out
}

func anchorAt(t *testing.T, s string) int64 {
	t.Helper()
	tm, err := time.ParseInLocation("2006-01-02 15:04", s, time.FixedZone("cst", 8*3600))
	if err != nil {
		t.Fatal(err)
	}
	return tm.UnixNano()
}

func eq(t *testing.T, got, want []string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("got %d, want %d\ngot  %v\nwant %v", len(got), len(want), got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("index %d: got %q want %q\ngot  %v\nwant %v", i, got[i], want[i], got, want)
		}
	}
}

func TestMonthlyDay31SkipAndClamp(t *testing.T) {
	anchor := anchorAt(t, "2028-01-05 10:00")
	skip := calendar.Rule{
		ID: "d31", Kind: calendar.KindMonthlyDay, Day: 31,
		TimeOfDay: "09:30", Missing: calendar.MissingSkip,
		Created: anchor, Payload: "x",
	}
	eq(t, datesOf(t, skip, 9), []string{
		"2028-01-31 09:30", "2028-03-31 09:30", "2028-05-31 09:30",
		"2028-07-31 09:30", "2028-08-31 09:30", "2028-10-31 09:30",
		"2028-12-31 09:30", "2029-01-31 09:30", "2029-03-31 09:30",
	})

	clamp := skip
	clamp.Missing = calendar.MissingClamp
	eq(t, datesOf(t, clamp, 8), []string{
		"2028-01-31 09:30", "2028-02-29 09:30", "2028-03-31 09:30",
		"2028-04-30 09:30", "2028-05-31 09:30", "2028-06-30 09:30",
		"2028-07-31 09:30", "2028-08-31 09:30",
	})
}

func TestLeapFebruary(t *testing.T) {
	// 2028 is a leap year, 2029 and 2100 are not.
	anchor := anchorAt(t, "2028-02-01 00:00")
	r := calendar.Rule{
		ID: "feb29", Kind: calendar.KindMonthlyDay, Day: 29,
		TimeOfDay: "00:05", Missing: calendar.MissingClamp, Created: anchor,
	}
	eq(t, datesOf(t, r, 13), []string{
		"2028-02-29 00:05", "2028-03-29 00:05", "2028-04-29 00:05",
		"2028-05-29 00:05", "2028-06-29 00:05", "2028-07-29 00:05",
		"2028-08-29 00:05", "2028-09-29 00:05", "2028-10-29 00:05",
		"2028-11-29 00:05", "2028-12-29 00:05", "2029-01-29 00:05",
		"2029-02-28 00:05",
	})

	// A yearly 29 February rule expressed as a date list.
	yearly := calendar.Rule{
		ID: "y", Kind: calendar.KindDates, TimeOfDay: "12:00",
		Dates:   []string{"2028-02-29", "2032-02-29", "2036-02-29"},
		Created: anchor,
	}
	eq(t, datesOf(t, yearly, 3), []string{
		"2028-02-29 12:00", "2032-02-29 12:00", "2036-02-29 12:00",
	})

	// A 31st-of-month rule crossing February 2100 (not a leap year).
	r2100 := calendar.Rule{
		ID: "c", Kind: calendar.KindMonthlyDay, Day: 31,
		TimeOfDay: "08:00", Missing: calendar.MissingClamp, Created: anchorAt(t, "2100-01-01 00:00"),
	}
	got := datesOf(t, r2100, 3)
	eq(t, got, []string{"2100-01-31 08:00", "2100-02-28 08:00", "2100-03-31 08:00"})
}

func TestMonthlyLastSaturday(t *testing.T) {
	anchor := anchorAt(t, "2026-11-01 00:00")
	r := calendar.Rule{
		ID: "lastsat", Kind: calendar.KindMonthlyNthWeekday,
		Nth: -1, Weekday: int(time.Saturday), TimeOfDay: "16:00",
		OffsetSec: -1800, Created: anchor, // 30 minutes before
	}
	eq(t, datesOf(t, r, 6), []string{
		"2026-11-28 15:30", "2026-12-26 15:30", "2027-01-30 15:30",
		"2027-02-27 15:30", "2027-03-27 15:30", "2027-04-24 15:30",
	})
}

func TestMonthlyNthWeekdayFifthMissing(t *testing.T) {
	anchor := anchorAt(t, "2027-01-01 00:00")
	r := calendar.Rule{
		ID: "5th", Kind: calendar.KindMonthlyNthWeekday, Nth: 5,
		Weekday: int(time.Sunday), TimeOfDay: "10:00",
		Missing: calendar.MissingSkip, Created: anchor,
	}
	// January 2027 has 5 Sundays (3,10,17,24,31); February has 4.
	eq(t, datesOf(t, r, 5), []string{
		"2027-01-31 10:00", "2027-05-30 10:00", "2027-08-29 10:00",
		"2027-10-31 10:00", "2028-01-30 10:00",
	})
	clamp := r
	clamp.Missing = calendar.MissingClamp
	if got := datesOf(t, clamp, 2); got[0] != "2027-01-31 10:00" || got[1] != "2027-02-28 10:00" {
		t.Fatalf("clamp: %v", got)
	}
}

func TestMonthlyNthWorkdayWithHolidays(t *testing.T) {
	anchor := anchorAt(t, "2027-09-01 00:00")
	r := calendar.Rule{
		ID: "pay", Kind: calendar.KindMonthlyNthWorkday, Nth: 3,
		TimeOfDay: "09:00", OffsetSec: -3600,
		Holidays: []string{"2027-09-06", "2027-10-01", "2027-10-04", "2027-10-05", "2027-10-06", "2027-10-07"},
		Created:  anchor,
	}
	// September 2027: 1 Wed, 2 Thu, 3 Fri are the first three working days, so
	// the third is 3 September. October 2027: 1 Fri is a holiday and 4-7 are
	// holidays, so the third working day is 12 October.
	eq(t, datesOf(t, r, 3), []string{
		"2027-09-03 08:00", "2027-10-12 08:00", "2027-11-03 08:00",
	})

	last := r
	last.Nth = -1
	eq(t, datesOf(t, last, 2), []string{"2027-09-30 08:00", "2027-10-29 08:00"})
}

func TestOffsetAcrossMidnight(t *testing.T) {
	anchor := anchorAt(t, "2027-01-01 00:00")
	r := calendar.Rule{
		ID: "late", Kind: calendar.KindMonthlyDay, Day: 1,
		TimeOfDay: "00:15", OffsetSec: -30 * 60, Created: anchor,
	}
	// 00:15 minus 30 minutes falls on the previous day.
	eq(t, datesOf(t, r, 3), []string{
		"2026-12-31 23:45", "2027-01-31 23:45", "2027-02-28 23:45",
	})
}

func TestExplicitDatesAcrossYearBoundary(t *testing.T) {
	r := calendar.Rule{
		ID: "fest", Kind: calendar.KindDates, TimeOfDay: "09:00",
		Dates:   []string{"2027-12-31", "2028-01-01", "2028-01-02"},
		Created: anchorAt(t, "2027-06-01 00:00"),
	}
	eq(t, datesOf(t, r, 3), []string{"2027-12-31 09:00", "2028-01-01 09:00", "2028-01-02 09:00"})
	if _, err := r.Next(); err != nil {
		// Next starts at index 0, fine.
		t.Fatal(err)
	}
	r.NextIdx = 3
	if _, err := r.Next(); err != calendar.ErrExhausted {
		t.Fatalf("want ErrExhausted, got %v", err)
	}
}

func TestRuleValidation(t *testing.T) {
	bad := []calendar.Rule{
		{Kind: calendar.KindMonthlyDay, Day: 0, TimeOfDay: "09:00"},
		{Kind: calendar.KindMonthlyDay, Day: 32, TimeOfDay: "09:00"},
		{Kind: calendar.KindMonthlyNthWeekday, Nth: 0, Weekday: 1, TimeOfDay: "09:00"},
		{Kind: calendar.KindMonthlyNthWeekday, Nth: 1, Weekday: 7, TimeOfDay: "09:00"},
		{Kind: calendar.KindMonthlyNthWorkday, Nth: 0, TimeOfDay: "09:00"},
		{Kind: calendar.KindMonthlyDay, Day: 1, TimeOfDay: "25:00"},
		{Kind: calendar.KindMonthlyDay, Day: 1, TimeOfDay: "09:00", Missing: "nearest"},
		{Kind: calendar.KindDates, TimeOfDay: "09:00"},
		{Kind: "weekly", TimeOfDay: "09:00"},
	}
	for i, r := range bad {
		if err := r.Validate(); err == nil {
			t.Fatalf("case %d should be invalid: %+v", i, r)
		}
	}
}
