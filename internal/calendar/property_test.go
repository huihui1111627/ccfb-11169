package calendar_test

import (
	"fmt"
	"testing"
	"time"

	"kitewheel/internal/calendar"
)

// TestOccurrencesAreStrictlyIncreasingAndInTZ checks structural invariants for
// every rule kind over a multi-year window: due times strictly increase within
// a rule, and each event falls on the day and time the description claims.
func TestOccurrencesAreStrictlyIncreasingAndInTZ(t *testing.T) {
	loc := time.FixedZone("cst", 8*3600)
	anchor := anchorAt(t, "2027-11-15 12:00")
	rules := []calendar.Rule{
		{ID: "d31skip", Kind: calendar.KindMonthlyDay, Day: 31, TimeOfDay: "23:00", Missing: calendar.MissingSkip},
		{ID: "d31clamp", Kind: calendar.KindMonthlyDay, Day: 31, TimeOfDay: "23:00", Missing: calendar.MissingClamp},
		{ID: "lastsat", Kind: calendar.KindMonthlyNthWeekday, Nth: -1, Weekday: 6, TimeOfDay: "16:00"},
		{ID: "fifth", Kind: calendar.KindMonthlyNthWeekday, Nth: 5, Weekday: 0, TimeOfDay: "07:00", Missing: calendar.MissingSkip},
		{ID: "work3", Kind: calendar.KindMonthlyNthWorkday, Nth: 3, TimeOfDay: "09:00",
			Holidays: []string{"2028-01-03", "2028-05-01", "2028-10-02"}},
		{ID: "lastwork", Kind: calendar.KindMonthlyNthWorkday, Nth: -1, TimeOfDay: "18:30", OffsetSec: -1800},
	}
	for _, r := range rules {
		r.Created = anchor
		t.Run(r.ID, func(t *testing.T) {
			var prev int64 = -1 << 62
			occs, err := r.Upcoming(80)
			if err != nil {
				t.Fatal(err)
			}
			if len(occs) < 40 {
				t.Fatalf("only %d occurrences", len(occs))
			}
			for i, o := range occs {
				if o.Due <= prev {
					t.Fatalf("occurrence %d (%s) not after previous", i, o.Description)
				}
				prev = o.Due
				ev := time.Unix(0, o.Event).In(loc)
				if ev.Hour() != mustHour(t, r.TimeOfDay) {
					t.Fatalf("%s: event hour %d", o.Description, ev.Hour())
				}
				if r.Kind == calendar.KindMonthlyNthWeekday {
					want := time.Weekday(r.Weekday)
					if ev.Weekday() != want {
						t.Fatalf("%s: weekday %v want %v", o.Description, ev.Weekday(), want)
					}
				}
				if r.Kind == calendar.KindMonthlyNthWorkday {
					if wd := ev.Weekday(); wd == time.Saturday || wd == time.Sunday {
						t.Fatalf("%s: fell on a weekend", o.Description)
					}
					for _, h := range r.Holidays {
						if ev.Format("2006-01-02") == h {
							t.Fatalf("%s: fell on holiday %s", o.Description, h)
						}
					}
				}
				if want := o.Event + r.OffsetSec*int64(time.Second); want != o.Due {
					t.Fatalf("%s: due %d is not event %d plus offset", o.Description, o.Due, o.Event)
				}
			}
		})
	}
}

func mustHour(t *testing.T, hhmm string) int {
	t.Helper()
	var h, m int
	if _, err := fmt.Sscanf(hhmm, "%d:%d", &h, &m); err != nil {
		t.Fatal(err)
	}
	return h
}

// TestMonthdaySkipVsClampCounts compares the two policies over a four-year
// window: skip must drop Feb/Apr/Jun/Sep/Nov 31sts, clamp must keep every
// month with the short month landing on its last day.
func TestMonthdaySkipVsClampCounts(t *testing.T) {
	anchor := anchorAt(t, "2027-01-01 00:00")
	base := calendar.Rule{ID: "x", Kind: calendar.KindMonthlyDay, Day: 31, TimeOfDay: "12:00", Created: anchor}
	clamp := base
	clamp.Missing = calendar.MissingClamp
	// An empty Missing policy defaults to skip.
	skip := calendar.Rule{ID: "skip", Kind: calendar.KindMonthlyDay, Day: 31,
		TimeOfDay: "12:00", Created: anchor}

	// Walk 48 months by index, which is the rule's month sequence, and count
	// how many indices each policy actually produces.
	const months = 48
	var skipCount, clampCount int
	loc := time.FixedZone("cst", 8*3600)
	for i := int64(0); i < months; i++ {
		if o, ok := skip.OccurrenceAt(i); ok {
			skipCount++
			if o.Index != i {
				t.Fatalf("skip occurrence index %d, want %d", o.Index, i)
			}
		}
		o, ok := clamp.OccurrenceAt(i)
		if !ok {
			t.Fatalf("clamp dropped month index %d", i)
		}
		clampCount++
		d := time.Unix(0, o.Event).In(loc)
		last := time.Date(d.Year(), d.Month()+1, 0, 0, 0, 0, 0, loc).Day()
		if d.Day() != last {
			t.Fatalf("%s: clamped occurrence is not the last day of its month", o.Description)
		}
		if d.Year() == 2028 && d.Month() == time.February && d.Day() != 29 {
			t.Fatalf("leap February clamped to %d, want 29", d.Day())
		}
		if d.Year() == 2029 && d.Month() == time.February && d.Day() != 28 {
			t.Fatalf("non-leap February clamped to %d, want 28", d.Day())
		}
	}
	if clampCount != months {
		t.Fatalf("clamp produced %d of %d months", clampCount, months)
	}
	// 48 months starting 2027-01 includes 12 months of 2027, 12 of 2028 (a
	// leap year), 12 of 2029 and 12 of 2030; each year has seven 31-day months.
	if skipCount != 28 {
		t.Fatalf("skip produced %d of %d months, want 28", skipCount, months)
	}
}
