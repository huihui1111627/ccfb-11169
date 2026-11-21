// Package calendar computes occurrences for recurring schedules. It has no
// dependency on the scheduler, so the same rule can both materialize messages
// and be verified independently in tests.
//
// A rule denotes an event time (the start of a flying session, a refund
// deadline, ...) plus an offset applied to it, e.g. -30m for "half an hour
// before the session opens". The occurrence's due time is event + offset.
//
// Supported kinds:
//
//	dates                 an explicit list of calendar dates
//	monthly_day           the Nth day of every month (day 31 handled by policy)
//	monthly_nth_weekday   the Nth weekday of every month (Nth = -1 means last)
//	monthly_nth_workday   the Nth Mon-Fri working day (-1 means last)
//
// Monthly rules index occurrences by consecutive months starting at the month
// in which the rule was created. That anchor is stored with the rule, so
// "month index i" maps to a fixed month for the rule's whole life and a skipped
// month never shifts later occurrence indices.
package calendar

import (
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"
)

// Kind enumerates rule kinds.
type Kind string

// Rule kinds.
const (
	KindDates             Kind = "dates"
	KindMonthlyDay        Kind = "monthly_day"
	KindMonthlyNthWeekday Kind = "monthly_nth_weekday"
	KindMonthlyNthWorkday Kind = "monthly_nth_workday"
)

// Missing is the policy for a month that has no such day.
type Missing string

// Missing policies.
const (
	// MissingSkip drops the occurrence for that month.
	MissingSkip Missing = "skip"
	// MissingClamp moves it to the nearest day the month does have: the last
	// day of the month for monthly_day, the last matching weekday or working
	// day for the nth kinds.
	MissingClamp Missing = "clamp"
)

// DefaultTZOffsetSec is +08:00, the festival's timezone.
const DefaultTZOffsetSec = 8 * 3600

// Rule is a calendar recurrence. Its JSON form is what the log stores.
type Rule struct {
	ID          string   `json:"id"`
	Ord         uint64   `json:"ord"` // creation order, assigned by the engine
	Name        string   `json:"name"`
	Kind        Kind     `json:"kind"`
	TZOffsetSec int      `json:"tz_offset_sec"`
	TimeOfDay   string   `json:"time_of_day"` // "HH:MM"
	OffsetSec   int64    `json:"offset_sec"`  // added to the event time
	Dates       []string `json:"dates,omitempty"`
	Day         int      `json:"day,omitempty"`
	Nth         int      `json:"nth,omitempty"` // -1 means last
	Weekday     int      `json:"weekday,omitempty"`
	Missing     Missing  `json:"missing,omitempty"`
	Holidays    []string `json:"holidays,omitempty"`
	Payload     string   `json:"payload,omitempty"`
	Priority    int      `json:"priority,omitempty"`
	Remark      string   `json:"remark,omitempty"`
	Created     int64    `json:"created"`
	NextIdx     int64    `json:"next_idx"`
}

// Occurrence is one computed instance.
type Occurrence struct {
	Index       int64  `json:"index"`
	Due         int64  `json:"due"` // unix nanoseconds
	Event       int64  `json:"event"`
	Description string `json:"description"`
}

// maxScanMonths bounds Next when a rule can produce nothing for a long time.
const maxScanMonths = 2400 // 200 years

var errInvalid = errors.New("calendar: invalid rule")

type compiled struct {
	loc     *time.Location
	hour    int
	minute  int
	dates   []time.Time
	holiday map[string]bool
	anchor  time.Time
}

// tz returns the rule's fixed UTC offset in seconds.
func (r *Rule) tz() int {
	if r.TZOffsetSec == 0 {
		return DefaultTZOffsetSec
	}
	return r.TZOffsetSec
}

// Location returns the rule's fixed-offset location.
func (r *Rule) Location() *time.Location { return time.FixedZone("rule", r.tz()) }

func parseDate(s string, loc *time.Location) (time.Time, error) {
	return time.ParseInLocation("2006-01-02", s, loc)
}

func dateKey(t time.Time) string { return t.Format("2006-01-02") }

// compile validates the rule and precomputes its constants.
func (r *Rule) compile() (*compiled, error) {
	loc := r.Location()
	c := &compiled{loc: loc, holiday: map[string]bool{}}
	parts := strings.Split(strings.TrimSpace(r.TimeOfDay), ":")
	if len(parts) != 2 {
		return nil, fmt.Errorf("%w: time_of_day must be HH:MM", errInvalid)
	}
	h, err1 := strconv.Atoi(parts[0])
	m, err2 := strconv.Atoi(parts[1])
	if err1 != nil || err2 != nil || h < 0 || h > 23 || m < 0 || m > 59 {
		return nil, fmt.Errorf("%w: time_of_day must be HH:MM", errInvalid)
	}
	c.hour, c.minute = h, m
	for _, hs := range r.Holidays {
		d, err := parseDate(hs, loc)
		if err != nil {
			return nil, fmt.Errorf("%w: holiday %q: %v", errInvalid, hs, err)
		}
		c.holiday[dateKey(d)] = true
	}
	created := time.Unix(0, r.Created).In(loc)
	c.anchor = time.Date(created.Year(), created.Month(), 1, 0, 0, 0, 0, loc)
	switch r.Kind {
	case KindDates:
		if len(r.Dates) == 0 {
			return nil, fmt.Errorf("%w: dates rule needs at least one date", errInvalid)
		}
		for _, s := range r.Dates {
			d, err := parseDate(s, loc)
			if err != nil {
				return nil, fmt.Errorf("%w: date %q: %v", errInvalid, s, err)
			}
			c.dates = append(c.dates, d)
		}
		sort.Slice(c.dates, func(i, j int) bool { return c.dates[i].Before(c.dates[j]) })
	case KindMonthlyDay:
		if r.Day < 1 || r.Day > 31 {
			return nil, fmt.Errorf("%w: day must be 1..31", errInvalid)
		}
		if err := checkMissing(r); err != nil {
			return nil, err
		}
	case KindMonthlyNthWeekday:
		if r.Weekday < 0 || r.Weekday > 6 {
			return nil, fmt.Errorf("%w: weekday must be 0..6 (Sunday=0)", errInvalid)
		}
		if r.Nth == 0 || r.Nth < -1 || r.Nth > 5 {
			return nil, fmt.Errorf("%w: nth must be 1..5 or -1 for last", errInvalid)
		}
		if err := checkMissing(r); err != nil {
			return nil, err
		}
	case KindMonthlyNthWorkday:
		if r.Nth == 0 || r.Nth < -1 || r.Nth > 23 {
			return nil, fmt.Errorf("%w: nth workday must be 1..23 or -1 for last", errInvalid)
		}
		if err := checkMissing(r); err != nil {
			return nil, err
		}
	default:
		return nil, fmt.Errorf("%w: unknown kind %q", errInvalid, r.Kind)
	}
	return c, nil
}

// checkMissing validates the missing-day policy without mutating the rule:
// compile must stay side-effect free so the same rule can be enumerated many
// times concurrently and a rule's stored form stays exactly what was saved.
// An empty policy means skip.
func checkMissing(r *Rule) error {
	switch r.Missing {
	case "", MissingSkip, MissingClamp:
		return nil
	default:
		return fmt.Errorf("%w: missing must be %q or %q", errInvalid, MissingSkip, MissingClamp)
	}
}

func (r *Rule) skipsMissingMonth() bool { return r.Missing != MissingClamp }

func (c *compiled) isWorkday(d time.Time) bool {
	if wd := d.Weekday(); wd == time.Saturday || wd == time.Sunday {
		return false
	}
	return !c.holiday[dateKey(d)]
}

func daysIn(y int, m time.Month) int {
	return time.Date(y, m+1, 0, 0, 0, 0, 0, time.UTC).Day()
}

// nthWeekdayDay returns the day number of the nth weekday in the month, or 0
// when the month has no such occurrence. nth = -1 selects the last one.
func nthWeekdayDay(year int, month time.Month, weekday time.Weekday, nth int) int {
	last := daysIn(year, month)
	if nth == -1 {
		d := time.Date(year, month, last, 12, 0, 0, 0, time.UTC)
		return last - (int(d.Weekday())-int(weekday)+7)%7
	}
	first := time.Date(year, month, 1, 12, 0, 0, 0, time.UTC)
	day := 1 + (int(weekday)-int(first.Weekday())+7)%7 + (nth-1)*7
	if day > last {
		return 0
	}
	return day
}

// nthWorkdayDay returns the day number of the nth working day, or 0.
func nthWorkdayDay(c *compiled, year int, month time.Month, nth int) int {
	last := daysIn(year, month)
	var days []int
	for d := 1; d <= last; d++ {
		if c.isWorkday(time.Date(year, month, d, 12, 0, 0, 0, c.loc)) {
			days = append(days, d)
		}
	}
	switch {
	case len(days) == 0:
		return 0
	case nth == -1:
		return days[len(days)-1]
	case nth <= len(days):
		return days[nth-1]
	default:
		return 0
	}
}

// occurrence computes index i, reporting ok=false for a skipped index.
func (r *Rule) occurrence(c *compiled, i int64) (Occurrence, bool) {
	mk := func(event time.Time, desc string) Occurrence {
		return Occurrence{Index: i, Event: event.UnixNano(), Due: event.Add(time.Duration(r.OffsetSec) * time.Second).UnixNano(), Description: desc}
	}
	switch r.Kind {
	case KindDates:
		if i < 0 || i >= int64(len(c.dates)) {
			return Occurrence{}, false
		}
		ev := c.at(c.dates[i])
		return mk(ev, fmt.Sprintf("%s %02d:%02d%s", c.dates[i].Format("2006-01-02"), c.hour, c.minute, offsetText(r.OffsetSec))), true
	case KindMonthlyDay:
		m := c.anchor.AddDate(0, int(i), 0)
		last := daysIn(m.Year(), m.Month())
		day := r.Day
		desc := fmt.Sprintf("%d日", r.Day)
		if day > last {
			if r.skipsMissingMonth() {
				return Occurrence{}, false
			}
			day = last
			desc = fmt.Sprintf("%d日(该月无%d日,落到月末)", day, r.Day)
		}
		return mk(c.atDay(m.Year(), m.Month(), day), fmt.Sprintf("%s %s%s", c.atDay(m.Year(), m.Month(), day).Format("2006-01-02"), desc, offsetText(r.OffsetSec))), true
	case KindMonthlyNthWeekday:
		m := c.anchor.AddDate(0, int(i), 0)
		name := weekdayName(time.Weekday(r.Weekday))
		if r.Nth == -1 {
			day := nthWeekdayDay(m.Year(), m.Month(), time.Weekday(r.Weekday), -1)
			ev := c.atDay(m.Year(), m.Month(), day)
			return mk(ev, fmt.Sprintf("%s 最后一个%s%s", ev.Format("2006-01-02"), name, offsetText(r.OffsetSec))), true
		}
		day := nthWeekdayDay(m.Year(), m.Month(), time.Weekday(r.Weekday), r.Nth)
		desc := fmt.Sprintf("第%d个%s", r.Nth, name)
		if day == 0 {
			if r.skipsMissingMonth() {
				return Occurrence{}, false
			}
			day = nthWeekdayDay(m.Year(), m.Month(), time.Weekday(r.Weekday), -1)
			desc = fmt.Sprintf("该月无第%d个%s,落到最后一个%s", r.Nth, name, name)
		}
		ev := c.atDay(m.Year(), m.Month(), day)
		return mk(ev, fmt.Sprintf("%s %s%s", ev.Format("2006-01-02"), desc, offsetText(r.OffsetSec))), true
	case KindMonthlyNthWorkday:
		m := c.anchor.AddDate(0, int(i), 0)
		if r.Nth == -1 {
			day := nthWorkdayDay(c, m.Year(), m.Month(), -1)
			ev := c.atDay(m.Year(), m.Month(), day)
			return mk(ev, fmt.Sprintf("%s 最后一个工作日%s", ev.Format("2006-01-02"), offsetText(r.OffsetSec))), true
		}
		day := nthWorkdayDay(c, m.Year(), m.Month(), r.Nth)
		desc := fmt.Sprintf("第%d个工作日", r.Nth)
		if day == 0 {
			if r.skipsMissingMonth() {
				return Occurrence{}, false
			}
			day = nthWorkdayDay(c, m.Year(), m.Month(), -1)
			desc = fmt.Sprintf("该月无第%d个工作日,落到最后一个工作日", r.Nth)
		}
		ev := c.atDay(m.Year(), m.Month(), day)
		return mk(ev, fmt.Sprintf("%s %s%s", ev.Format("2006-01-02"), desc, offsetText(r.OffsetSec))), true
	}
	return Occurrence{}, false
}

func (c *compiled) at(d time.Time) time.Time {
	return time.Date(d.Year(), d.Month(), d.Day(), c.hour, c.minute, 0, 0, c.loc)
}

func (c *compiled) atDay(y int, m time.Month, d int) time.Time {
	return time.Date(y, m, d, c.hour, c.minute, 0, 0, c.loc)
}

func offsetText(sec int64) string {
	if sec == 0 {
		return ""
	}
	sign, v := "+", sec
	if v < 0 {
		sign, v = "-", -v
	}
	h, m := v/3600, (v%3600)/60
	switch {
	case h == 0:
		return fmt.Sprintf(" %s%d分", sign, m)
	case m == 0:
		return fmt.Sprintf(" %s%d小时", sign, h)
	default:
		return fmt.Sprintf(" %s%d小时%d分", sign, h, m)
	}
}

func weekdayName(w time.Weekday) string {
	return [...]string{"周日", "周一", "周二", "周三", "周四", "周五", "周六"}[int(w)%7]
}

// OccurrenceAt returns occurrence index i explicitly (used by tests).
func (r *Rule) OccurrenceAt(i int64) (Occurrence, bool) {
	c, err := r.compile()
	if err != nil {
		return Occurrence{}, false
	}
	return r.occurrence(c, i)
}

// Next returns the first non-skipped occurrence at or after the rule's
// NextIdx. It does not modify the rule.
func (r *Rule) Next() (Occurrence, error) {
	c, err := r.compile()
	if err != nil {
		return Occurrence{}, err
	}
	start := r.NextIdx
	if start < 0 {
		start = 0
	}
	limit := start + maxScanMonths
	for i := start; ; i++ {
		if r.Kind != KindDates && i > limit {
			return Occurrence{}, fmt.Errorf("calendar: rule %q produced no occurrence within %d months", r.ID, maxScanMonths)
		}
		o, ok := r.occurrence(c, i)
		if !ok {
			if r.Kind == KindDates && i >= int64(len(c.dates)) {
				return Occurrence{}, ErrExhausted
			}
			continue
		}
		return o, nil
	}
}

// ErrExhausted reports that a dates rule has no further occurrences.
var ErrExhausted = errors.New("calendar: date list exhausted")

// Upcoming returns the first n occurrences from NextIdx, skipping absent
// months, for tests and the UI.
func (r *Rule) Upcoming(n int) ([]Occurrence, error) {
	c, err := r.compile()
	if err != nil {
		return nil, err
	}
	out := make([]Occurrence, 0, n)
	i := r.NextIdx
	if i < 0 {
		i = 0
	}
	limit := i + maxScanMonths
	for len(out) < n {
		if r.Kind != KindDates && i > limit {
			break
		}
		o, ok := r.occurrence(c, i)
		i++
		if !ok {
			if r.Kind == KindDates && i > int64(len(c.dates)) {
				break
			}
			continue
		}
		out = append(out, o)
	}
	return out, nil
}

// Validate checks a rule without materializing anything.
func (r *Rule) Validate() error {
	_, err := r.compile()
	return err
}

// AnchorMonth returns the rule's month anchor as "2006-01".
func (r *Rule) AnchorMonth() string {
	c, err := r.compile()
	if err != nil {
		return ""
	}
	return c.anchor.Format("2006-01")
}

// Marshal serializes the rule.
func (r *Rule) Marshal() []byte {
	b, _ := json.Marshal(r)
	return b
}

// UnmarshalRule parses and validates a rule.
func UnmarshalRule(b []byte) (Rule, error) {
	var r Rule
	if err := json.Unmarshal(b, &r); err != nil {
		return r, err
	}
	return r, r.Validate()
}
