// Package recur parses and evaluates calendar recurrence rules such as
// "30 minutes before gates open on every kite-flying day". Rules are
// validated at add time; a malformed rule is rejected immediately.
package recur

import (
	"fmt"
	"time"
)

// Rule describes a recurring calendar schedule. Exactly one day selector
// must be set: Daily, Weekdays or Dates.
//
//	{"daily": true, "time": "09:00", "offset": "-30m"}
//	{"weekdays": [6,0], "time": "08:30"}                 // Sat & Sun
//	{"dates": ["2026-04-19","2026-04-20"], "time": "09:00", "offset": "-30m"}
type Rule struct {
	Daily    bool     `json:"daily,omitempty"`
	Weekdays []int    `json:"weekdays,omitempty"` // 0 = Sunday
	Dates    []string `json:"dates,omitempty"`    // YYYY-MM-DD
	Time     string   `json:"time"`               // "HH:MM" local time
	Offset   string   `json:"offset,omitempty"`   // Go duration, e.g. "-30m" = 30 min before
	Until    string   `json:"until,omitempty"`    // last eligible YYYY-MM-DD (inclusive)
}

// Compiled is a validated rule ready to compute occurrences.
type Compiled struct {
	Raw      *Rule
	Loc      *time.Location
	hour     int
	minute   int
	offset   time.Duration
	dates    map[string]bool
	weekdays map[int]bool
	until    time.Time
	hasUntil bool
}

// Compile validates a rule. Any mistake (bad time, bad offset, unknown or
// missing day selector, malformed date, until in the past is NOT an error
// here — that is checked by the caller against its clock) is reported.
func Compile(r *Rule, loc *time.Location) (*Compiled, error) {
	if r == nil {
		return nil, fmt.Errorf("规则为空")
	}
	if loc == nil {
		loc = time.Local
	}
	c := &Compiled{Raw: r, Loc: loc}

	t, err := time.Parse("15:04", r.Time)
	if err != nil {
		return nil, fmt.Errorf("time 格式应为 HH:MM（如 09:30）: %v", err)
	}
	c.hour, c.minute = t.Hour(), t.Minute()

	if r.Offset != "" {
		d, err := time.ParseDuration(r.Offset)
		if err != nil {
			return nil, fmt.Errorf("offset 应为时长（如 -30m、2h）: %v", err)
		}
		c.offset = d
	}

	selectors := 0
	if r.Daily {
		selectors++
	}
	if len(r.Weekdays) > 0 {
		selectors++
		c.weekdays = map[int]bool{}
		for _, wd := range r.Weekdays {
			if wd < 0 || wd > 6 {
				return nil, fmt.Errorf("weekdays 取值 0-6（0=周日），收到 %d", wd)
			}
			c.weekdays[wd] = true
		}
	}
	if len(r.Dates) > 0 {
		selectors++
		c.dates = map[string]bool{}
		for _, ds := range r.Dates {
			if _, err := time.ParseInLocation("2006-01-02", ds, loc); err != nil {
				return nil, fmt.Errorf("dates 日期格式应为 YYYY-MM-DD: %q", ds)
			}
			c.dates[ds] = true
		}
	}
	if selectors == 0 {
		return nil, fmt.Errorf("缺少日期选择器：daily / weekdays / dates 三选一")
	}
	if selectors > 1 {
		return nil, fmt.Errorf("daily / weekdays / dates 只能选一个")
	}

	if r.Until != "" {
		u, err := time.ParseInLocation("2006-01-02", r.Until, loc)
		if err != nil {
			return nil, fmt.Errorf("until 日期格式应为 YYYY-MM-DD: %q", r.Until)
		}
		c.until = u
		c.hasUntil = true
	}
	return c, nil
}

// matches reports whether the calendar day d (in the rule's location) is
// selected by the rule.
func (c *Compiled) matches(d time.Time) bool {
	if c.hasUntil && d.After(c.until) {
		return false
	}
	switch {
	case c.Raw.Daily:
		return true
	case c.weekdays != nil:
		return c.weekdays[int(d.Weekday())]
	default:
		return c.dates[d.Format("2006-01-02")]
	}
}

// NextAfter returns the first occurrence strictly after ms. ok is false if
// the rule produces no further occurrence within a 2-year search horizon.
func (c *Compiled) NextAfter(ms int64) (next int64, ok bool) {
	t := time.UnixMilli(ms).In(c.Loc)
	day := time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, c.Loc)
	limit := day.AddDate(2, 0, 1)
	for d := day; !d.After(limit); d = d.AddDate(0, 0, 1) {
		if !c.matches(d) {
			continue
		}
		cand := time.Date(d.Year(), d.Month(), d.Day(), c.hour, c.minute, 0, 0, c.Loc).
			Add(c.offset)
		if cand.UnixMilli() > ms {
			return cand.UnixMilli(), true
		}
	}
	return 0, false
}

// Describe renders the rule in short human-readable form.
func (c *Compiled) Describe() string {
	r := c.Raw
	what := ""
	switch {
	case r.Daily:
		what = "每天"
	case len(r.Weekdays) > 0:
		names := []string{"日", "一", "二", "三", "四", "五", "六"}
		what = "每周"
		for i, wd := range r.Weekdays {
			if i > 0 {
				what += "、"
			}
			what += names[wd]
		}
	default:
		what = fmt.Sprintf("%d 个指定日期", len(r.Dates))
	}
	s := fmt.Sprintf("%s %s", what, r.Time)
	if r.Offset != "" && r.Offset != "0s" {
		s += fmt.Sprintf("（偏移 %s）", r.Offset)
	}
	if r.Until != "" {
		s += "，截至 " + r.Until
	}
	return s
}
