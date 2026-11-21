// Command kitewheel runs the festival delayed-message service: a hierarchical
// timing wheel with a persisted record log and a virtual-clock control page.
//
//	kitewheel -addr :8080 -data ./data -now 2026-09-18T08:00:00Z
//	kitewheel -demo -addr :8080          # seed a festival-sized dataset
package main

import (
	"flag"
	"fmt"
	"log"
	"math/rand"
	"time"

	"kitewheel/internal/calendar"
	"kitewheel/internal/engine"
	"kitewheel/internal/server"
	"kitewheel/internal/wheel"
)

func main() {
	var (
		addr      = flag.String("addr", "127.0.0.1:8080", "HTTP listen address")
		data      = flag.String("data", "data", "store directory (empty for memory-only)")
		nowText   = flag.String("now", "", "virtual clock start, RFC3339 (default: now)")
		levels    = flag.String("levels", "60s,60m,24h,31d", "level layout, finest first: <slots><unit>")
		demo      = flag.Bool("demo", false, "seed demo messages and calendar rules")
		demoN     = flag.Int("demo-n", 300000, "number of demo messages")
		snapEvery = flag.Int64("snapshot-every", 20000, "records between snapshots (0 disables)")
	)
	flag.Parse()

	specs, err := parseLevels(*levels)
	if err != nil {
		log.Fatalf("levels: %v", err)
	}
	start := time.Now().UTC().Truncate(time.Second)
	if *nowText != "" {
		start, err = time.Parse(time.RFC3339, *nowText)
		if err != nil {
			log.Fatalf("now: %v", err)
		}
	}
	cfg := engine.Config{
		Dir:           *data,
		Levels:        specs,
		Now:           start.UnixNano(),
		SnapshotEvery: *snapEvery,
		History:       5000,
	}
	if *demo {
		cfg.Dir = "" // demo runs in memory so it always starts from a clean board
	}
	eng, err := engine.New(cfg)
	if err != nil {
		log.Fatalf("engine: %v", err)
	}
	defer eng.Close()

	if *demo {
		seed(eng, start, *demoN)
	}
	srv, err := server.New(eng, start)
	if err != nil {
		log.Fatalf("server: %v", err)
	}
	log.Printf("levels: %s", describe(specs))
	log.Printf("virtual clock starts at %s", start.Format(time.RFC3339))
	log.Fatal(srv.Serve(*addr))
}

// parseLevels parses a layout such as "60s,60m,24h,31d". Each level must be
// exactly one revolution of the level below.
func parseLevels(s string) ([]wheel.LevelSpec, error) {
	var out []wheel.LevelSpec
	names := []string{"秒", "分", "时", "天", "层"}
	fields := split(s, ',')
	for i, f := range fields {
		var n int
		var unit string
		if _, err := fmt.Sscanf(f, "%d%s", &n, &unit); err != nil || n <= 0 {
			return nil, fmt.Errorf("bad level %q", f)
		}
		var d time.Duration
		switch unit {
		case "s", "sec":
			d = time.Second
		case "m", "min":
			d = time.Minute
		case "h", "hour":
			d = time.Hour
		case "d", "day":
			d = 24 * time.Hour
		default:
			return nil, fmt.Errorf("bad unit %q in %q", unit, f)
		}
		name := "层" + fmt.Sprint(i)
		if i < len(names) {
			name = names[i]
		}
		out = append(out, wheel.LevelSpec{Name: name, Len: n, Span: d})
	}
	return out, nil
}

func split(s string, sep byte) []string {
	var out []string
	cur := ""
	for i := 0; i < len(s); i++ {
		if s[i] == sep {
			out = append(out, cur)
			cur = ""
			continue
		}
		cur += string(s[i])
	}
	return append(out, cur)
}

func describe(specs []wheel.LevelSpec) string {
	out := ""
	for i, s := range specs {
		if i > 0 {
			out += " → "
		}
		out += fmt.Sprintf("%s %d×%s", s.Name, s.Len, s.Span)
	}
	return out
}

// seed fills the scheduler with a festival-sized, deliberately skewed batch:
// a huge cluster on the first night, refund deadlines through the week,
// session reminders for the whole festival, plus calendar rules.
func seed(eng *engine.Engine, start time.Time, n int) {
	log.Printf("seeding %d demo messages", n)
	rng := rand.New(rand.NewSource(20261121))
	base := start
	// Opening night: tens of thousands of gate notices at one instant.
	openNight := base.Add(26 * time.Hour)
	for i := 0; i < n; i++ {
		var due time.Time
		switch r := rng.Intn(100); {
		case r < 55:
			due = openNight.Add(time.Duration(rng.Intn(2000)) * time.Nanosecond)
		case r < 72:
			due = base.Add(time.Duration(rng.Int63n(int64(7 * 24 * time.Hour))))
		case r < 90:
			due = base.Add(time.Duration(rng.Int63n(int64(30 * 24 * time.Hour))))
		default:
			due = base.Add(time.Duration(rng.Int63n(int64(2 * time.Hour))))
		}
		id := fmt.Sprintf("ticket-%07d", i)
		if _, err := eng.Enqueue(id, due.UnixNano(), rng.Intn(4), "入场提醒"); err != nil {
			log.Fatalf("seed %s: %v", id, err)
		}
		if i%50000 == 0 && i > 0 {
			log.Printf("  %d seeded", i)
		}
	}
	loc := 8 * 3600
	// The festival opens at 09:00 local time each day. Sessions are listed as
	// explicit dates, each with its own start hour, so "half an hour before
	// the session" is a per-date event rather than one fixed time.
	sessions := []struct {
		day  int
		hour int
	}{
		{1, 9}, {1, 15}, {2, 9}, {2, 15}, {3, 9}, {3, 15}, {4, 9},
	}
	opening := calendar.Rule{
		ID: "opening", Name: "放飞场次开场前半小时", Kind: calendar.KindDates,
		TZOffsetSec: loc, OffsetSec: -30 * 60,
		Payload: "放飞场次提醒", Priority: 3,
	}
	// A dates rule shares one time of day, so the afternoon sessions get their
	// own rule; the point is that both are ordinary calendar rules.
	var morning, afternoon []string
	for _, s := range sessions {
		day := start.AddDate(0, 0, s.day).Format("2006-01-02")
		if s.hour < 12 {
			morning = append(morning, day)
		} else {
			afternoon = append(afternoon, day)
		}
	}
	opening.Dates = morning
	opening.TimeOfDay = "09:00"
	evening := calendar.Rule{
		ID: "evening", Name: "下午场开场前半小时", Kind: calendar.KindDates,
		TZOffsetSec: loc, TimeOfDay: "15:00", OffsetSec: -30 * 60,
		Dates: afternoon, Payload: "下午场提醒", Priority: 2,
	}
	rules := []calendar.Rule{
		opening,
		evening,
		{
			ID: "month-end", Name: "每月最后一个周六", Kind: calendar.KindMonthlyNthWeekday,
			TZOffsetSec: loc, TimeOfDay: "16:00", Nth: -1, Weekday: int(time.Saturday),
			OffsetSec: -1800, Payload: "月末放飞活动", Priority: 2, Missing: calendar.MissingSkip,
		},
		{
			ID: "day31", Name: "每月31日退票截止", Kind: calendar.KindMonthlyDay,
			TZOffsetSec: loc, TimeOfDay: "23:00", Day: 31, Missing: calendar.MissingClamp,
			Payload: "退票截止", Priority: 1,
		},
		{
			ID: "payroll", Name: "每月第三个工作日", Kind: calendar.KindMonthlyNthWorkday,
			TZOffsetSec: loc, TimeOfDay: "10:00", Nth: 3, Missing: calendar.MissingSkip,
			Payload: "结算", Priority: 1,
		},
	}
	for _, r := range rules {
		if _, err := eng.AddRule(r); err != nil {
			log.Fatalf("seed rule %s: %v", r.ID, err)
		}
	}
	log.Printf("seeded: %d pending", eng.Pending())
}
