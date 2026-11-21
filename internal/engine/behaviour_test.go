package engine_test

import (
	"path/filepath"
	"testing"
	"time"

	"kitewheel/internal/calendar"
	"kitewheel/internal/engine"
)

func TestRescheduleAcrossLevels(t *testing.T) {
	base := time.Date(2026, 9, 18, 8, 0, 0, 0, time.UTC)
	e := newMem(t, base)

	// Start in the days level, move to the seconds level, then back out to
	// days again. Each move must leave exactly one live instance.
	if _, err := e.Enqueue("m", base.Add(10*24*time.Hour).UnixNano(), 0, ""); err != nil {
		t.Fatal(err)
	}
	if _, err := e.Reschedule("m", base.Add(5*time.Second).UnixNano(), 0); err != nil {
		t.Fatal(err)
	}
	if got := e.Pending(); got != 1 {
		t.Fatalf("after first move: %d pending", got)
	}
	if _, err := e.Reschedule("m", base.Add(20*24*time.Hour).UnixNano(), 0); err != nil {
		t.Fatal(err)
	}
	if got := e.Pending(); got != 1 {
		t.Fatalf("after second move: %d pending", got)
	}
	// Advancing past the original second-level due must not deliver anything.
	if err := e.AdvanceTo(base.Add(time.Minute).UnixNano()); err != nil {
		t.Fatal(err)
	}
	if d := e.Stats().Delivered; d != 0 {
		t.Fatalf("delivered %d early", d)
	}
	if err := e.AdvanceTo(base.Add(21 * 24 * time.Hour).UnixNano()); err != nil {
		t.Fatal(err)
	}
	h := e.History(1)
	if len(h) != 1 || h[0].At != base.Add(20*24*time.Hour).UnixNano() {
		t.Fatalf("history %+v", h)
	}
	if s := e.Stats(); s.Rescheduled != 2 || s.Delivered != 1 {
		t.Fatalf("stats %+v", s)
	}
}

func TestRescheduleKeepsSequenceOrdering(t *testing.T) {
	base := time.Date(2026, 9, 18, 8, 0, 0, 0, time.UTC)
	e := newMem(t, base)
	target := base.Add(10 * time.Second).UnixNano()
	if _, err := e.Enqueue("a", target, 0, ""); err != nil {
		t.Fatal(err)
	}
	if _, err := e.Enqueue("b", base.Add(time.Hour).UnixNano(), 0, ""); err != nil {
		t.Fatal(err)
	}
	if _, err := e.Enqueue("c", target, 0, ""); err != nil {
		t.Fatal(err)
	}
	// Moving b onto the instant makes it a fresh insertion there, so it orders
	// after a and c, which were already scheduled.
	if _, err := e.Reschedule("b", target, 0); err != nil {
		t.Fatal(err)
	}
	if err := e.AdvanceTo(base.Add(11 * time.Second).UnixNano()); err != nil {
		t.Fatal(err)
	}
	got := e.History(0)
	// History is newest first. b is rescheduled, so it is a fresh insertion at
	// the destination instant and is delivered after a and c.
	want := []string{"b", "c", "a"}
	if len(got) != 3 {
		t.Fatalf("delivered %d", len(got))
	}
	for i, id := range want {
		if got[i].ID != id {
			t.Fatalf("order %v, want %v", []string{got[0].ID, got[1].ID, got[2].ID}, want)
		}
	}
}

func TestCancelThenRescheduleIsRejected(t *testing.T) {
	base := time.Date(2026, 9, 18, 8, 0, 0, 0, time.UTC)
	e := newMem(t, base)
	if _, err := e.Enqueue("x", base.Add(time.Hour).UnixNano(), 0, ""); err != nil {
		t.Fatal(err)
	}
	if err := e.Cancel("x"); err != nil {
		t.Fatal(err)
	}
	if err := e.Cancel("x"); err == nil {
		t.Fatal("double cancel must fail")
	}
	if _, err := e.Reschedule("x", base.Add(2*time.Hour).UnixNano(), 0); err == nil {
		t.Fatal("rescheduling a cancelled message must fail")
	}
	// The ID is free again.
	if _, err := e.Enqueue("x", base.Add(3*time.Hour).UnixNano(), 0, ""); err != nil {
		t.Fatalf("reusing the id: %v", err)
	}
	if err := e.AdvanceTo(base.Add(4 * time.Hour).UnixNano()); err != nil {
		t.Fatal(err)
	}
	if s := e.Stats(); s.Delivered != 1 || s.Cancelled != 1 {
		t.Fatalf("stats %+v", s)
	}
}

func TestCalendarAndRealtimeAtSameInstant(t *testing.T) {
	base := time.Date(2026, 9, 18, 8, 0, 0, 0, time.UTC)
	e := newMem(t, base)
	r := calendar.Rule{
		ID: "r", Kind: calendar.KindDates, TimeOfDay: "09:00",
		Dates:   []string{"2026-09-19"},
		Created: base.UnixNano(), Priority: 0, Payload: "session",
	}
	if _, err := e.AddRule(r); err != nil {
		t.Fatal(err)
	}
	same := time.Date(2026, 9, 19, 9, 0, 0, 0, time.FixedZone("cst", 8*3600)).UnixNano()
	// A real-time message inserted after the rule, same instant and priority.
	if _, err := e.Enqueue("t", same, 0, ""); err != nil {
		t.Fatal(err)
	}
	if err := e.AdvanceTo(same + 1); err != nil {
		t.Fatal(err)
	}
	h := e.History(0)
	if len(h) != 2 {
		t.Fatalf("delivered %d", len(h))
	}
	// Real time precedes calendar at the same due and priority; history is
	// newest first, so the calendar occurrence comes before it in the slice.
	if h[0].ID != "r#0" || h[1].ID != "t" {
		t.Fatalf("order %s then %s", h[0].ID, h[1].ID)
	}
}

func TestPastDueBurstIsCountedSeparately(t *testing.T) {
	base := time.Date(2026, 9, 18, 8, 0, 0, 0, time.UTC)
	e := newMem(t, base)
	// Simulate the service coming back after an outage: a batch of reminders
	// whose due times all passed while it was down.
	// i starts at 1: a due exactly at the current instant is on time, not late.
	for i := 1; i <= 50; i++ {
		due := base.Add(time.Duration(-i) * time.Second).UnixNano()
		if _, err := e.Enqueue("late"+string(rune('A'+i)), due, 0, ""); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := e.Enqueue("future", base.Add(time.Hour).UnixNano(), 0, ""); err != nil {
		t.Fatal(err)
	}
	s := e.Stats()
	if s.Delivered != 50 || s.PastDue != 50 {
		t.Fatalf("stats %+v", s)
	}
	if e.Pending() != 1 {
		t.Fatalf("pending %d", e.Pending())
	}
}

func TestDurableRestartPreservesOrdering(t *testing.T) {
	base := time.Date(2026, 9, 18, 8, 0, 0, 0, time.UTC)
	dir := t.TempDir()
	open := func() *engine.Engine {
		e, err := engine.New(engine.Config{Dir: filepath.Join(dir, "store"), Now: base.UnixNano(), History: 1024})
		if err != nil {
			t.Fatal(err)
		}
		return e
	}
	e := open()
	due := base.Add(48 * time.Hour).UnixNano()
	for i, prio := range []int{1, 5, 1, 9, 5, 5, 2} {
		if _, err := e.Enqueue("m"+string(rune('a'+i)), due, prio, ""); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := e.Enqueue("far", base.Add(20*24*time.Hour).UnixNano(), 0, ""); err != nil {
		t.Fatal(err)
	}
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}
	e = open()
	if got := e.Pending(); got != 8 {
		t.Fatalf("pending after reopen %d", got)
	}
	if err := e.AdvanceTo(base.Add(21 * 24 * time.Hour).UnixNano()); err != nil {
		t.Fatal(err)
	}
	if err := e.AdvanceTo(base.Add(49 * 24 * time.Hour).UnixNano()); err != nil {
		t.Fatal(err)
	}
	got := e.History(0)
	if len(got) != 8 {
		t.Fatalf("delivered %d", len(got))
	}
	// The 48-hour batch comes first, highest priority first and equal
	// priorities in insertion order; the 20-day message comes last.
	order := make([]string, len(got))
	for i := range got {
		order[len(got)-1-i] = got[i].ID
	}
	wantOldest := []string{"md", "mb", "me", "mf", "mg", "ma", "mc", "far"}
	for i := range wantOldest {
		if order[i] != wantOldest[i] {
			t.Fatalf("delivery order %v, want %v", order, wantOldest)
		}
	}
}
