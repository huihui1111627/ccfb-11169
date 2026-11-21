package sim_test

import (
	"testing"
	"time"

	"kitewheel/internal/engine"
)

// TestExactBoundariesAcrossLevels checks that a message due exactly on every
// level boundary is delivered at that instant, not one slot early or late, and
// that the cascade happens exactly at the boundary.
func TestExactBoundariesAcrossLevels(t *testing.T) {
	// The wheel epoch is zero, so align the start to a whole minute to make
	// every level boundary land on a whole second, minute, hour and day.
	base := time.Date(2026, 9, 18, 0, 0, 0, 0, time.UTC)
	e, err := engine.New(engine.Config{Now: base.UnixNano(), History: 1 << 16})
	if err != nil {
		t.Fatal(err)
	}
	type edge struct {
		id  string
		off time.Duration
	}
	edges := []edge{
		{"sec+1", time.Second},
		{"min", time.Minute},
		{"min+1ns", time.Minute + time.Nanosecond},
		{"hour", time.Hour},
		{"hour-1ns", time.Hour - time.Nanosecond},
		{"day", 24 * time.Hour},
		{"day-1ns", 24*time.Hour - time.Nanosecond},
		{"week", 7 * 24 * time.Hour},
		{"month", 30 * 24 * time.Hour},
		{"h-24", 24*time.Hour + 59*time.Minute + 59*time.Second},
	}
	for _, ed := range edges {
		if _, err := e.Enqueue(ed.id, base.Add(ed.off).UnixNano(), 0, ""); err != nil {
			t.Fatalf("%s: %v", ed.id, err)
		}
	}
	// Step the clock in one-second increments across the first hour, then jump.
	for at := base.Add(time.Second); !at.After(base.Add(time.Hour)); at = at.Add(time.Second) {
		if err := e.AdvanceTo(at.UnixNano()); err != nil {
			t.Fatal(err)
		}
	}
	if err := e.AdvanceTo(base.Add(31 * 24 * time.Hour).UnixNano()); err != nil {
		t.Fatal(err)
	}
	got := e.History(0)
	byID := map[string]engine.Delivery{}
	for _, d := range got {
		byID[d.ID] = d
	}
	for _, ed := range edges {
		d, ok := byID[ed.id]
		if !ok {
			t.Fatalf("%s not delivered", ed.id)
		}
		want := base.Add(ed.off).UnixNano()
		if d.At != want {
			t.Fatalf("%s delivered at %s, want %s", ed.id, time.Duration(d.At-base.UnixNano()), ed.off)
		}
	}
}

// TestIdleMonthsAreCheap checks that advancing across many months with only a
// distant message costs a handful of cascades rather than one per empty slot.
func TestIdleMonthsAreCheap(t *testing.T) {
	base := time.Date(2026, 9, 18, 0, 0, 0, 0, time.UTC)
	e, err := engine.New(engine.Config{Now: base.UnixNano()})
	if err != nil {
		t.Fatal(err)
	}
	due := base.Add(20 * 24 * time.Hour)
	if _, err := e.Enqueue("far", due.UnixNano(), 0, ""); err != nil {
		t.Fatal(err)
	}
	before := e.Stats().Cascades
	if err := e.AdvanceTo(due.UnixNano()); err != nil {
		t.Fatal(err)
	}
	if got := e.Stats().Cascades - before; got > 4 {
		t.Fatalf("a 20-day idle jump caused %d cascades, expected one per level", got)
	}
	if e.Stats().Delivered != 1 {
		t.Fatal("message not delivered")
	}
	// The jump ends exactly at the message's due time, which is itself an
	// event, so it is counted as a cascade rather than an idle tail.
	if got := e.Stats().IdleJumps; got != 0 {
		t.Fatalf("idle jumps %d, expected the advance to land exactly on the only event", got)
	}
}

// TestMultiDayJumpCascadesEachLevelInOrder verifies that messages spread over
// days, hours, minutes and seconds, jumped over in one step, all come out at
// their own due times in chronological order.
func TestMultiDayJumpCascadesEachLevelInOrder(t *testing.T) {
	base := time.Date(2026, 9, 18, 0, 0, 0, 0, time.UTC)
	e, err := engine.New(engine.Config{Now: base.UnixNano(), History: 1 << 16})
	if err != nil {
		t.Fatal(err)
	}
	dues := []time.Duration{
		30 * time.Second,
		5*time.Minute + 3*time.Second,
		3*time.Hour + 7*time.Minute,
		2*24*time.Hour + 11*time.Hour,
		19 * 24 * time.Hour,
		29*24*time.Hour + 59*time.Minute,
	}
	for i, d := range dues {
		if _, err := e.Enqueue(idOf(i), base.Add(d).UnixNano(), 0, ""); err != nil {
			t.Fatalf("%s: %v", d, err)
		}
	}
	// One jump across the whole span.
	if err := e.AdvanceTo(base.Add(31 * 24 * time.Hour).UnixNano()); err != nil {
		t.Fatal(err)
	}
	got := e.History(0)
	if len(got) != len(dues) {
		t.Fatalf("delivered %d of %d", len(got), len(dues))
	}
	for i := len(got) - 1; i >= 0; i-- {
		want := base.Add(dues[len(dues)-1-i]).UnixNano()
		if got[i].At != want {
			t.Fatalf("position %d: %s delivered at %s, want %s", i,
				got[i].ID, time.Duration(got[i].At-base.UnixNano()), dues[len(dues)-1-i])
		}
	}
}
