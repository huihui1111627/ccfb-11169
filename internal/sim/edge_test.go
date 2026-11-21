package sim_test

import (
	"testing"
	"time"

	"kitewheel/internal/engine"
	"kitewheel/internal/sim"
)

// TestBoundaryExactlyAtHorizon pins the classic off-by-one: the wheel holds
// dues up to exactly one coarsest horizon minus one nanosecond, delivers the
// last one at its due instant, and refuses anything later rather than silently
// truncating or misplacing it.
func TestBoundaryExactlyAtHorizon(t *testing.T) {
	h := int64(744 * time.Hour)
	for _, off := range []int64{h - 2, h - 1, h, h + 1} {
		e, err := engine.New(engine.Config{Now: base})
		if err != nil {
			t.Fatal(err)
		}
		_, eerr := e.Enqueue("edge", base+off, 0, "")
		if off > h-1 && eerr == nil {
			t.Fatalf("offset %s: expected refusal beyond the horizon", time.Duration(off))
		}
		if off <= h-1 && eerr != nil {
			t.Fatalf("offset %s: %v", time.Duration(off), eerr)
		}
		if off <= h-1 {
			if err := e.AdvanceTo(base + off + 1); err != nil {
				t.Fatal(err)
			}
			hist := e.History(1)
			if len(hist) != 1 || hist[0].At != base+off {
				t.Fatalf("offset %s: %+v", time.Duration(off), hist)
			}
		}
	}
}

// TestEveryLevelBoundary walks dues at exact level boundaries and compares with
// the reference. A boundary time belongs to the finer level, so it must be
// delivered exactly at its due time, neither a slot early nor late.
func TestEveryLevelBoundary(t *testing.T) {
	e, err := engine.New(engine.Config{Now: base})
	if err != nil {
		t.Fatal(err)
	}
	type edge struct {
		off  time.Duration
		name string
	}
	var edges []edge
	for _, d := range []time.Duration{
		time.Second, time.Minute, time.Hour, 24 * time.Hour,
		60 * time.Second, 60 * time.Minute, 24 * time.Hour,
		30 * 24 * time.Hour, 60 * 60 * time.Second,
	} {
		edges = append(edges, edge{d, d.String()})
		edges = append(edges, edge{d - time.Nanosecond, d.String() + "-1ns"})
		edges = append(edges, edge{d + time.Nanosecond, d.String() + "+1ns"})
	}
	want := map[string]int64{}
	for i, ed := range edges {
		id := "e" + string(rune('a'+i))
		if _, err := e.Enqueue(id, base+int64(ed.off), 0, ""); err != nil {
			t.Fatalf("%s: %v", ed.name, err)
		}
		want[id] = base + int64(ed.off)
	}
	// Advance to just before the end, then exactly to it.
	if err := e.AdvanceTo(base + int64(31*24*time.Hour)); err != nil {
		t.Fatal(err)
	}
	got := e.History(0)
	for _, d := range got {
		if want[d.ID] != d.At {
			t.Fatalf("%s delivered at %s, want %s", d.ID, time.Duration(d.At-base), time.Duration(want[d.ID]-base))
		}
	}
	if len(got) != len(want) {
		t.Fatalf("delivered %d of %d", len(got), len(want))
	}
}

// TestCrossMonthAndLeapCalendar walks a rule over a year boundary and a leap
// February through the scheduler and checks each occurrence's delivery instant.
func TestCrossMonthAndLeapCalendar(t *testing.T) {
	sim.Run(t, sim.Spec{
		Seed: 301, Start: time.Date(2027, 12, 1, 0, 0, 0, 0, time.UTC).UnixNano(),
		Horizon: int64(90 * 24 * time.Hour), Ops: 2000, Rules: 4, Bursts: 2, MaxPrior: 2,
	})
}
