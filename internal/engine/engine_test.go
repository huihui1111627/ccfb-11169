package engine_test

import (
	"testing"
	"time"

	"kitewheel/internal/engine"
	"kitewheel/internal/model"
)

func newMem(t *testing.T, now time.Time) *engine.Engine {
	t.Helper()
	e, err := engine.New(engine.Config{Now: now.UnixNano()})
	if err != nil {
		t.Fatal(err)
	}
	return e
}

func TestOrderingSameInstant(t *testing.T) {
	base := time.Date(2026, 9, 18, 8, 0, 0, 0, time.UTC)
	e := newMem(t, base)
	due := base.Add(90 * time.Second).UnixNano()
	for i, tc := range []struct {
		prio int
	}{{1}, {5}, {1}, {9}, {5}} {
		if _, err := e.Enqueue("m"+string(rune('a'+i)), due, tc.prio, ""); err != nil {
			t.Fatal(err)
		}
	}
	if err := e.AdvanceTo(base.Add(2 * time.Minute).UnixNano()); err != nil {
		t.Fatal(err)
	}
	got := e.History(-1)
	// newest first; reverse
	want := []string{"md", "mb", "me", "ma", "mc"}
	if len(got) != len(want) {
		t.Fatalf("got %d deliveries", len(got))
	}
	for i := range want {
		g := got[len(got)-1-i]
		if g.ID != want[i] {
			t.Fatalf("position %d: got %s want %s", i, g.ID, want[i])
		}
		if g.At != due {
			t.Fatalf("%s delivered at %d, want %d", g.ID, g.At, due)
		}
	}
}

func TestHierarchyCascade(t *testing.T) {
	base := time.Date(2026, 9, 18, 8, 0, 0, 0, time.UTC)
	e := newMem(t, base)
	dues := []time.Duration{
		0, time.Second, 3 * time.Second, 45 * time.Second,
		2*time.Minute + 500*time.Millisecond, 26 * time.Hour,
		29 * 24 * time.Hour, 30*24*time.Hour + 3*time.Second,
	}
	for i, d := range dues {
		if _, err := e.Enqueue("m"+string(rune('0'+i)), base.Add(d).UnixNano(), 0, ""); err != nil {
			t.Fatalf("due %s: %v", d, err)
		}
	}
	if err := e.AdvanceTo(base.Add(31 * 24 * time.Hour).UnixNano()); err != nil {
		t.Fatal(err)
	}
	h := e.History(-1)
	if len(h) != len(dues) {
		t.Fatalf("delivered %d of %d", len(h), len(dues))
	}
	for i := 1; i < len(h); i++ {
		// history is newest first
		if h[i-1].At < h[i].At {
			t.Fatalf("out of order at %d: %d then %d", i, h[i-1].At, h[i].At)
		}
	}
	for i, d := range dues {
		id := "m" + string(rune('0'+i))
		got := h[len(h)-1-i]
		if got.ID != id || got.At != base.Add(d).UnixNano() {
			t.Fatalf("%s delivered %s at %s", id, got.ID, time.Duration(got.At-base.UnixNano()))
		}
	}
}

func TestPastDue(t *testing.T) {
	base := time.Date(2026, 9, 18, 8, 0, 0, 0, time.UTC)
	e := newMem(t, base.Add(time.Hour))
	m, err := e.Enqueue("late", base.UnixNano(), 0, "")
	if err != nil {
		t.Fatal(err)
	}
	if m.ID != "late" {
		t.Fatal("not returned")
	}
	st := e.Stats()
	if st.Delivered != 1 || st.PastDue != 1 {
		t.Fatalf("stats %+v", st)
	}
	h := e.History(1)
	if !h[0].PastDue || h[0].At != base.Add(time.Hour).UnixNano() {
		t.Fatalf("delivery %+v", h[0])
	}
	// Exactly now is delivered but not past due.
	if _, err := e.Enqueue("now", base.Add(time.Hour).UnixNano(), 0, ""); err != nil {
		t.Fatal(err)
	}
	if st := e.Stats(); st.PastDue != 1 || st.Delivered != 2 {
		t.Fatalf("stats %+v", st)
	}
}

func TestCancelAndReschedule(t *testing.T) {
	base := time.Date(2026, 9, 18, 8, 0, 0, 0, time.UTC)
	e := newMem(t, base)
	due := base.Add(10 * time.Hour)
	if _, err := e.Enqueue("a", due.UnixNano(), 0, ""); err != nil {
		t.Fatal(err)
	}
	if _, err := e.Enqueue("b", due.UnixNano(), 0, ""); err != nil {
		t.Fatal(err)
	}
	if _, err := e.Enqueue("c", due.UnixNano(), 0, ""); err != nil {
		t.Fatal(err)
	}
	if err := e.Cancel("b"); err != nil {
		t.Fatal(err)
	}
	newDue := base.Add(25*24*time.Hour + 123*time.Millisecond)
	if _, err := e.Reschedule("c", newDue.UnixNano(), 0); err != nil {
		t.Fatal(err)
	}
	if err := e.AdvanceTo(base.Add(60 * 24 * time.Hour).UnixNano()); err != nil {
		t.Fatal(err)
	}
	h := e.History(-1)
	if len(h) != 2 {
		t.Fatalf("delivered %d", len(h))
	}
	if h[len(h)-1].ID != "a" || h[0].ID != "c" {
		t.Fatalf("order: %s then %s", h[len(h)-1].ID, h[0].ID)
	}
	if h[0].At != newDue.UnixNano() {
		t.Fatalf("c at %d want %d", h[0].At, newDue.UnixNano())
	}
	if st := e.Stats(); st.Cancelled != 1 {
		t.Fatalf("stats %+v", st)
	}
}

func TestIdleJumpCost(t *testing.T) {
	base := time.Date(2026, 9, 18, 8, 0, 0, 0, time.UTC)
	e := newMem(t, base)
	if _, err := e.Enqueue("far", base.Add(30*24*time.Hour).UnixNano(), 0, ""); err != nil {
		t.Fatal(err)
	}
	before := e.Stats().Cascades
	if err := e.AdvanceTo(base.Add(31 * 24 * time.Hour).UnixNano()); err != nil {
		t.Fatal(err)
	}
	// A 31-day jump with one message must cost a handful of cascades (days ->
	// hours -> minutes -> seconds), never one per empty slot.
	if got := e.Stats().Cascades - before; got > 10 {
		t.Fatalf("cascades = %d, expected <= 10", got)
	}
	if e.Stats().Delivered != 1 {
		t.Fatal("not delivered")
	}
	if class := e.History(1)[0].Class; class != model.ClassRealtime {
		t.Fatalf("class %v", class)
	}
}
