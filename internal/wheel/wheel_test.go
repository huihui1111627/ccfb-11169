package wheel

import "testing"

func testLayers() []LayerConfig {
	return []LayerConfig{
		{Name: "s", TickMs: 1000, WheelSize: 60},
		{Name: "m", TickMs: 60 * 1000, WheelSize: 60},
		{Name: "h", TickMs: 3600 * 1000, WheelSize: 24},
	}
}

func drain(w *Wheel, now int64) []uint64 {
	var ids []uint64
	for _, e := range w.Advance(now) {
		ids = append(ids, e.ID)
	}
	return ids
}

// A deadline exactly on a layer boundary must not be delivered a slot early
// or a slot late.
func TestBoundaryExact(t *testing.T) {
	w := New(testLayers(), 0)
	w.Add(&Entry{ID: 1, Deadline: 60000}, 0)   // exactly 1 minute = layer0 interval
	w.Add(&Entry{ID: 2, Deadline: 59999}, 0)   // one ms before
	w.Add(&Entry{ID: 3, Deadline: 3600000}, 0) // exactly 1 hour = layer1 interval
	w.Add(&Entry{ID: 4, Deadline: 3599999}, 0)

	if got := drain(w, 59998); len(got) != 0 {
		t.Fatalf("t=59998: expected nothing, got %v", got)
	}
	if got := drain(w, 59999); len(got) != 1 || got[0] != 2 {
		t.Fatalf("t=59999: expected [2], got %v", got)
	}
	if got := drain(w, 60000); len(got) != 1 || got[0] != 1 {
		t.Fatalf("t=60000: expected [1], got %v", got)
	}
	if got := drain(w, 3599999); len(got) != 1 || got[0] != 4 {
		t.Fatalf("t=3599999: expected [4], got %v", got)
	}
	if got := drain(w, 3600000); len(got) != 1 || got[0] != 3 {
		t.Fatalf("t=3600000: expected [3], got %v", got)
	}
}

// Advancing across a long empty stretch must jump, not walk slot by slot —
// and must not lose anything on the far side.
func TestEmptySkipAndCascade(t *testing.T) {
	w := New(testLayers(), 0)
	w.Add(&Entry{ID: 1, Deadline: 10*3600*1000 + 5}, 0) // 10h later, cascades h→m→s
	w.Add(&Entry{ID: 2, Deadline: 10*3600*1000 + 6}, 0)
	if got := drain(w, 5*3600*1000); len(got) != 0 {
		t.Fatalf("expected nothing at 5h, got %v", got)
	}
	got := drain(w, 10*3600*1000+10)
	if len(got) != 2 || got[0] != 1 || got[1] != 2 {
		t.Fatalf("expected [1 2] in order, got %v", got)
	}
	if p := w.Pending(); p != 0 {
		t.Fatalf("expected empty wheel, %d pending", p)
	}
}

// Delivery order within one Advance must be (deadline, id) even when
// messages land in the same slot out of order.
func TestOrderWithinSlot(t *testing.T) {
	w := New(testLayers(), 0)
	w.Add(&Entry{ID: 3, Deadline: 1500}, 0)
	w.Add(&Entry{ID: 1, Deadline: 1200}, 0)
	w.Add(&Entry{ID: 2, Deadline: 1200}, 0)
	got := drain(w, 2000)
	want := []uint64{1, 2, 3}
	if len(got) != 3 {
		t.Fatalf("got %v", got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("got %v want %v", got, want)
		}
	}
}

// Entries added while the clock is already inside their slot are delivered
// on the next Advance, never early.
func TestAddedMidTick(t *testing.T) {
	w := New(testLayers(), 0)
	w.Advance(5000) // current second-slot = 5000
	w.Add(&Entry{ID: 1, Deadline: 5500}, 5000)
	if got := drain(w, 5499); len(got) != 0 {
		t.Fatalf("early delivery: %v", got)
	}
	if got := drain(w, 5500); len(got) != 1 || got[0] != 1 {
		t.Fatalf("got %v", got)
	}
}
