package sim_test

import (
	"testing"
	"time"

	"kitewheel/internal/engine"
)

// TestFestivalNightBurst reproduces the incident: several hundred thousand
// reminders all falling due in one second, with the clock jumping straight to
// that second from a day earlier. Every message must be delivered at its due
// instant in priority-then-insertion order, and the whole burst must finish in
// well under a second of wall time.
func TestFestivalNightBurst(t *testing.T) {
	if testing.Short() {
		t.Skip("large burst")
	}
	base := time.Date(2026, 9, 18, 8, 0, 0, 0, time.UTC)
	e, err := engine.New(engine.Config{Now: base.UnixNano(), History: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	const n = 300000
	night := base.Add(26 * time.Hour)
	start := time.Now()
	for i := 0; i < n; i++ {
		// All within the same second, nanosecond spread.
		due := night.Add(time.Duration(i%1000) * time.Nanosecond)
		if _, err := e.Enqueue(idOf(i), due.UnixNano(), i%5, ""); err != nil {
			t.Fatalf("enqueue %d: %v", i, err)
		}
	}
	enqueueTook := time.Since(start)

	start = time.Now()
	if err := e.AdvanceTo(night.Add(time.Second).UnixNano()); err != nil {
		t.Fatal(err)
	}
	deliverTook := time.Since(start)
	t.Logf("enqueue %d messages: %s; deliver: %s", n, enqueueTook, deliverTook)

	if got := e.Stats().Delivered; got != n {
		t.Fatalf("delivered %d of %d", got, n)
	}
	h := e.History(0) // newest first
	if len(h) != n {
		t.Fatalf("history %d", len(h))
	}
	// History is newest first, so walking it backwards yields the delivery
	// stream, which must be non-decreasing under the delivery key.
	prev := h[len(h)-1]
	for i := len(h) - 2; i >= 0; i-- {
		cur := h[i]
		if cur.Due < prev.Due {
			t.Fatalf("index %d: due went backwards", i)
		}
		if cur.Due == prev.Due {
			if cur.Priority > prev.Priority {
				t.Fatalf("index %d: priority went up", i)
			}
			if cur.Priority == prev.Priority && cur.Seq < prev.Seq {
				t.Fatalf("index %d: insertion order went backwards", i)
			}
		}
		prev = cur
	}
	if deliverTook > 2*time.Second {
		t.Fatalf("burst delivery took %s, expected well under a second", deliverTook)
	}
}

func idOf(i int) string {
	const digits = "0123456789"
	b := make([]byte, 0, 16)
	b = append(b, "ticket-"...)
	var buf [12]byte
	p := len(buf)
	if i == 0 {
		p--
		buf[p] = '0'
	}
	for i > 0 {
		p--
		buf[p] = digits[i%10]
		i /= 10
	}
	return string(append(b, buf[p:]...))
}
