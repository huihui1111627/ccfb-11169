package engine

import (
	"fmt"
	"testing"
	"time"
)

func TestBurstScaling(t *testing.T) {
	// A festival-night burst must be linear-ish in the number of messages.
	// Doubling the burst should not quadruple the delivery time.
	var prev time.Duration
	for _, n := range []int{20000, 40000, 80000, 160000} {
		base := time.Date(2026, 9, 18, 8, 0, 0, 0, time.UTC).UnixNano()
		e, _ := New(Config{Now: base, History: 16})
		open := base + int64(26*time.Hour)
		for i := 0; i < n; i++ {
			e.Enqueue(fmt.Sprintf("m%d", i), open+int64(i%500), i%5, "")
		}
		start := time.Now()
		if err := e.AdvanceTo(base + int64(27*time.Hour)); err != nil {
			t.Fatal(err)
		}
		elapsed := time.Since(start)
		if e.Stats().Delivered != uint64(n) {
			t.Fatalf("n=%d delivered %d", n, e.Stats().Delivered)
		}
		if prev > 0 && elapsed > prev*3 {
			t.Fatalf("burst of %d took %s after %s for half as many: not linear", n, elapsed, prev)
		}
		prev = elapsed
		t.Logf("%d messages delivered in %s", n, elapsed)
	}
}
