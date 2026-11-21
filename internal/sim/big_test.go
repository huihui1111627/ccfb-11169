package sim_test

import (
	"testing"

	"kitewheel/internal/sim"
)

// TestBigRandom is a large differential run: tens of thousands of messages
// over a month of virtual time with bursts, cancellations, reschedules,
// calendar rules and multi-day clock jumps.
func TestBigRandom(t *testing.T) {
	if testing.Short() {
		t.Skip("large differential run")
	}
	for _, seed := range []int64{101, 102} {
		sim.Run(t, sim.Spec{
			Seed: seed, Start: base, Horizon: horizon,
			Ops: 60000, Rules: 12, Bursts: 8, MaxPrior: 8, JumpDays: 3,
		})
	}
}
