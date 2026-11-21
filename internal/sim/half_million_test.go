package sim_test

import (
	"testing"

	"kitewheel/internal/sim"
)

// TestHalfMillionSkewed is the headline comparison: roughly half a million
// messages whose due times are deliberately very unevenly distributed (a dense
// cluster at a handful of instants, long quiet stretches, a heavy tail near
// the horizon, and a slice already in the past), interleaved with
// cancellations, reschedules, calendar rules and multi-day clock jumps, all
// verified message-by-message against the min-heap reference.
func TestHalfMillionSkewed(t *testing.T) {
	if testing.Short() {
		t.Skip("half-million-message differential run")
	}
	sim.Run(t, sim.Spec{
		Seed: 9091, Start: base, Horizon: horizon,
		Ops: 520000, Rules: 24, Bursts: 16, MaxPrior: 12, JumpDays: 5,
	})
}
