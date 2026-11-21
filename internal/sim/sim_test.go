package sim_test

import (
	"testing"
	"time"

	"kitewheel/internal/sim"
)

var base = time.Date(2026, 9, 18, 8, 0, 0, 0, time.UTC).UnixNano()

var horizon = int64(31 * 24 * time.Hour)

func TestRandomBatches(t *testing.T) {
	for _, seed := range []int64{11, 12, 13, 14, 15, 16} {
		sim.Run(t, sim.Spec{
			Seed: seed, Start: base, Horizon: horizon,
			Ops: 3000, Rules: 5, Bursts: 5, MaxPrior: 4,
		})
	}
}

func TestDenseSameSecond(t *testing.T) {
	for _, seed := range []int64{21, 22, 23} {
		sim.Run(t, sim.Spec{
			Seed: seed, Start: base, Horizon: int64(2 * time.Hour),
			Ops: 4000, Rules: 0, Bursts: 1, MaxPrior: 6,
		})
	}
}

func TestLargeClockJumps(t *testing.T) {
	for _, seed := range []int64{31, 32} {
		sim.Run(t, sim.Spec{
			Seed: seed, Start: base, Horizon: horizon,
			Ops: 3000, Rules: 6, Bursts: 2, MaxPrior: 3, JumpDays: 7,
		})
	}
}

func TestFullScale(t *testing.T) {
	if testing.Short() {
		t.Skip("full-scale differential run")
	}
	sim.Run(t, sim.Spec{
		Seed: 101, Start: base, Horizon: horizon,
		Ops: 120000, Rules: 12, Bursts: 8, MaxPrior: 8, JumpDays: 3,
	})
}

func TestManySeeds(t *testing.T) {
	if testing.Short() {
		t.Skip("wide seed sweep")
	}
	starts := []time.Time{
		time.Date(2027, 12, 28, 0, 0, 0, 0, time.UTC), // year boundary
		time.Date(2028, 2, 20, 0, 0, 0, 0, time.UTC),  // leap February
		time.Date(2029, 2, 20, 0, 0, 0, 0, time.UTC),  // non-leap February
		time.Date(2028, 6, 30, 0, 0, 0, 0, time.UTC),  // mid-year
	}
	for seed := int64(500); seed < 520; seed++ {
		sim.Run(t, sim.Spec{
			Seed: seed, Start: starts[int(seed)%len(starts)].UnixNano(),
			Horizon: horizon, Ops: 1500, Rules: 4, Bursts: 4,
			MaxPrior: 5, JumpDays: int(seed % 4),
		})
	}
}
