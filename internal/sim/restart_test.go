package sim_test

import (
	"testing"

	"kitewheel/internal/sim"
)

// TestRestartMidRun closes and reopens the durable engine in the middle of a
// busy run. Recovery has to reproduce the exact pending state, and every
// subsequent delivery must still match the uninterrupted reference.
func TestRestartMidRun(t *testing.T) {
	for _, at := range []int{1, 500, 4000} {
		dir := t.TempDir()
		spec := sim.Spec{
			Seed: 201, Start: base, Horizon: horizon,
			Ops: 6000, Rules: 6, Bursts: 6, MaxPrior: 5,
		}
		sim.RunWithRestart(t, spec, sim.Gen(spec), at, dir)
	}
}

// TestRestartAfterSnapshot forces several snapshot rotations (SnapshotEvery is
// 5000 records in the harness) before the restart, so recovery has to combine a
// snapshot with a log tail.
func TestRestartAfterSnapshot(t *testing.T) {
	dir := t.TempDir()
	spec := sim.Spec{
		Seed: 202, Start: base, Horizon: horizon,
		Ops: 16000, Rules: 8, Bursts: 8, MaxPrior: 5,
	}
	sim.RunWithRestart(t, spec, sim.Gen(spec), 14000, dir)
}

// TestReplayDeterminism proves the engine state is a pure function of its
// record log by replaying the same script twice into separate directories and
// comparing delivery streams.
func TestReplayDeterminism(t *testing.T) {
	spec := sim.Spec{
		Seed: 203, Start: base, Horizon: horizon,
		Ops: 4000, Rules: 5, Bursts: 4, MaxPrior: 4,
	}
	ops := sim.Gen(spec)
	sim.RunWithRestart(t, spec, ops, 0, t.TempDir())
	sim.RunWithRestart(t, spec, ops, 0, t.TempDir())
}
