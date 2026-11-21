// Package sim drives the engine and the independent reference scheduler
// through identical operation streams and compares every delivery: identity,
// due time, delivery instant and past-due flag, in order.
//
// The generator is adversarial on purpose. Due times come from a mix of
// distributions: a handful of dense burst instants (a festival night's
// simultaneous reminders), the next minute, the next two days, the next month,
// the far tail of the horizon, and times already in the past. Operations are
// interleaved randomly: enqueue, cancel, reschedule, clock advances (some
// multi-day jumps), and calendar rule creation and deletion.
package sim

import (
	"fmt"
	"math/rand"
	"time"

	"kitewheel/internal/calendar"
	"kitewheel/internal/engine"
	"kitewheel/internal/refsched"
)

// Op is one generated operation.
type Op struct {
	Kind  string // enqueue, cancel, reschedule, advance, addRule, delRule
	ID    string
	Due   int64
	Prio  int
	Rule  calendar.Rule
	Delta int64 // absolute advance target
}

// Spec controls a generated scenario.
type Spec struct {
	Seed     int64
	Start    int64 // starting virtual time
	Horizon  int64 // usable future window
	Ops      int   // number of operations
	Rules    int   // maximum number of live calendar rules
	Bursts   int   // number of dense burst instants
	MaxPrior int
	// JumpDays, when positive, forces every advance to move the clock by at
	// least this many days instead of a random step.
	JumpDays int
}

// T is the subset of testing.T the harness uses.
type T interface {
	Helper()
	Fatalf(string, ...any)
	Logf(string, ...any)
}

// Gen produces a deterministic operation script. Due times are relative to
// the virtual clock at the moment the operation is generated, which is what a
// real producer does; the clock itself advances in mostly small steps with
// occasional multi-day jumps.
func Gen(spec Spec) []Op {
	rng := rand.New(rand.NewSource(spec.Seed))
	base := spec.Start
	now := base
	var ops []Op

	// Event instants are shared absolute times a large share of messages aim
	// at, like a session opening or a refund deadline. They produce genuine
	// many-hundreds-at-one-instant bursts.
	var events []int64
	newEvent := func() int64 {
		e := now + int64(rng.Int63n(int64(3*time.Hour))) + int64(rng.Int63n(int64(1000)))
		events = append(events, e)
		if len(events) > 32 {
			events = events[1:]
		}
		return e
	}
	pickDue := func() int64 {
		var off int64
		switch r := rng.Intn(100); {
		case r < 35 && len(events) > 0:
			// Exactly one shared instant, with nanosecond jitter only. Events
			// already in the past are skipped: a producer schedules ahead.
			if e, ok := pickFutureEvent(events, now+int64(time.Second)); ok {
				return e + int64(rng.Intn(3))
			}
			off = int64(rng.Int63n(int64(90 * time.Second)))
		case r < 48:
			// A brand new shared instant that later messages will target.
			return newEvent()
		case r < 55:
			off = int64(rng.Int63n(int64(90 * time.Second)))
		case r < 58:
			off = int64(rng.Intn(int(5*time.Second))) + int64(rng.Intn(1000))
		case r < 75:
			off = int64(rng.Int63n(int64(48 * time.Hour)))
		case r < 95:
			off = int64(rng.Int63n(int64(25 * 24 * time.Hour)))
		case r < 98:
			// Near but inside the coarsest horizon: the tail the wheel must
			// still hold without overflowing.
			off = int64(730*24*time.Hour) + int64(rng.Int63n(int64(14*24*time.Hour)))
		default:
			// Already past: delivered at once, counted separately.
			off = -1 - int64(rng.Int63n(int64(2*time.Hour)))
		}
		return now + off
	}
	pickStep := func() int64 {
		switch r := rng.Intn(1000); {
		case r < 900:
			return int64(rng.Int63n(int64(2 * time.Minute)))
		case r < 960:
			return int64(rng.Int63n(int64(2 * time.Hour)))
		case r < 992:
			return int64(rng.Int63n(int64(24 * time.Hour)))
		default:
			days := int64(spec.JumpDays)
			if days <= 0 {
				days = 4
			}
			return int64(rng.Int63n(days*24+1)) * int64(time.Hour)
		}
	}

	var (
		pending []string
		rules   []string
		nextID  int
		nextRID int
	)
	enqueue := func(n int) {
		// The first message of a clump also tends to target a shared instant,
		// so clumps stack onto event bursts instead of spreading evenly.
		burstAt := pickDue()
		for i := 0; i < n; i++ {
			id := fmt.Sprintf("msg-%07d", nextID)
			nextID++
			due := pickDue()
			if rng.Intn(3) == 0 {
				due = burstAt + int64(rng.Intn(5))
			}
			ops = append(ops, Op{Kind: "enqueue", ID: id, Due: due, Prio: rng.Intn(spec.MaxPrior + 1)})
			pending = append(pending, id)
		}
	}

	// The script alternates between clumps of enqueues (which build up a deep
	// wheel with genuine same-instant bursts) and single disruptive
	// operations: a clock step, a cancellation, a reschedule, a rule change.
	// Without clumps the wheel would never hold more than a few hundred
	// messages, and the hierarchy would go largely untested.
	for i := 0; i < spec.Ops; {
		// A clump is 20-200 messages, occasionally much larger.
		clump := 20 + rng.Intn(180)
		if rng.Intn(20) == 0 {
			clump = 500 + rng.Intn(2000)
		}
		if i+clump > spec.Ops {
			clump = spec.Ops - i
		}
		enqueue(clump)
		i += clump
		if i >= spec.Ops {
			break
		}
		switch choice := rng.Intn(100); {
		case choice < 42:
			if step := pickStep(); step > 0 {
				now += step
				ops = append(ops, Op{Kind: "advance", Delta: now})
			}
		case choice < 70 && len(pending) > 0:
			idx := rng.Intn(len(pending))
			ops = append(ops, Op{Kind: "cancel", ID: pending[idx]})
			pending = append(pending[:idx], pending[idx+1:]...)
		case choice < 90 && len(pending) > 0:
			id := pending[rng.Intn(len(pending))]
			ops = append(ops, Op{Kind: "reschedule", ID: id, Due: pickDue(), Prio: rng.Intn(spec.MaxPrior + 1)})
		case len(rules) < spec.Rules:
			rid := fmt.Sprintf("rule-%04d", nextRID)
			nextRID++
			rules = append(rules, rid)
			ops = append(ops, Op{Kind: "addRule", Rule: randomRule(rng, rid, now)})
		case len(rules) > 0:
			idx := rng.Intn(len(rules))
			ops = append(ops, Op{Kind: "delRule", ID: rules[idx]})
			rules = append(rules[:idx], rules[idx+1:]...)
		}
		i++
	}
	// Final advance far enough that a month of reminders has come due. Rules
	// keep generating future occurrences, so this is not a full drain.
	ops = append(ops, Op{Kind: "advance", Delta: now + int64(744*time.Hour)})
	return ops
}

func pickFutureEvent(events []int64, after int64) (int64, bool) {
	var cand []int64
	for i := len(events) - 1; i >= 0 && len(cand) < 8; i-- {
		if events[i] > after {
			cand = append(cand, events[i])
		}
	}
	if len(cand) == 0 {
		return 0, false
	}
	return cand[0], true
}

func nextTarget(rng *rand.Rand, spec Spec, now, base, last int64) int64 {
	var target int64
	if spec.JumpDays > 0 {
		days := spec.JumpDays + rng.Intn(spec.JumpDays+1)
		target = now + int64(days)*int64(24*time.Hour) + int64(rng.Int63n(int64(time.Hour)))
	} else {
		target = now + int64(rng.Int63n(spec.Horizon/2+1))
	}
	if target > last {
		target = last
	}
	return target
}

func randomRule(rng *rand.Rand, id string, base int64) calendar.Rule {
	loc := time.FixedZone("cst", 8*3600)
	start := time.Unix(0, base).In(loc)
	r := calendar.Rule{ID: id, Created: base, TZOffsetSec: 8 * 3600}
	switch rng.Intn(5) {
	case 0:
		r.Kind = calendar.KindMonthlyDay
		r.Day = 1 + rng.Intn(31)
		r.Missing = []calendar.Missing{calendar.MissingSkip, calendar.MissingClamp}[rng.Intn(2)]
	case 1:
		r.Kind = calendar.KindMonthlyNthWeekday
		r.Nth = []int{-1, 1, 2, 3, 4, 5}[rng.Intn(6)]
		r.Weekday = rng.Intn(7)
		r.Missing = []calendar.Missing{calendar.MissingSkip, calendar.MissingClamp}[rng.Intn(2)]
	case 2:
		r.Kind = calendar.KindMonthlyNthWorkday
		r.Nth = []int{-1, 1, 2, 3, 10, 23}[rng.Intn(6)]
		r.Missing = []calendar.Missing{calendar.MissingSkip, calendar.MissingClamp}[rng.Intn(2)]
		for i := 0; i < rng.Intn(4); i++ {
			r.Holidays = append(r.Holidays, start.AddDate(0, 0, rng.Intn(400)).Format("2006-01-02"))
		}
	case 3:
		r.Kind = calendar.KindDates
		seen := map[string]bool{}
		for i, n := 0, 1+rng.Intn(5); i < n; i++ {
			s := start.AddDate(0, 0, rng.Intn(400)).Format("2006-01-02")
			if !seen[s] {
				seen[s] = true
				r.Dates = append(r.Dates, s)
			}
		}
		if len(r.Dates) == 0 {
			r.Dates = []string{start.AddDate(0, 0, 1).Format("2006-01-02")}
		}
	default:
		r.Kind = calendar.KindMonthlyDay
		r.Day = 28 + rng.Intn(4)
		r.Missing = calendar.MissingClamp
	}
	r.TimeOfDay = fmt.Sprintf("%02d:%02d", rng.Intn(24), []int{0, 15, 30, 45}[rng.Intn(4)])
	r.OffsetSec = []int64{0, -1800, -3600, 1800, -30, 3600}[rng.Intn(6)]
	r.Priority = rng.Intn(3)
	return r
}

// Run generates and executes a scenario.
func Run(t T, spec Spec) []Op {
	t.Helper()
	return RunOps(t, spec, Gen(spec))
}

// RunOps executes a script against both schedulers and compares every
// delivery.
func RunOps(t T, spec Spec, ops []Op) []Op {
	t.Helper()
	run(t, spec, ops, 0)
	return ops
}

// RunWithRestart executes a script against a durable engine rooted at dir,
// closing and reopening it at the given operation index. The reopened engine
// must present the identical pending count and clock, and its subsequent
// deliveries must still match the uninterrupted reference exactly.
func RunWithRestart(t T, spec Spec, ops []Op, restartAt int, dir string) {
	t.Helper()
	runAt(t, spec, ops, restartAt, dir)
}

func run(t T, spec Spec, ops []Op, restartAt int) {
	t.Helper()
	runAt(t, spec, ops, restartAt, "")
}

func runAt(t T, spec Spec, ops []Op, restartAt int, dir string) {
	t.Helper()
	horizon := engine.HorizonOf(engine.DefaultLevels())
	ref := refsched.New(spec.Start, horizon)

	var got []engine.Delivery
	open := func() *engine.Engine {
		e, err := engine.New(engine.Config{
			Dir:           dir,
			Levels:        engine.DefaultLevels(),
			Now:           spec.Start,
			History:       1 << 24,
			SnapshotEvery: 5000,
			Sink:          func(d engine.Delivery) { got = append(got, d) },
		})
		if err != nil {
			t.Fatalf("engine: %v", err)
		}
		return e
	}
	eng := open()
	compared := 0

	compareNew := func(step int) {
		for compared < len(ref.Out) {
			if compared >= len(got) {
				t.Fatalf("step %d: engine delivered %d, reference %d (missing %s due %d at %d)",
					step, len(got), len(ref.Out), ref.Out[compared].ID,
					ref.Out[compared].Due-spec.Start, ref.Out[compared].At-spec.Start)
			}
			g, r := got[compared], ref.Out[compared]
			if g.ID != r.ID || g.Due != r.Due || g.At != r.At || g.PastDue != r.PastDue {
				t.Fatalf("step %d, delivery %d: engine %s due %d at %d past=%v; reference %s due %d at %d past=%v",
					step, compared, g.ID, g.Due-spec.Start, g.At-spec.Start, g.PastDue,
					r.ID, r.Due-spec.Start, r.At-spec.Start, r.PastDue)
			}
			compared++
		}
		if len(got) != compared {
			t.Fatalf("step %d: engine delivered %d, reference only %d (%s at %d)",
				step, len(got), compared, got[compared].ID, got[compared].At-spec.Start)
		}
	}

	for i, op := range ops {
		if restartAt > 0 && i == restartAt {
			before := eng.Pending()
			if err := eng.Close(); err != nil {
				t.Fatalf("restart %d: close: %v", i, err)
			}
			eng = open()
			t.Logf("restarted at op %d: %d pending before, %d after", i, before, eng.Pending())
			if eng.Pending() != before {
				t.Fatalf("restart at op %d: %d pending before, %d after", i, before, eng.Pending())
			}
			if eng.Now() != ref.Now() {
				t.Fatalf("restart at op %d: clock %d, reference %d", i, eng.Now(), ref.Now())
			}
		}
		switch op.Kind {
		case "enqueue":
			_, eerr := eng.Enqueue(op.ID, op.Due, op.Prio, "")
			rerr := ref.Enqueue(op.ID, op.Due, op.Prio, "")
			if (eerr == nil) != (rerr == nil) {
				t.Fatalf("op %d enqueue %s due %d: engine %v, reference %v", i, op.ID, op.Due-spec.Start, eerr, rerr)
			}
		case "cancel":
			eerr := eng.Cancel(op.ID)
			rerr := ref.Cancel(op.ID)
			if (eerr == nil) != (rerr == nil) {
				t.Fatalf("op %d cancel %s: engine %v, reference %v", i, op.ID, eerr, rerr)
			}
		case "reschedule":
			_, eerr := eng.Reschedule(op.ID, op.Due, op.Prio)
			rerr := ref.Reschedule(op.ID, op.Due, op.Prio)
			if (eerr == nil) != (rerr == nil) {
				t.Fatalf("op %d reschedule %s: engine %v, reference %v", i, op.ID, eerr, rerr)
			}
		case "advance":
			eerr := eng.AdvanceTo(op.Delta)
			rerr := ref.AdvanceTo(op.Delta)
			if (eerr == nil) != (rerr == nil) {
				t.Fatalf("op %d advance to %d: engine %v, reference %v", i, op.Delta-spec.Start, eerr, rerr)
			}
		case "addRule":
			_, eerr := eng.AddRule(op.Rule)
			rerr := ref.AddRule(op.Rule)
			if (eerr == nil) != (rerr == nil) {
				t.Fatalf("op %d addRule %s: engine %v, reference %v", i, op.Rule.ID, eerr, rerr)
			}
		case "delRule":
			eerr := eng.DeleteRule(op.ID)
			rerr := ref.DeleteRule(op.ID)
			if (eerr == nil) != (rerr == nil) {
				t.Fatalf("op %d delRule %s: engine %v, reference %v", i, op.ID, eerr, rerr)
			}
		}
		compareNew(i)
	}
	compareNew(len(ops))
	// Recurring rules keep producing occurrences forever, so the run cannot
	// end with an empty wheel. Both schedulers must agree on exactly which
	// occurrences remain pending.
	if got, want := eng.Pending(), ref.Pending(); got != want {
		t.Fatalf("pending at end: engine %d, reference %d", got, want)
	}
	if err := eng.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
}
