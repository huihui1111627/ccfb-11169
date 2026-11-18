package scheduler_test

import (
	"container/heap"
	"math/rand"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"kitefestival/delaymsg/internal/clock"
	"kitefestival/delaymsg/internal/recur"
	"kitefestival/delaymsg/internal/scheduler"
)

// event is one delivery: message id + the clock reading when it fired.
type event struct {
	id uint64
	at int64
}

// ---------- reference scheduler: a plain min-heap ----------

type refItem struct {
	deadline int64
	id       uint64
	gen      uint64
}

type refHeap []refItem

func (h refHeap) Len() int { return len(h) }
func (h refHeap) Less(i, j int) bool {
	if h[i].deadline != h[j].deadline {
		return h[i].deadline < h[j].deadline
	}
	if h[i].id != h[j].id {
		return h[i].id < h[j].id
	}
	return h[i].gen < h[j].gen
}
func (h refHeap) Swap(i, j int)       { h[i], h[j] = h[j], h[i] }
func (h *refHeap) Push(x interface{}) { *h = append(*h, x.(refItem)) }
func (h *refHeap) Pop() interface{} {
	old := *h
	it := old[len(old)-1]
	*h = old[:len(old)-1]
	return it
}

// refSched mirrors the scheduler's externally visible semantics with the
// simplest possible correct implementation.
type refSched struct {
	h    refHeap
	gen  map[uint64]uint64
	dead map[uint64]bool // cancelled or fully delivered
	pend map[uint64]bool
	rec  map[uint64]*recur.Compiled
}

func newRef() *refSched {
	return &refSched{
		gen:  map[uint64]uint64{},
		dead: map[uint64]bool{},
		pend: map[uint64]bool{},
		rec:  map[uint64]*recur.Compiled{},
	}
}

func (r *refSched) add(id uint64, deadline, now int64, ev *[]event) {
	if deadline < now {
		*ev = append(*ev, event{id, now})
		return
	}
	r.pend[id] = true
	heap.Push(&r.h, refItem{deadline, id, r.gen[id]})
}

func (r *refSched) cancel(id uint64) {
	delete(r.pend, id)
	r.dead[id] = true
}

func (r *refSched) resched(id uint64, d, now int64, ev *[]event) {
	r.gen[id]++
	if d < now {
		delete(r.pend, id)
		r.dead[id] = true
		*ev = append(*ev, event{id, now})
		return
	}
	heap.Push(&r.h, refItem{d, id, r.gen[id]})
}

func (r *refSched) advance(now int64, ev *[]event) {
	for len(r.h) > 0 && r.h[0].deadline <= now {
		it := heap.Pop(&r.h).(refItem)
		if r.dead[it.id] || r.gen[it.id] != it.gen || !r.pend[it.id] {
			continue
		}
		*ev = append(*ev, event{it.id, now})
		if c, ok := r.rec[it.id]; ok {
			if next, ok2 := c.NextAfter(now); ok2 {
				r.gen[it.id]++
				heap.Push(&r.h, refItem{next, it.id, r.gen[it.id]})
				continue // stays pending
			}
		}
		delete(r.pend, it.id)
		r.dead[it.id] = true
	}
}

// ---------- the differential test ----------

// TestDifferential runs a virtual clock through a very unevenly distributed
// message load (festival-eve spike, boundary-exact deadlines, past-due
// messages), interleaves cancels and reschedules, restarts from the
// operation log midway, compacts, and requires the delivery sequence —
// ids and timestamps — to match a min-heap reference scheduler exactly.
func TestDifferential(t *testing.T) {
	total := 300000
	if v := os.Getenv("KITE_N"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			total = n
		}
	}
	seed := int64(20260419)
	if v := os.Getenv("KITE_SEED"); v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil {
			seed = n
		}
	}
	rng := rand.New(rand.NewSource(seed))
	loc := time.UTC

	start := int64(1_790_000_000_000) // fixed virtual epoch
	start -= start % 1000
	clk := clock.NewVirtual(start)
	logPath := filepath.Join(t.TempDir(), "ops.log")

	var got []event
	hook := func(m *scheduler.Msg, now int64) {
		got = append(got, event{m.ID, now})
	}
	newSched := func() *scheduler.Scheduler {
		s, err := scheduler.New(clk, logPath, scheduler.Config{
			CompactMinOps: 50000,
			Loc:           loc,
		}, hook)
		if err != nil {
			t.Fatalf("open scheduler: %v", err)
		}
		return s
	}
	sched := newSched()

	ref := newRef()
	var want []event

	eve := start + 26*3600*1000 // 节前一晚 20:00 附近的尖峰
	units := []int64{60 * 1000, 3600 * 1000, 24 * 3600 * 1000}
	genDeadline := func(now int64) int64 {
		switch p := rng.Float64(); {
		case p < 0.40: // 节前一晚尖峰：±3 分钟
			return eve + int64(rng.NormFloat64()*180000)
		case p < 0.60: // 10 分钟内
			return now + rng.Int63n(600000)
		case p < 0.75: // 7 天内均匀
			return now + rng.Int63n(7*24*3600*1000)
		case p < 0.85: // 正好压在层边界上
			u := units[rng.Intn(3)]
			return (now/u)*u + u*(1+rng.Int63n(100))
		case p < 0.95: // 几秒内
			return now + rng.Int63n(5000)
		default: // 已过期 → 立即投递
			return now - 1 - rng.Int63n(60000)
		}
	}
	genStep := func() int64 {
		switch p := rng.Float64(); {
		case p < 0.70:
			return 100 + rng.Int63n(1400)
		case p < 0.90:
			return 1000 + rng.Int63n(59000)
		case p < 0.98:
			return 60000 + rng.Int63n(29*60000)
		default: // 大步跳过空转区
			return 1800000 + rng.Int63n(5*3600*1000)
		}
	}

	// recurring rules under test
	startDay := time.UnixMilli(start).UTC()
	day := func(add int) string {
		return startDay.AddDate(0, 0, add).Format("2006-01-02")
	}
	rulePool := []*recur.Rule{
		{Daily: true, Time: "09:00", Offset: "-30m"},
		{Weekdays: []int{1, 3, 5}, Time: "08:15"},
		{Dates: []string{day(1), day(2)}, Time: "10:00", Offset: "15m"},
	}

	// driver-side pending set (mirrors ref.pend), as slice+set for random picks
	pendingSet := map[uint64]bool{}
	var pendingList []uint64
	pendAdd := func(id uint64) {
		if !pendingSet[id] {
			pendingSet[id] = true
			pendingList = append(pendingList, id)
		}
	}
	pendDel := func(id uint64) {
		if pendingSet[id] {
			delete(pendingSet, id)
			for i, v := range pendingList {
				if v == id {
					pendingList[i] = pendingList[len(pendingList)-1]
					pendingList = pendingList[:len(pendingList)-1]
					break
				}
			}
		}
	}
	pendPick := func() uint64 { return pendingList[rng.Intn(len(pendingList))] }

	recCompiled := map[uint64]*recur.Compiled{}
	recFires := map[uint64]int{}
	var ackable []uint64

	compared := 0
	compareEvents := func() {
		if len(got)-compared != len(want) {
			t.Fatalf("delivery count diverged: got +%d, want +%d\nlast got: %v\nlast want: %v",
				len(got)-compared, len(want), tail(got), tail(want))
		}
		for i, e := range want {
			if got[compared+i] != e {
				t.Fatalf("delivery #%d diverged: got %+v, want %+v", compared+i, got[compared+i], e)
			}
		}
		compared = len(got)
		// retire / reap recurring messages based on this round's deliveries
		for _, e := range want {
			id := e.id
			c, isRec := recCompiled[id]
			if !isRec {
				pendDel(id)
				ackable = append(ackable, id)
				continue
			}
			recFires[id]++
			if _, ok := c.NextAfter(e.at); !ok {
				pendDel(id)
				delete(recCompiled, id)
				ackable = append(ackable, id) // stays Delivered, can be acked
			} else if recFires[id] >= 3 {
				if !sched.Cancel(id) {
					t.Fatalf("cancel recurring %d failed", id)
				}
				ref.cancel(id)
				pendDel(id)
				delete(recCompiled, id)
			}
		}
		want = want[:0]
	}

	const recurringTotal = 300
	added, recurringAdded := 0, 0
	recovered, compacted := false, false

	for round := 0; ; round++ {
		if round > 300000 {
			t.Fatal("did not converge")
		}
		now := clk.Now()

		// --- adds ---
		if added < total {
			batch := 1200
			if total-added < batch {
				batch = total - added
			}
			for i := 0; i < batch; i++ {
				if recurringAdded < recurringTotal && rng.Float64() < 0.02 {
					rule := rulePool[rng.Intn(len(rulePool))]
					id, err := sched.Add("周期提醒", 0, rule)
					if err != nil {
						t.Fatalf("add recurring: %v", err)
					}
					c, _ := recur.Compile(rule, loc)
					next, _ := c.NextAfter(now)
					ref.rec[id] = c
					recCompiled[id] = c
					ref.add(id, next, now, &want)
					pendAdd(id)
					recurringAdded++
				} else {
					d := genDeadline(now)
					id, err := sched.Add("入场提醒", d, nil)
					if err != nil {
						t.Fatalf("add: %v", err)
					}
					ref.add(id, d, now, &want)
					if d >= now {
						pendAdd(id)
					}
				}
				added++
			}
		}

		// --- cancels & reschedules (cross-layer moves included) ---
		for i := 0; i < 10 && len(pendingList) > 0; i++ {
			id := pendPick()
			if rng.Float64() < 0.45 {
				if !sched.Cancel(id) {
					t.Fatalf("cancel %d failed", id)
				}
				ref.cancel(id)
				pendDel(id)
				delete(recCompiled, id)
			} else {
				nd := genDeadline(now)
				if _, isRec := recCompiled[id]; isRec && nd < now {
					// 周期消息改期到过去会立即触发并继续排期，
					// 该路径由单次消息覆盖，这里避开。
					nd = now + rng.Int63n(60000)
				}
				if !sched.Reschedule(id, nd) {
					t.Fatalf("reschedule %d failed", id)
				}
				if c, ok := recCompiled[id]; ok {
					_ = c // recurring keeps its rule; only the next fire moves
				}
				ref.resched(id, nd, now, &want)
				if nd < now {
					pendDel(id)
					delete(recCompiled, id) // fired on the spot; rule is done
				}
			}
		}

		// --- acks: confirm ~90%, leave the rest unacked (must survive
		// compaction so they can be redelivered/acked after restart) ---
		if n := len(ackable); n > 0 {
			for i := 0; i < n*9/10; i++ {
				sched.Ack(ackable[i])
			}
			ackable = ackable[:0]
		}

		// --- advance the virtual clock ---
		clk.Advance(genStep())
		sched.Tick()
		ref.advance(clk.Now(), &want)
		compareEvents()

		// --- mid-run restart from the operation log ---
		if !recovered && added > total/2 {
			recovered = true
			if err := sched.Close(); err != nil {
				t.Fatal(err)
			}
			sched = newSched()
			t.Logf("recovered from log at round %d (got %d deliveries so far)", round, len(got))
		}
		// --- forced compaction, then restart again ---
		if !compacted && added == total && len(pendingList) < total/3 {
			compacted = true
			sched.Compact()
			if err := sched.Close(); err != nil {
				t.Fatal(err)
			}
			sched = newSched()
			t.Logf("compacted + recovered at round %d", round)
		}

		if added == total && len(pendingList) == 0 {
			break
		}
	}

	// Drain stale (cancelled / superseded) entries sitting in far-future
	// buckets: step forward until the wheel is physically empty.
	for i := 0; i < 400 && sched.Snapshot().Stats.WheelPending != 0; i++ {
		clk.Advance(24 * 3600 * 1000)
		sched.Tick()
		ref.advance(clk.Now(), &want)
		compareEvents()
	}

	snap := sched.Snapshot()
	for _, l := range snap.Layers {
		t.Logf("layer %s: buckets=%d pending=%d", l.Name, l.Buckets, l.Pending)
	}
	if snap.Stats.WheelPending != 0 {
		t.Fatalf("wheel not empty at end: %d", snap.Stats.WheelPending)
	}
	if snap.Stats.Immediate == 0 {
		t.Fatal("expected some past-due immediate deliveries")
	}
	if snap.Stats.Compactions == 0 {
		t.Fatal("expected at least one automatic compaction")
	}
	t.Logf("OK: %d messages, %d deliveries matched exactly; immediate=%d cancelled=%d compactions=%d",
		total, len(got), snap.Stats.Immediate, snap.Stats.Cancelled, snap.Stats.Compactions)
	sched.Close()
}

func tail(ev []event) []event {
	if len(ev) > 5 {
		return ev[len(ev)-5:]
	}
	return ev
}
