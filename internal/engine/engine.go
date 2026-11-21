// Package engine is the scheduler state machine: a hierarchical timing wheel
// whose observable behaviour is a deterministic function of a record log.
// Delivery order, delivery time, cancellations, reschedules and calendar
// occurrences replay identically from that log.
//
// # Time model
//
// The engine has a virtual clock, Now. Nothing happens until AdvanceTo moves
// it. At a given instant events are processed in this order:
//
//  1. calendar rules whose next occurrence enters the coarsest horizon are
//     materialized;
//  2. bucket cascades, coarsest level first, so a message that lands in a finer
//     bucket starting at the same instant is redistributed again in the same
//     step;
//  3. level-0 deliveries due at or before that instant, in key order.
//
// The next instant is the exact minimum of the earliest non-empty coarser
// bucket start, the earliest level-0 due time, and the next rule
// materialization. Empty slots are never visited, so a jump across a month
// costs a handful of steps, not 2.6 million.
//
// # Ordering
//
// Every message due at the same instant, whatever level it travelled through,
// ends up in a level-0 bucket, and level-0 delivery picks the globally smallest
// key across buckets. The key is: due, then priority descending, then class,
// then insertion order (for calendar occurrences, rule creation order and
// occurrence index), then unique id. Cascades merge sorted runs, so relocating
// a message between levels cannot perturb the order. See model.Key.
//
// # Identities
//
// A message has a Seq (insertion order) and a UID (unique instance identity).
// Both are renewed on every reschedule, so a rescheduled message counts as
// added at its new instant, exactly as if it had been enqueued there. Bucket
// membership is tracked by UID, so a stale entry for an older version can never
// be delivered. Calendar occurrences use the deterministic ID "ruleID#index".
package engine

import (
	"container/heap"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"kitewheel/internal/calendar"
	"kitewheel/internal/model"
	"kitewheel/internal/store"
	"kitewheel/internal/wheel"
)

// Public errors.
var (
	// ErrDuplicateID is returned when scheduling an ID that is already pending.
	ErrDuplicateID = errors.New("engine: message id already pending")
	// ErrNotFound is returned by Cancel and Reschedule for unknown IDs.
	ErrNotFound = errors.New("engine: message id not found")
	// ErrDuplicateRule is returned when adding a rule with an existing ID.
	ErrDuplicateRule = errors.New("engine: rule id already exists")
	// ErrRuleNotFound is returned by DeleteRule for unknown rules.
	ErrRuleNotFound = errors.New("engine: rule id not found")
	// ErrClockBackwards is returned when the clock is asked to move backwards.
	ErrClockBackwards = errors.New("engine: virtual clock cannot move backwards")
)

// Delivery is one delivered message.
type Delivery struct {
	UID      uint64            `json:"uid"`
	Seq      uint64            `json:"seq"`
	ID       string            `json:"id"`
	Due      int64             `json:"due"`
	At       int64             `json:"at"`
	Priority int               `json:"priority"`
	Class    model.Class       `json:"class"`
	Payload  string            `json:"payload"`
	Rule     *model.Occurrence `json:"rule,omitempty"`
	PastDue  bool              `json:"past_due"`
}

// Stats are lifetime counters.
type Stats struct {
	Enqueued    uint64 `json:"enqueued"`
	Cancelled   uint64 `json:"cancelled"`
	Rescheduled uint64 `json:"rescheduled"`
	Delivered   uint64 `json:"delivered"`
	PastDue     uint64 `json:"past_due"`
	Cascades    uint64 `json:"cascades"`
	IdleJumps   uint64 `json:"idle_jumps"`
	IdleNanos   int64  `json:"idle_nanos"`
	Rules       uint64 `json:"rules"`
}

// Config configures an engine.
type Config struct {
	// Dir is the store root. Empty means memory-only.
	Dir string
	// Levels defaults to DefaultLevels.
	Levels []wheel.LevelSpec
	// Epoch is the wheel origin in unix nanoseconds.
	Epoch int64
	// History is the delivered-history ring size, default 4096.
	History int
	// SnapshotEvery is the number of records after which the engine snapshots
	// and rotates the log. Zero disables automatic snapshots.
	SnapshotEvery int64
	// Sink receives every delivery synchronously. It must not call back into
	// the engine.
	Sink func(Delivery)
	// Now, when non-zero, is where a fresh store's clock starts.
	Now int64
}

// DefaultLevels returns the standard 60s / 60m / 24h / 31d layout.
func DefaultLevels() []wheel.LevelSpec {
	return []wheel.LevelSpec{
		{Name: "秒", Len: 60, Span: time.Second},
		{Name: "分", Len: 60, Span: time.Minute},
		{Name: "时", Len: 24, Span: time.Hour},
		{Name: "天", Len: 31, Span: 24 * time.Hour},
	}
}

type bucketKey struct {
	level int
	start int64
}

// bucket is an append-only message slice with a delivery cursor. Messages
// before head have been delivered; cancelled or rescheduled messages are
// marked dead and skipped when the cursor reaches them. The slice is sorted
// only when a drain, peek or snapshot needs order, so bulk insertion of a
// night's worth of messages stays linear.
type bucket struct {
	key    bucketKey
	msgs   []model.Message
	head   int
	live   int
	dead   map[uint64]struct{}
	sorted bool
}

func newBucket(k bucketKey) *bucket {
	return &bucket{key: k, dead: map[uint64]struct{}{}, sorted: true}
}

func (b *bucket) add(m model.Message) {
	b.msgs = append(b.msgs, m)
	b.live++
	b.sorted = false
}

func (b *bucket) kill(uid uint64) {
	if _, ok := b.dead[uid]; ok {
		return
	}
	b.dead[uid] = struct{}{}
	b.live--
}

// compact drops the delivered prefix and dead entries. It is amortised: it
// only runs once the wasted space is a large fraction of the bucket.
func (b *bucket) compact() {
	if b.head == 0 && len(b.dead) == 0 {
		return
	}
	out := b.msgs[:0]
	for i := b.head; i < len(b.msgs); i++ {
		m := b.msgs[i]
		if _, dead := b.dead[m.UID]; dead {
			continue
		}
		out = append(out, m)
	}
	b.msgs = out
	b.head = 0
	b.dead = map[uint64]struct{}{}
}

// sortNow orders the live region. Delivered entries before head are ignored.
func (b *bucket) sortNow() {
	if b.sorted {
		return
	}
	if len(b.dead) > len(b.msgs)/4 && len(b.dead) > 64 {
		b.compact()
	}
	model.SortMessages(b.msgs[b.head:])
	b.sorted = true
}

func (b *bucket) peek() (model.Message, bool) {
	if b.live <= 0 {
		return model.Message{}, false
	}
	b.sortNow()
	for b.head < len(b.msgs) {
		m := b.msgs[b.head]
		if _, dead := b.dead[m.UID]; dead {
			b.head++
			continue
		}
		return m, true
	}
	return model.Message{}, false
}

func (b *bucket) pop() (model.Message, bool) {
	m, ok := b.peek()
	if !ok {
		return model.Message{}, false
	}
	b.head++
	b.live--
	// Compact only when the dead prefix is as large as the live remainder, so
	// the cost is O(n) once per n pops rather than O(n) per pop.
	if b.head >= 64 && b.head >= len(b.msgs)-b.head {
		b.compact()
	}
	return m, true
}

// drain returns every live message sorted, then resets the bucket.
func (b *bucket) drain() []model.Message {
	if b.live <= 0 {
		b.reset()
		return nil
	}
	b.sortNow()
	out := make([]model.Message, 0, b.live)
	for i := b.head; i < len(b.msgs); i++ {
		m := b.msgs[i]
		if _, dead := b.dead[m.UID]; dead {
			continue
		}
		out = append(out, m)
	}
	b.reset()
	return out
}

func (b *bucket) reset() {
	b.msgs, b.head, b.dead, b.live, b.sorted = nil, 0, map[uint64]struct{}{}, 0, true
}

// snapshotCopy returns the live messages, sorted, without changing the bucket.
func (b *bucket) snapshotCopy() []model.Message {
	if b.live <= 0 {
		return nil
	}
	out := make([]model.Message, 0, b.live)
	for i := b.head; i < len(b.msgs); i++ {
		m := b.msgs[i]
		if _, dead := b.dead[m.UID]; dead {
			continue
		}
		out = append(out, m)
	}
	model.SortMessages(out)
	return out
}

// int64Heap is a min-heap of bucket start times.
type int64Heap []int64

func (h int64Heap) Len() int           { return len(h) }
func (h int64Heap) Less(i, j int) bool { return h[i] < h[j] }
func (h int64Heap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *int64Heap) Push(x any)        { *h = append(*h, x.(int64)) }
func (h *int64Heap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[:n-1]
	return x
}

type levelState struct {
	buckets map[int64]*bucket
	starts  int64Heap // coarser levels: bucket start times
}

type ruleState struct {
	rule    calendar.Rule
	ruleOrd uint64
	pending bool
}

// Engine is the scheduler. Public methods are safe for concurrent use.
type Engine struct {
	mu   sync.Mutex
	geom *wheel.Geometry

	now     int64
	nextUID uint64
	nextSeq uint64
	ruleOrd uint64

	levels  []*levelState
	byID    map[string]model.Message
	idOfUID map[uint64]string
	where   map[uint64]bucketKey
	rules   map[string]*ruleState
	stats   Stats

	history []Delivery
	histPos int

	dir       string
	st        *store.Dir
	snapEvery int64
	sinceSnap int64
	sink      func(Delivery)
	loading   bool
}

// New creates an engine, restoring its snapshot and replaying the log tail.
func New(cfg Config) (*Engine, error) {
	if len(cfg.Levels) == 0 {
		cfg.Levels = DefaultLevels()
	}
	if cfg.History <= 0 {
		cfg.History = 4096
	}
	geom, err := wheel.NewGeometry(cfg.Epoch, cfg.Levels)
	if err != nil {
		return nil, err
	}
	e := &Engine{
		geom:      geom,
		now:       cfg.Epoch,
		nextUID:   1,
		nextSeq:   1,
		levels:    make([]*levelState, len(geom.Levels())),
		byID:      map[string]model.Message{},
		idOfUID:   map[uint64]string{},
		where:     map[uint64]bucketKey{},
		rules:     map[string]*ruleState{},
		history:   make([]Delivery, 0, cfg.History),
		snapEvery: cfg.SnapshotEvery,
		dir:       cfg.Dir,
		sink:      cfg.Sink,
	}
	for i := range e.levels {
		e.levels[i] = &levelState{buckets: map[int64]*bucket{}}
	}
	if cfg.Dir != "" {
		if err := e.restore(cfg.Now); err != nil {
			return nil, err
		}
	} else if cfg.Now != 0 {
		e.now = cfg.Now
	}
	return e, nil
}

// restore loads a snapshot (if any), then replays the log tail. A store with no
// snapshot yet starts its clock at initialNow, so a fresh durable engine
// behaves exactly like an in-memory one.
func (e *Engine) restore(initialNow int64) error {
	d, hdr, buckets, err := store.OpenDir(e.dir)
	if err != nil {
		return err
	}
	e.st = d
	if hdr == nil && initialNow != 0 {
		e.now = initialNow
	}
	if hdr != nil {
		e.now = hdr.Now
		e.nextUID = hdr.NextUID
		e.nextSeq = hdr.NextSeq
		e.ruleOrd = hdr.RuleOrd
		if len(hdr.Stats) > 0 {
			if err := json.Unmarshal(hdr.Stats, &e.stats); err != nil {
				return fmt.Errorf("engine: decode stats: %w", err)
			}
		}
		if len(hdr.Rules) > 0 {
			var rules []calendar.Rule
			if err := json.Unmarshal(hdr.Rules, &rules); err != nil {
				return fmt.Errorf("engine: decode rules: %w", err)
			}
			for _, r := range rules {
				e.rules[r.ID] = &ruleState{rule: r, ruleOrd: r.Ord}
			}
		}
		for _, b := range buckets {
			if b.Level < 0 || b.Level >= len(e.levels) {
				return fmt.Errorf("engine: snapshot bucket level %d out of range", b.Level)
			}
			for _, m := range b.Msgs {
				e.insertDirect(m, bucketKey{level: b.Level, start: b.Start})
			}
		}
	}
	e.loading = true
	rd, err := store.NewReader(d.Log().Path(), d.Log().Seq()-d.Log().Count())
	if err != nil {
		return err
	}
	for rd.Next() {
		if err := e.apply(rd.Record()); err != nil {
			rd.Close()
			return err
		}
	}
	err = rd.Err()
	rd.Close()
	if err != nil {
		return err
	}
	e.loading = false
	for _, m := range e.byID {
		if m.Rule != nil {
			if rs := e.rules[m.Rule.RuleID]; rs != nil {
				rs.pending = true
			}
		}
	}
	return nil
}

// Close snapshots and closes the store.
func (e *Engine) Close() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.st == nil {
		return nil
	}
	if err := e.snapshotLocked(); err != nil {
		return err
	}
	return e.st.Close()
}

// Now returns the virtual clock.
func (e *Engine) Now() int64 {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.now
}

// Geometry returns the level layout.
func (e *Engine) Geometry() *wheel.Geometry { return e.geom }

// MaxDue returns the latest due time the wheel can currently accept.
func (e *Engine) MaxDue() int64 {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.now + e.geom.Coarsest().Horizon() - 1
}

// ---- placement ----

func (e *Engine) bucketFor(due int64) (bucketKey, error) {
	lvl, ok := e.geom.LevelForDue(e.now, due)
	if !ok {
		return bucketKey{}, fmt.Errorf("%w: horizon is %s; add a coarser level to hold later dues",
			wheel.ErrNoLevel, time.Duration(e.geom.Coarsest().Horizon()))
	}
	start := e.geom.Levels()[lvl].SlotStart(e.geom.Epoch, due)
	if start < e.now {
		lvl = 0
		start = e.geom.Levels()[0].SlotStart(e.geom.Epoch, due)
	}
	return bucketKey{level: lvl, start: start}, nil
}

func (e *Engine) insertDirect(m model.Message, k bucketKey) {
	ls := e.levels[k.level]
	b := ls.buckets[k.start]
	if b == nil {
		b = newBucket(k)
		ls.buckets[k.start] = b
		if k.level > 0 {
			heap.Push(&ls.starts, k.start)
		}
	}
	b.add(m)
	e.byID[m.ID] = m
	e.idOfUID[m.UID] = m.ID
	e.where[m.UID] = k
}

// zeroHead returns the smallest-key head among level-0 buckets.
//
// Level 0 has Len slots (60 seconds by default), so the number of level-0
// buckets is bounded by a small constant and scanning them is O(1) in the
// message count. That is simpler and, unlike a heap keyed on a snapshot of each
// bucket head, it stays correct when a bucket gains a smaller head after the
// heap entry was pushed.
func (e *Engine) zeroHead(t int64) (*bucket, model.Message, bool) {
	var (
		best    *bucket
		bestMsg model.Message
		bestKey model.Key
	)
	for start, b := range e.levels[0].buckets {
		if b.live <= 0 {
			delete(e.levels[0].buckets, start)
			continue
		}
		m, ok := b.peek()
		if !ok {
			delete(e.levels[0].buckets, start)
			continue
		}
		if m.Due > t {
			continue
		}
		k := model.KeyOf(m)
		if best == nil || k.Less(bestKey) {
			best, bestMsg, bestKey = b, m, k
		}
	}
	return best, bestMsg, best != nil
}

func (e *Engine) removeByID(id string) (model.Message, bool) {
	m, ok := e.byID[id]
	if !ok {
		return model.Message{}, false
	}
	delete(e.byID, id)
	delete(e.idOfUID, m.UID)
	if k, ok := e.where[m.UID]; ok {
		delete(e.where, m.UID)
		if ls := e.levels[k.level]; ls != nil {
			if b := ls.buckets[k.start]; b != nil {
				b.kill(m.UID)
				if b.live <= 0 {
					delete(ls.buckets, k.start)
				}
			}
		}
	}
	return m, true
}

// ---- record application ----

// apply executes one record. It is the single application path for both live
// operations and replay, so live and recovered state are identical.
func (e *Engine) apply(rec store.Record) error {
	switch rec.Kind {
	case store.RecEnqueue:
		m, err := store.DecodeMessage(rec.Data)
		if err != nil {
			return err
		}
		e.now = rec.Time
		e.enqueueLocked(m)
		e.stats.Enqueued++
	case store.RecUpdate:
		m, err := store.DecodeMessage(rec.Data)
		if err != nil {
			return err
		}
		e.now = rec.Time
		e.rescheduleLocked(m)
		e.stats.Rescheduled++
	case store.RecCancel:
		uid, _, err := store.DecodeCancel(rec.Data)
		if err != nil {
			return err
		}
		e.cancelLocked(uid)
	case store.RecRuleCreate:
		ord, raw, err := store.DecodeRuleCreate(rec.Data)
		if err != nil {
			return err
		}
		r, err := calendar.UnmarshalRule(raw)
		if err != nil {
			return err
		}
		r.Ord = ord
		e.rules[r.ID] = &ruleState{rule: r, ruleOrd: ord}
		if ord >= e.ruleOrd {
			e.ruleOrd = ord + 1
		}
		e.stats.Rules++
	case store.RecRuleDelete:
		id, err := store.DecodeRuleDelete(rec.Data)
		if err != nil {
			return err
		}
		delete(e.rules, id)
	case store.RecAdvance:
		at, err := store.DecodeAdvance(rec.Data)
		if err != nil {
			return err
		}
		e.now = rec.Time
		e.advanceTo(at)
	default:
		return fmt.Errorf("engine: unknown record kind %d", rec.Kind)
	}
	return nil
}

func (e *Engine) enqueueLocked(m model.Message) {
	if _, ok := e.byID[m.ID]; ok {
		e.removeByID(m.ID)
	}
	e.trackCounters(m)
	if m.Due <= e.now {
		e.deliverNow(m)
		return
	}
	k, err := e.bucketFor(m.Due)
	if err != nil {
		// Unrepresentable dues are refused before they are logged; if one is
		// nevertheless replayed, keep it at level 0 rather than lose it.
		k = bucketKey{level: 0, start: e.geom.Levels()[0].SlotStart(e.geom.Epoch, m.Due)}
	}
	e.insertDirect(m, k)
}

func (e *Engine) rescheduleLocked(m model.Message) {
	e.removeByID(m.ID)
	e.trackCounters(m)
	if m.Due <= e.now {
		e.deliverNow(m)
		return
	}
	k, err := e.bucketFor(m.Due)
	if err != nil {
		k = bucketKey{level: 0, start: e.geom.Levels()[0].SlotStart(e.geom.Epoch, m.Due)}
	}
	e.insertDirect(m, k)
}

func (e *Engine) trackCounters(m model.Message) {
	if m.UID >= e.nextUID {
		e.nextUID = m.UID + 1
	}
	if m.Seq >= e.nextSeq {
		e.nextSeq = m.Seq + 1
	}
}

func (e *Engine) cancelLocked(uid uint64) {
	id, ok := e.idOfUID[uid]
	if !ok {
		return
	}
	m, ok := e.removeByID(id)
	if !ok {
		return
	}
	e.stats.Cancelled++
	if m.Rule != nil {
		e.completeOccurrence(m.Rule.RuleID, m.Rule.Index)
	}
}

// deliverNow delivers at the current clock. A due strictly in the past is
// counted separately as past due.
func (e *Engine) deliverNow(m model.Message) {
	if _, ok := e.byID[m.ID]; ok {
		e.removeByID(m.ID)
	}
	e.emit(Delivery{
		UID: m.UID, Seq: m.Seq, ID: m.ID, Due: m.Due, At: e.now,
		Priority: m.Priority, Class: m.Class, Payload: m.Payload,
		Rule: m.Rule, PastDue: m.Due < e.now,
	})
}

func (e *Engine) emit(d Delivery) {
	e.stats.Delivered++
	if d.PastDue {
		e.stats.PastDue++
	}
	size := cap(e.history)
	if size == 0 {
		size = 1
	}
	if len(e.history) < size {
		e.history = append(e.history, d)
	} else {
		e.history[e.histPos] = d
		e.histPos = (e.histPos + 1) % size
	}
	if e.sink != nil && !e.loading {
		e.sink(d)
	}
}

// ---- calendar rules ----

// ruleHorizon is the lead time at which a calendar occurrence is materialized:
// far enough ahead that it survives a cascade through every level.
func (e *Engine) ruleHorizon() int64 { return e.geom.Coarsest().Horizon() }

// materializationAt is the instant at which an occurrence must be materialized.
func (e *Engine) materializationAt(due int64) int64 {
	if at := due - e.ruleHorizon() + 1; at > e.now {
		return at
	}
	return e.now
}

// nextRuleEvent returns the earliest pending materialization instant.
func (e *Engine) nextRuleEvent() (int64, bool) {
	best, ok := int64(0), false
	for _, rs := range e.rules {
		if rs.pending {
			continue
		}
		occ, err := rs.rule.Next()
		if err != nil {
			continue
		}
		at := e.materializationAt(occ.Due)
		if !ok || at < best {
			best, ok = at, true
		}
	}
	return best, ok
}

// materializeDue materializes, one at a time and in (due, rule order,
// occurrence index) order, every occurrence whose materialization instant is at
// or before t. Each resulting message is an ordinary wheel message from then
// on: it is placed by the same level-selection rule and cascades like any
// other, so a clock jump over its due time delivers it exactly once.
func (e *Engine) materializeDue(t int64) {
	for e.materializeOnce(t) {
	}
}

func (e *Engine) materializeOnce(t int64) bool {
	var (
		best    *ruleState
		bestOcc calendar.Occurrence
		found   bool
	)
	for _, rs := range e.rules {
		if rs.pending {
			continue
		}
		occ, err := rs.rule.Next()
		if err != nil {
			continue
		}
		if e.materializationAt(occ.Due) > t {
			continue
		}
		if !found || occ.Due < bestOcc.Due ||
			(occ.Due == bestOcc.Due && rs.ruleOrd < best.ruleOrd) {
			best, bestOcc, found = rs, occ, true
		}
	}
	if !found {
		return false
	}
	rs, occ := best, bestOcc
	rs.rule.NextIdx = occ.Index
	rs.pending = true
	m := model.Message{
		UID: e.nextUID, Seq: e.nextSeq, ID: fmt.Sprintf("%s#%d", rs.rule.ID, occ.Index),
		Due: occ.Due, Priority: rs.rule.Priority, Class: model.ClassCalendar,
		Payload: rs.rule.Payload, Created: e.now,
		Rule: &model.Occurrence{
			RuleID: rs.rule.ID, RuleOrd: rs.ruleOrd,
			Index: occ.Index, Description: occ.Description,
		},
	}
	e.nextUID++
	e.nextSeq++
	if m.Due <= e.now {
		// Materialized after its due time (only possible for an occurrence
		// that became due during a large clock jump): deliver at once,
		// counted as past due, exactly like a late real-time message.
		e.emit(Delivery{
			UID: m.UID, Seq: m.Seq, ID: m.ID, Due: m.Due, At: e.now,
			Priority: m.Priority, Class: m.Class, Payload: m.Payload,
			Rule: m.Rule, PastDue: m.Due < e.now,
		})
		rs.pending = false
		e.completeOccurrenceAfter(rs, occ.Index)
		return true
	}
	k, err := e.bucketFor(m.Due)
	if err != nil {
		k = bucketKey{level: 0, start: e.geom.Levels()[0].SlotStart(e.geom.Epoch, m.Due)}
	}
	e.insertDirect(m, k)
	return true
}

// completeOccurrence advances a rule after one of its occurrences is consumed.
func (e *Engine) completeOccurrence(ruleID string, index int64) {
	if rs := e.rules[ruleID]; rs != nil {
		rs.pending = false
		e.completeOccurrenceAfter(rs, index)
	}
}

func (e *Engine) completeOccurrenceAfter(rs *ruleState, index int64) {
	if next := index + 1; next > rs.rule.NextIdx {
		rs.rule.NextIdx = next
	}
	// No queue to update: the next occurrence is found from NextIdx by
	// nextRuleEvent, and its due time is what schedules it.
}

// ---- event loop ----

func (e *Engine) nextInstant() (int64, bool) {
	best, ok := int64(0), false
	if t, yes := e.levelZeroInstant(); yes {
		best, ok = t, true
	}
	for l := 1; l < len(e.levels); l++ {
		ls := e.levels[l]
		for ls.starts.Len() > 0 {
			s := ls.starts[0]
			b := ls.buckets[s]
			if b == nil || b.live <= 0 {
				heap.Pop(&ls.starts)
				delete(ls.buckets, s)
				continue
			}
			if !ok || s < best {
				best, ok = s, true
			}
			break
		}
	}
	if t, yes := e.nextRuleEvent(); yes {
		if !ok || t < best {
			best, ok = t, true
		}
	}
	return best, ok
}

func (e *Engine) levelZeroInstant() (int64, bool) {
	var (
		best  int64
		found bool
	)
	for start, b := range e.levels[0].buckets {
		m, ok := b.peek()
		if !ok {
			delete(e.levels[0].buckets, start)
			continue
		}
		if !found || m.Due < best {
			best, found = m.Due, true
		}
	}
	return best, found
}

// cascade drains every bucket whose start is at or before t, coarsest level
// first. A finer level is visited later in the same pass, so a message that
// lands on a boundary at that instant is redistributed again before anything
// is delivered.
func (e *Engine) cascade(t int64) {
	for l := len(e.levels) - 1; l >= 1; l-- {
		ls := e.levels[l]
		for {
			for ls.starts.Len() > 0 {
				s := ls.starts[0]
				b := ls.buckets[s]
				if b != nil && b.live > 0 {
					break
				}
				heap.Pop(&ls.starts)
				delete(ls.buckets, s)
			}
			if ls.starts.Len() == 0 {
				break
			}
			s := ls.starts[0]
			if s > t {
				break
			}
			heap.Pop(&ls.starts)
			b := ls.buckets[s]
			if b != nil {
				delete(ls.buckets, s)
			}
			if b == nil {
				continue
			}
			msgs := b.drain()
			if len(msgs) == 0 {
				continue
			}
			e.stats.Cascades++
			for _, m := range msgs {
				delete(e.where, m.UID)
				k, err := e.bucketFor(m.Due)
				if err != nil || k.level >= l {
					// Never expected: a due inside a coarse bucket belongs to a
					// strictly finer level. Fall back to level 0 at its second
					// rather than lose the message.
					k = bucketKey{level: 0, start: e.geom.Levels()[0].SlotStart(e.geom.Epoch, m.Due)}
				}
				e.insertDirect(m, k)
			}
		}
	}
}

// deliverDue delivers level-0 messages due at or before t, always choosing the
// globally smallest key across buckets. Level-0 slots are one second wide while
// dues are nanosecond-precise, so ordering cannot be left to bucket layout.
func (e *Engine) deliverDue(t int64) {
	ls := e.levels[0]
	for {
		b, m, ok := e.zeroHead(t)
		if !ok {
			return
		}
		b.pop()
		delete(e.byID, m.ID)
		delete(e.idOfUID, m.UID)
		delete(e.where, m.UID)
		if b.live <= 0 {
			delete(ls.buckets, b.key.start)
		}
		e.emit(Delivery{
			UID: m.UID, Seq: m.Seq, ID: m.ID, Due: m.Due, At: m.Due,
			Priority: m.Priority, Class: m.Class, Payload: m.Payload, Rule: m.Rule,
		})
		if m.Rule != nil {
			e.completeOccurrence(m.Rule.RuleID, m.Rule.Index)
		}
	}
}

// advanceTo processes every event at or before target in chronological order,
// then moves the clock to target.
func (e *Engine) advanceTo(target int64) {
	if target <= e.now {
		return
	}
	for {
		t, ok := e.nextInstant()
		if !ok || t > target {
			break
		}
		if t < e.now {
			t = e.now
		}
		e.now = t
		e.materializeDue(t)
		e.cascade(t)
		e.deliverDue(t)
		if e.now >= target {
			return
		}
	}
	if target > e.now {
		e.stats.IdleJumps++
		e.stats.IdleNanos += target - e.now
		e.now = target
	}
}

// ---- public API ----

func (e *Engine) log(kind uint8, payload []byte) error {
	if e.st == nil {
		return nil
	}
	if _, err := e.st.Log().Append(kind, e.now, payload); err != nil {
		return err
	}
	e.sinceSnap++
	return nil
}

// commit logs a record (when durable) and applies it through the same path
// recovery uses.
func (e *Engine) commit(kind uint8, payload []byte) error {
	if err := e.log(kind, payload); err != nil {
		return err
	}
	rec := store.Record{Kind: kind, Time: e.now, Data: payload}
	if e.st != nil {
		rec.Seq = e.st.Log().Seq()
	}
	if err := e.apply(rec); err != nil {
		return err
	}
	e.maybeSnapshot()
	return nil
}

// Enqueue schedules a real-time message. A due at or before the clock is
// delivered immediately; a due strictly before it is also counted as past due.
// A due beyond the coarsest horizon is refused.
func (e *Engine) Enqueue(id string, due int64, priority int, payload string) (model.Message, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if id == "" {
		return model.Message{}, errors.New("engine: message id required")
	}
	if _, ok := e.byID[id]; ok {
		return model.Message{}, fmt.Errorf("%w: %q", ErrDuplicateID, id)
	}
	if due > e.now {
		if _, err := e.bucketFor(due); err != nil {
			return model.Message{}, err
		}
	}
	m := model.Message{
		UID: e.nextUID, Seq: e.nextSeq, ID: id, Due: due, Priority: priority,
		Class: model.ClassRealtime, Payload: payload, Created: e.now,
	}
	b, err := store.EncodeMessage(m)
	if err != nil {
		return model.Message{}, err
	}
	if err := e.commit(store.RecEnqueue, b); err != nil {
		return model.Message{}, err
	}
	return m, nil
}

// EnqueueAt schedules a message due at a wall-clock time.
func (e *Engine) EnqueueAt(id string, due time.Time, priority int, payload string) (model.Message, error) {
	return e.Enqueue(id, due.UnixNano(), priority, payload)
}

// Cancel removes a pending message. Cancelling a calendar occurrence consumes
// it, so its rule advances to the next occurrence.
func (e *Engine) Cancel(id string) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	m, ok := e.byID[id]
	if !ok {
		return fmt.Errorf("%w: %q", ErrNotFound, id)
	}
	return e.commit(store.RecCancel, store.CancelPayload(m.UID, id))
}

// Reschedule moves a pending message to a new due time and priority. The new
// instance receives a fresh sequence and UID, so (like adding it anew) it
// orders after messages already added at the destination instant, and a stale
// bucket entry for the old version can never be delivered.
func (e *Engine) Reschedule(id string, due int64, priority int) (model.Message, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	old, ok := e.byID[id]
	if !ok {
		return model.Message{}, fmt.Errorf("%w: %q", ErrNotFound, id)
	}
	if due > e.now {
		if _, err := e.bucketFor(due); err != nil {
			return model.Message{}, err
		}
	}
	m := old
	m.Due = due
	m.Priority = priority
	m.UID = e.nextUID
	m.Seq = e.nextSeq
	b, err := store.EncodeMessage(m)
	if err != nil {
		return model.Message{}, err
	}
	if err := e.commit(store.RecUpdate, b); err != nil {
		return model.Message{}, err
	}
	return m, nil
}

// AdvanceTo moves the virtual clock forward, delivering everything on the way.
func (e *Engine) AdvanceTo(target int64) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if target < e.now {
		return fmt.Errorf("%w: now %d, target %d", ErrClockBackwards, e.now, target)
	}
	if target == e.now {
		return nil
	}
	return e.commit(store.RecAdvance, store.AdvancePayload(target))
}

// AdvanceBy moves the clock by a duration.
func (e *Engine) AdvanceBy(d time.Duration) error {
	return e.AdvanceTo(e.Now() + int64(d))
}

// AddRule creates a calendar rule. Occurrences whose due time has already
// passed are skipped: a rule added today must not fire last month's reminder.
func (e *Engine) AddRule(r calendar.Rule) (calendar.Rule, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if r.ID == "" {
		return r, errors.New("engine: rule id required")
	}
	if _, ok := e.rules[r.ID]; ok {
		return r, fmt.Errorf("%w: %q", ErrDuplicateRule, r.ID)
	}
	if r.TimeOfDay == "" {
		r.TimeOfDay = "09:00"
	}
	if err := r.Validate(); err != nil {
		return r, err
	}
	r.Ord = e.ruleOrd
	r.Created = e.now
	r.NextIdx = 0
	// Skip occurrences already in the past.
	for {
		occ, err := r.Next()
		if err != nil {
			break
		}
		if occ.Due > e.now {
			break
		}
		r.NextIdx = occ.Index + 1
	}
	payload := store.RuleCreatePayload(r.Ord, r.Marshal())
	if err := e.commit(store.RecRuleCreate, payload); err != nil {
		return r, err
	}
	return r, nil
}

// DeleteRule removes a rule and any occurrence it has pending.
func (e *Engine) DeleteRule(id string) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	rs, ok := e.rules[id]
	if !ok {
		return fmt.Errorf("%w: %q", ErrRuleNotFound, id)
	}
	if rs.pending {
		occID := fmt.Sprintf("%s#%d", id, rs.rule.NextIdx)
		if m, ok := e.byID[occID]; ok {
			if err := e.commit(store.RecCancel, store.CancelPayload(m.UID, occID)); err != nil {
				return err
			}
		}
	}
	return e.commit(store.RecRuleDelete, store.RuleDeletePayload(id))
}

// ---- persistence ----

func (e *Engine) maybeSnapshot() {
	if e.st == nil || e.snapEvery <= 0 || e.sinceSnap < e.snapEvery {
		return
	}
	_ = e.snapshotLocked()
}

func (e *Engine) snapshotLocked() error {
	if e.st == nil {
		return nil
	}
	var buckets []store.BucketData
	for l, ls := range e.levels {
		for start, b := range ls.buckets {
			msgs := b.snapshotCopy()
			if len(msgs) == 0 {
				continue
			}
			buckets = append(buckets, store.BucketData{Level: l, Start: start, Msgs: msgs})
		}
	}
	rules := make([]calendar.Rule, 0, len(e.rules))
	for _, rs := range e.rules {
		rules = append(rules, rs.rule)
	}
	sort.Slice(rules, func(i, j int) bool { return rules[i].Ord < rules[j].Ord })
	statsJSON, _ := json.Marshal(e.stats)
	rulesJSON, _ := json.Marshal(rules)
	hdr := store.SnapshotHeader{
		Now: e.now, NextUID: e.nextUID, NextSeq: e.nextSeq, RuleOrd: e.ruleOrd,
		Stats: statsJSON, Rules: rulesJSON,
	}
	hb, err := json.Marshal(&hdr)
	if err != nil {
		return err
	}
	if err := e.st.SaveSnapshot(e.st.Log().Seq(), hb, buckets); err != nil {
		return err
	}
	e.sinceSnap = 0
	return nil
}

// ---- views ----

// Pending returns the number of pending messages.
func (e *Engine) Pending() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return len(e.byID)
}

// Stats returns the counters.
func (e *Engine) Stats() Stats {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.stats
}

// History returns up to n most recent deliveries, newest first.
func (e *Engine) History(n int) []Delivery {
	e.mu.Lock()
	defer e.mu.Unlock()
	size := len(e.history)
	if n <= 0 || n > size {
		n = size
	}
	out := make([]Delivery, 0, n)
	for i := 0; i < n; i++ {
		out = append(out, e.history[(e.histPos-1-i+size)%size])
	}
	return out
}

// RuleView is a calendar rule with its current state.
type RuleView struct {
	Rule       calendar.Rule         `json:"rule"`
	Pending    bool                  `json:"pending"`
	PendingID  string                `json:"pending_id,omitempty"`
	PendingDue int64                 `json:"pending_due"`
	Next       *calendar.Occurrence  `json:"next,omitempty"`
	Preview    []calendar.Occurrence `json:"preview"`
	// Anchor is the rule's month anchor as "2006-01"; monthly occurrence
	// indices count months from it.
	Anchor string `json:"anchor"`
}

// Rules returns all rules for display, ordered by creation.
func (e *Engine) Rules() []RuleView {
	e.mu.Lock()
	defer e.mu.Unlock()
	out := make([]RuleView, 0, len(e.rules))
	for _, rs := range e.rules {
		rv := RuleView{Rule: rs.rule, Pending: rs.pending, Anchor: rs.rule.AnchorMonth()}
		if occ, err := rs.rule.Next(); err == nil {
			o := occ
			rv.Next = &o
		}
		if rs.pending {
			id := fmt.Sprintf("%s#%d", rs.rule.ID, rs.rule.NextIdx)
			rv.PendingID = id
			if m, ok := e.byID[id]; ok {
				rv.PendingDue = m.Due
			}
		}
		rv.Preview, _ = rs.rule.Upcoming(6)
		out = append(out, rv)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Rule.Ord < out[j].Rule.Ord })
	return out
}

// BucketView is one occupied bucket.
type BucketView struct {
	Start int64 `json:"start"`
	Slot  int64 `json:"slot"`
	Count int   `json:"count"`
}

// LevelView is one wheel level for display.
type LevelView struct {
	Index      int          `json:"index"`
	Name       string       `json:"name"`
	Len        int64        `json:"len"`
	SpanSec    int64        `json:"span_sec"`
	HorizonSec int64        `json:"horizon_sec"`
	Count      int          `json:"count"`
	Buckets    []BucketView `json:"buckets"`
}

// WheelView renders the levels. When withBuckets is set, every occupied bucket
// is included so the page can draw the slot grid.
func (e *Engine) WheelView(withBuckets bool) []LevelView {
	e.mu.Lock()
	defer e.mu.Unlock()
	levels := e.geom.Levels()
	out := make([]LevelView, 0, len(levels))
	for i, l := range levels {
		lv := LevelView{
			Index: i, Name: l.Name, Len: l.Len,
			SpanSec:    int64(l.Span) / int64(time.Second),
			HorizonSec: l.Horizon() / int64(time.Second),
		}
		for start, b := range e.levels[i].buckets {
			n := b.live
			if n <= 0 {
				continue
			}
			lv.Count += n
			if withBuckets {
				lv.Buckets = append(lv.Buckets, BucketView{
					Start: start, Slot: (start - e.geom.Epoch) / l.Span, Count: n,
				})
			}
		}
		sort.Slice(lv.Buckets, func(a, b int) bool { return lv.Buckets[a].Start < lv.Buckets[b].Start })
		out = append(out, lv)
	}
	return out
}

// Upcoming returns pending messages ordered by key, up to n.
func (e *Engine) Upcoming(n int) []Delivery {
	e.mu.Lock()
	defer e.mu.Unlock()
	all := make([]model.Message, 0, len(e.byID))
	for _, m := range e.byID {
		all = append(all, m)
	}
	model.SortMessages(all)
	if n > 0 && len(all) > n {
		all = all[:n]
	}
	out := make([]Delivery, 0, len(all))
	for _, m := range all {
		out = append(out, Delivery{
			UID: m.UID, Seq: m.Seq, ID: m.ID, Due: m.Due, Priority: m.Priority,
			Class: m.Class, Payload: m.Payload, Rule: m.Rule,
		})
	}
	return out
}

// HorizonOf returns the coarsest level's horizon for a level spec list, which
// is the lead time calendar occurrences are materialized at.
func HorizonOf(levels []wheel.LevelSpec) int64 {
	var h int64 = 1
	for _, l := range levels {
		h = int64(l.Len) * int64(l.Span)
	}
	return h
}
