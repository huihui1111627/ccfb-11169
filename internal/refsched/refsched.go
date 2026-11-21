// Package refsched is an independent reference scheduler used to verify the
// hierarchical wheel. It keeps every pending message in one min-heap ordered by
// model.Key and never uses wheel geometry, so a disagreement between the two
// points at a defect in the wheel rather than in shared logic.
//
// It is deliberately simple: correctness over speed.
//
// Semantics it implements (the same the engine documents):
//
//   - a message due at or before the clock at insertion time is delivered
//     immediately; strictly earlier dues are marked past due;
//   - pending messages are delivered at their due time, ordered by
//     model.Key (due, priority desc, class, insertion order, uid);
//   - clock advances deliver everything due at or before the target, in that
//     order, and never move backwards;
//   - calendar occurrences are delivered at their due time like any other
//     message, ordered by rule creation order then occurrence index among
//     equal dues; an occurrence already past due when first materialized is
//     delivered immediately at the materialization instant, marked past due.
package refsched

import (
	"container/heap"
	"fmt"
	"sort"

	"kitewheel/internal/calendar"
	"kitewheel/internal/model"
)

// Delivery is one reference delivery.
type Delivery struct {
	ID      string
	Due     int64
	At      int64
	PastDue bool
	Class   model.Class
}

type item struct {
	m    model.Message
	dead bool
}

type msgHeap []*item

func (h msgHeap) Len() int           { return len(h) }
func (h msgHeap) Less(i, j int) bool { return model.KeyOf(h[i].m).Less(model.KeyOf(h[j].m)) }
func (h msgHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *msgHeap) Push(x any)        { *h = append(*h, x.(*item)) }
func (h *msgHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[:n-1]
	return x
}

type ruleState struct {
	rule    calendar.Rule
	ord     uint64
	pending bool
}

// Scheduler is the reference scheduler.
type Scheduler struct {
	now     int64
	nextUID uint64
	nextSeq uint64
	ruleOrd uint64
	horizon int64

	heap  msgHeap
	byID  map[string]*item
	rules map[string]*ruleState

	// Out is the delivery stream in order.
	Out []Delivery
}

// New creates a reference scheduler at the given virtual time. horizon is the
// lead time at which calendar occurrences are materialized; it must match the
// wheel's coarsest horizon.
func New(now int64, horizon int64) *Scheduler {
	return &Scheduler{
		now:     now,
		nextUID: 1,
		nextSeq: 1,
		horizon: horizon,
		byID:    map[string]*item{},
		rules:   map[string]*ruleState{},
	}
}

// Now returns the virtual clock.
func (s *Scheduler) Now() int64 { return s.now }

// Pending returns the number of pending messages.
func (s *Scheduler) Pending() int { return len(s.byID) }

func (s *Scheduler) insert(m model.Message) {
	it := &item{m: m}
	heap.Push(&s.heap, it)
	s.byID[m.ID] = it
	if m.UID >= s.nextUID {
		s.nextUID = m.UID + 1
	}
	if m.Seq >= s.nextSeq {
		s.nextSeq = m.Seq + 1
	}
}

func (s *Scheduler) remove(it *item) {
	delete(s.byID, it.m.ID)
	it.dead = true
}

// Enqueue adds a real-time message.
func (s *Scheduler) Enqueue(id string, due int64, priority int, payload string) error {
	if _, ok := s.byID[id]; ok {
		return fmt.Errorf("refsched: duplicate id %q", id)
	}
	if due > s.now && due > s.now+s.horizon-1 {
		return fmt.Errorf("refsched: due beyond horizon")
	}
	m := model.Message{
		UID: s.nextUID, Seq: s.nextSeq, ID: id, Due: due, Priority: priority,
		Class: model.ClassRealtime, Payload: payload, Created: s.now,
	}
	s.nextUID++
	s.nextSeq++
	if due <= s.now {
		s.emit(Delivery{ID: id, Due: due, At: s.now, PastDue: due < s.now, Class: model.ClassRealtime})
		return nil
	}
	s.insert(m)
	return nil
}

// Cancel removes a pending message, consuming a calendar occurrence.
func (s *Scheduler) Cancel(id string) error {
	it, ok := s.byID[id]
	if !ok {
		return fmt.Errorf("refsched: unknown id %q", id)
	}
	s.remove(it)
	if it.m.Rule != nil {
		s.consume(it.m.Rule.RuleID, it.m.Rule.Index)
	}
	return nil
}

// Reschedule moves a pending message.
func (s *Scheduler) Reschedule(id string, due int64, priority int) error {
	old, ok := s.byID[id]
	if !ok {
		return fmt.Errorf("refsched: unknown id %q", id)
	}
	if due > s.now && due > s.now+s.horizon-1 {
		return fmt.Errorf("refsched: due beyond horizon")
	}
	m := old.m
	m.Due = due
	m.Priority = priority
	m.UID = s.nextUID
	m.Seq = s.nextSeq
	s.nextUID++
	s.nextSeq++
	s.remove(old)
	if due <= s.now {
		s.emit(Delivery{ID: id, Due: due, At: s.now, PastDue: due < s.now, Class: m.Class})
		if m.Rule != nil {
			s.consume(m.Rule.RuleID, m.Rule.Index)
		}
		return nil
	}
	s.insert(m)
	return nil
}

// AddRule adds a calendar rule, skipping occurrences already in the past.
func (s *Scheduler) AddRule(r calendar.Rule) error {
	if _, ok := s.rules[r.ID]; ok {
		return fmt.Errorf("refsched: duplicate rule %q", r.ID)
	}
	if r.TimeOfDay == "" {
		r.TimeOfDay = "09:00"
	}
	if err := r.Validate(); err != nil {
		return err
	}
	r.Ord = s.ruleOrd
	r.Created = s.now
	r.NextIdx = 0
	for {
		occ, err := r.Next()
		if err != nil {
			break
		}
		if occ.Due > s.now {
			break
		}
		r.NextIdx = occ.Index + 1
	}
	s.rules[r.ID] = &ruleState{rule: r, ord: s.ruleOrd}
	s.ruleOrd++
	return nil
}

// DeleteRule deletes a rule and its pending occurrence.
func (s *Scheduler) DeleteRule(id string) error {
	rs, ok := s.rules[id]
	if !ok {
		return fmt.Errorf("refsched: unknown rule %q", id)
	}
	if rs.pending {
		if it, ok := s.byID[fmt.Sprintf("%s#%d", id, rs.rule.NextIdx)]; ok {
			s.remove(it)
		}
	}
	delete(s.rules, id)
	return nil
}

func (s *Scheduler) emit(d Delivery) { s.Out = append(s.Out, d) }

func (s *Scheduler) consume(ruleID string, index int64) {
	rs := s.rules[ruleID]
	if rs == nil {
		return
	}
	rs.pending = false
	if index+1 > rs.rule.NextIdx {
		rs.rule.NextIdx = index + 1
	}
}

// materializationAt mirrors the engine's lead time.
func (s *Scheduler) materializationAt(due int64) int64 {
	if at := due - s.horizon + 1; at > s.now {
		return at
	}
	return s.now
}

// materializeOne materializes the earliest not-yet-pending occurrence whose
// materialization instant is at or before target, ordered by due then rule
// creation order. It reports whether it did anything.
func (s *Scheduler) materializeOne(target int64) bool {
	var (
		bestRs   *ruleState
		bestOcc  calendar.Occurrence
		bestSort [3]uint64
		found    bool
	)
	for _, rs := range s.rules {
		if rs.pending {
			continue
		}
		occ, err := rs.rule.Next()
		if err != nil || s.materializationAt(occ.Due) > target {
			continue
		}
		sortKey := [3]uint64{uint64(occ.Due), rs.ord, uint64(occ.Index)}
		if !found || lessTriple(sortKey, bestSort) {
			bestRs, bestOcc, bestSort, found = rs, occ, sortKey, true
		}
	}
	if !found {
		return false
	}
	rs, occ := bestRs, bestOcc
	rs.rule.NextIdx = occ.Index
	rs.pending = true
	m := model.Message{
		UID: s.nextUID, Seq: s.nextSeq,
		ID:  fmt.Sprintf("%s#%d", rs.rule.ID, occ.Index),
		Due: occ.Due, Priority: rs.rule.Priority, Class: model.ClassCalendar,
		Payload: rs.rule.Payload, Created: s.now,
		Rule: &model.Occurrence{
			RuleID: rs.rule.ID, RuleOrd: rs.ord,
			Index: occ.Index, Description: occ.Description,
		},
	}
	s.nextUID++
	s.nextSeq++
	if occ.Due <= s.now {
		s.emit(Delivery{ID: m.ID, Due: occ.Due, At: s.now, PastDue: occ.Due < s.now, Class: model.ClassCalendar})
		rs.pending = false
		s.consume(rs.rule.ID, occ.Index)
		return true
	}
	s.insert(m)
	return true
}

func lessTriple(a, b [3]uint64) bool {
	for i := range a {
		if a[i] != b[i] {
			return a[i] < b[i]
		}
	}
	return false
}

// materialize inserts all occurrences due at or before target.
func (s *Scheduler) materialize(target int64) {
	for s.materializeOne(target) {
	}
}

// AdvanceTo moves the clock, delivering everything due, in heap order.
func (s *Scheduler) AdvanceTo(target int64) error {
	if target < s.now {
		return fmt.Errorf("refsched: clock cannot move backwards")
	}
	for {
		s.materialize(target)
		if s.heap.Len() == 0 {
			break
		}
		top := s.heap[0]
		if top.m.Due > target {
			break
		}
		heap.Pop(&s.heap)
		if top.dead {
			continue
		}
		delete(s.byID, top.m.ID)
		s.emit(Delivery{ID: top.m.ID, Due: top.m.Due, At: top.m.Due, Class: top.m.Class, PastDue: false})
		if top.m.Rule != nil {
			s.consume(top.m.Rule.RuleID, top.m.Rule.Index)
		}
	}
	s.now = target
	return nil
}

// RuleIDs returns the rule ids in creation order.
func (s *Scheduler) RuleIDs() []string {
	type pair struct {
		id  string
		ord uint64
	}
	ps := make([]pair, 0, len(s.rules))
	for id, rs := range s.rules {
		ps = append(ps, pair{id, rs.ord})
	}
	sort.Slice(ps, func(i, j int) bool { return ps[i].ord < ps[j].ord })
	out := make([]string, len(ps))
	for i, p := range ps {
		out[i] = p.id
	}
	return out
}
