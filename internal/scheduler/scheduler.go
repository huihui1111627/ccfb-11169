// Package scheduler ties the hierarchical timing wheel, the persistent
// operation log and the delivery hook together, and exposes monitoring
// snapshots for the dashboard.
package scheduler

import (
	"fmt"
	"sort"
	"sync"
	"time"

	"kitefestival/delaymsg/internal/clock"
	"kitefestival/delaymsg/internal/recur"
	"kitefestival/delaymsg/internal/store"
	"kitefestival/delaymsg/internal/wheel"
)

// MsgState is the lifecycle state of a message.
type MsgState int

const (
	Scheduled MsgState = iota // sitting in the wheel
	Delivered                 // fired, waiting for Ack
)

func (s MsgState) String() string {
	if s == Scheduled {
		return "待投递"
	}
	return "已投递待确认"
}

// Msg is one delayed message.
type Msg struct {
	ID          uint64
	Gen         uint64 // bumped on every reschedule; stale wheel entries are skipped
	Payload     string
	Deadline    int64
	RuleRaw     *recur.Rule
	Rule        *recur.Compiled
	State       MsgState
	DeliveredAt int64
}

// DeliveryRecord is one fired message, kept for the "已投递" view.
type DeliveryRecord struct {
	ID        uint64
	Payload   string
	Deadline  int64
	At        int64
	ErrMs     int64 // At - Deadline: negative = early, positive = late
	Immediate bool  // deadline was already past when added/rescheduled
	Acked     bool
}

// Stats is the aggregate counters block.
type Stats struct {
	Scheduled    int64 // currently in the wheel
	Delivered    int64 // total fired
	Unacked      int64 // delivered, awaiting ack
	Cancelled    int64
	Immediate    int64 // fired on the spot because deadline was past
	LogOps       int64
	LogBytes     int64
	Compactions  int64
	WheelPending int64
}

// Snapshot is the full monitoring view consumed by the dashboard.
type Snapshot struct {
	Now       int64
	Layers    []wheel.LayerView
	Soon      []MsgView
	Delivered []DeliveryRecord
	Rules     []RuleView
	Stats     Stats
}

// MsgView is a pending message in the "即将到期" list.
type MsgView struct {
	ID       uint64
	Payload  string
	Deadline int64
	InMs     int64
	Recur    bool
}

// RuleView is a live recurring rule with its next firing.
type RuleView struct {
	ID      uint64
	Payload string
	Rule    string
	Next    int64
}

// Config tunes the scheduler.
type Config struct {
	Layers []wheel.LayerConfig
	// CompactMinOps: compaction is considered once the log grew by at
	// least this many records since the last rewrite.
	CompactMinOps int64
	// Location for calendar rules; defaults to time.Local.
	Loc *time.Location
}

// Scheduler is the delayed-message service core.
type Scheduler struct {
	mu    sync.Mutex
	clock clock.Clock
	wheel *wheel.Wheel
	log   *store.Log
	loc   *time.Location
	hook  func(m *Msg, now int64)

	msgs   map[uint64]*Msg
	nextID uint64

	recent   []DeliveryRecord // newest first, capped
	recentCap int

	immediate   int64
	cancelled   int64
	delivered   int64
	compactions int64
	compactMin  int64
}

// New opens (or recovers) a scheduler on top of the given log file.
// hook is called for every delivery, synchronously, in delivery order.
func New(c clock.Clock, logPath string, cfg Config, hook func(m *Msg, now int64)) (*Scheduler, error) {
	if cfg.Layers == nil {
		cfg.Layers = wheel.DefaultLayers()
	}
	if cfg.Loc == nil {
		cfg.Loc = time.Local
	}
	if cfg.CompactMinOps <= 0 {
		cfg.CompactMinOps = 100000
	}
	lg, err := store.Open(logPath)
	if err != nil {
		return nil, err
	}
	s := &Scheduler{
		clock:      c,
		log:        lg,
		loc:        cfg.Loc,
		hook:       hook,
		msgs:       map[uint64]*Msg{},
		recentCap:  200,
		compactMin: cfg.CompactMinOps,
	}
	if err := s.recover(); err != nil {
		lg.Close()
		return nil, err
	}
	s.wheel = wheel.New(cfg.Layers, c.Now())
	now := c.Now()
	for _, m := range s.msgs {
		if m.State == Scheduled {
			s.wheel.Add(&wheel.Entry{ID: m.ID, Gen: m.Gen, Deadline: m.Deadline}, now)
		}
	}
	return s, nil
}

// recover replays the operation log into the in-memory message table.
func (s *Scheduler) recover() error {
	return s.log.Replay(func(r store.Record) error {
		switch r.Op {
		case store.OpAdd:
			m := &Msg{
				ID:       r.ID,
				Gen:      r.Gen,
				Payload:  r.Payload,
				Deadline: r.Deadline,
				RuleRaw:  r.Rule,
				State:    Scheduled,
			}
			if r.Rule != nil {
				c, err := recur.Compile(r.Rule, s.loc)
				if err != nil {
					return fmt.Errorf("日志中消息 %d 的周期规则无法解析: %w", r.ID, err)
				}
				m.Rule = c
			}
			s.msgs[r.ID] = m
			if r.ID >= s.nextID {
				s.nextID = r.ID + 1
			}
		case store.OpCancel:
			delete(s.msgs, r.ID)
		case store.OpReschedule:
			if m, ok := s.msgs[r.ID]; ok {
				m.Gen = r.Gen
				m.Deadline = r.Deadline
				m.State = Scheduled
			}
		case store.OpDelivered:
			if m, ok := s.msgs[r.ID]; ok {
				m.State = Delivered
			}
		case store.OpAck:
			delete(s.msgs, r.ID)
		}
		return nil
	})
}

// Add schedules a one-shot message at deadline (ms epoch), or a recurring
// one when rule is non-nil (deadline is then ignored and computed from the
// rule). Invalid rules are rejected here, before anything is stored.
func (s *Scheduler) Add(payload string, deadline int64, rule *recur.Rule) (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.clock.Now()
	var compiled *recur.Compiled
	if rule != nil {
		c, err := recur.Compile(rule, s.loc)
		if err != nil {
			return 0, err
		}
		next, ok := c.NextAfter(now)
		if !ok {
			return 0, fmt.Errorf("周期规则在未来两年内没有触发时间")
		}
		compiled = c
		deadline = next
	}
	id := s.nextID
	s.nextID++
	m := &Msg{
		ID:       id,
		Payload:  payload,
		Deadline: deadline,
		RuleRaw:  rule,
		Rule:     compiled,
		State:    Scheduled,
	}
	s.msgs[id] = m
	s.log.Append(store.Record{Op: store.OpAdd, ID: id, Deadline: deadline, Payload: payload, Rule: rule})
	if deadline < now {
		// Past-due on arrival: fire immediately, counted separately.
		s.deliverLocked(m, now, true)
	} else {
		s.wheel.Add(&wheel.Entry{ID: id, Deadline: deadline}, now)
	}
	return id, nil
}

// Cancel removes a pending message by id. Returns false if the message is
// unknown or already delivered.
func (s *Scheduler) Cancel(id uint64) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	m, ok := s.msgs[id]
	if !ok || m.State != Scheduled {
		return false
	}
	delete(s.msgs, id)
	s.cancelled++
	s.log.Append(store.Record{Op: store.OpCancel, ID: id})
	return true
}

// Reschedule moves a pending message to a new deadline. The old wheel entry
// is invalidated via the generation counter, so a cross-layer move never
// fires twice. Returns false if the message is not pending.
func (s *Scheduler) Reschedule(id uint64, newDeadline int64) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	m, ok := s.msgs[id]
	if !ok || m.State != Scheduled {
		return false
	}
	now := s.clock.Now()
	m.Gen++
	m.Deadline = newDeadline
	s.log.Append(store.Record{Op: store.OpReschedule, ID: id, Gen: m.Gen, Deadline: newDeadline})
	if newDeadline < now {
		s.deliverLocked(m, now, true)
	} else {
		s.wheel.Add(&wheel.Entry{ID: id, Gen: m.Gen, Deadline: newDeadline}, now)
	}
	return true
}

// Ack confirms a delivered message; it is dropped from the log on the next
// compaction. Returns false if the message is not awaiting ack.
func (s *Scheduler) Ack(id uint64) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	m, ok := s.msgs[id]
	if !ok || m.State != Delivered {
		return false
	}
	delete(s.msgs, id)
	s.log.Append(store.Record{Op: store.OpAck, ID: id})
	return true
}

// Tick advances the wheel to the current clock time and delivers everything
// due. Call it regularly (or after every virtual-clock jump).
func (s *Scheduler) Tick() {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.clock.Now()
	for _, e := range s.wheel.Advance(now) {
		m, ok := s.msgs[e.ID]
		if !ok || m.State != Scheduled || m.Gen != e.Gen {
			continue // cancelled or superseded by a reschedule
		}
		s.deliverLocked(m, now, false)
	}
	s.maybeCompactLocked()
}

// deliverLocked fires one message. For recurring rules it immediately
// computes and schedules the next occurrence.
func (s *Scheduler) deliverLocked(m *Msg, now int64, immediate bool) {
	m.State = Delivered
	m.DeliveredAt = now
	s.delivered++
	if immediate {
		s.immediate++
	}
	s.log.Append(store.Record{Op: store.OpDelivered, ID: m.ID, Gen: m.Gen})
	s.pushRecent(DeliveryRecord{
		ID: m.ID, Payload: m.Payload, Deadline: m.Deadline,
		At: now, ErrMs: now - m.Deadline, Immediate: immediate,
	})
	if s.hook != nil {
		s.hook(m, now)
	}
	if m.Rule != nil {
		if next, ok := m.Rule.NextAfter(now); ok {
			m.Gen++
			m.Deadline = next
			m.State = Scheduled
			s.log.Append(store.Record{Op: store.OpReschedule, ID: m.ID, Gen: m.Gen, Deadline: next})
			s.wheel.Add(&wheel.Entry{ID: m.ID, Gen: m.Gen, Deadline: next}, now)
		}
	}
}

func (s *Scheduler) pushRecent(d DeliveryRecord) {
	s.recent = append([]DeliveryRecord{d}, s.recent...)
	if len(s.recent) > s.recentCap {
		s.recent = s.recent[:s.recentCap]
	}
}

// maybeCompactLocked rewrites the log once it has grown to more than twice
// the live state (and past the minimum-op threshold).
func (s *Scheduler) maybeCompactLocked() {
	ops := s.log.Ops()
	if ops >= s.compactMin && ops > 2*int64(len(s.msgs)) {
		s.compactLocked()
	}
}

// Compact forces a log compaction: only pending and delivered-unacked
// messages survive.
func (s *Scheduler) Compact() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.compactLocked()
}

func (s *Scheduler) compactLocked() {
	recs := make([]store.Record, 0, len(s.msgs))
	for _, m := range s.msgs {
		recs = append(recs, store.Record{
			Op: store.OpAdd, ID: m.ID, Gen: m.Gen,
			Deadline: m.Deadline, Payload: m.Payload, Rule: m.RuleRaw,
		})
		if m.State == Delivered {
			recs = append(recs, store.Record{Op: store.OpDelivered, ID: m.ID, Gen: m.Gen})
		}
	}
	if err := s.log.Rewrite(recs); err == nil {
		s.compactions++
	}
}

// Flush forces the log to disk.
func (s *Scheduler) Flush() error { return s.log.Flush() }

// Close flushes and closes the log.
func (s *Scheduler) Close() error { return s.log.Close() }

// LogPath returns the log file path (used by tests to re-open).
func (s *Scheduler) LogOps() int64 { return s.log.Ops() }

// Snapshot builds the monitoring view.
func (s *Scheduler) Snapshot() Snapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.clock.Now()
	snap := Snapshot{
		Now:    now,
		Layers: s.wheel.Views(),
	}
	var soon []*Msg
	var rules []RuleView
	var unacked int64
	for _, m := range s.msgs {
		switch m.State {
		case Scheduled:
			soon = append(soon, m)
			if m.Rule != nil {
				rules = append(rules, RuleView{
					ID: m.ID, Payload: m.Payload,
					Rule: m.Rule.Describe(), Next: m.Deadline,
				})
			}
		case Delivered:
			unacked++
		}
	}
	sort.Slice(soon, func(i, j int) bool {
		if soon[i].Deadline != soon[j].Deadline {
			return soon[i].Deadline < soon[j].Deadline
		}
		return soon[i].ID < soon[j].ID
	})
	if len(soon) > 50 {
		soon = soon[:50]
	}
	for _, m := range soon {
		snap.Soon = append(snap.Soon, MsgView{
			ID: m.ID, Payload: m.Payload, Deadline: m.Deadline,
			InMs: m.Deadline - now, Recur: m.Rule != nil,
		})
	}
	sort.Slice(rules, func(i, j int) bool { return rules[i].Next < rules[j].Next })
	if len(rules) > 50 {
		rules = rules[:50]
	}
	snap.Rules = rules
	snap.Delivered = append(snap.Delivered, s.recent...)
	snap.Stats = Stats{
		Scheduled:    s.wheel.Pending(),
		Delivered:    s.delivered,
		Unacked:      unacked,
		Cancelled:    s.cancelled,
		Immediate:    s.immediate,
		LogOps:       s.log.Ops(),
		LogBytes:     s.log.Size(),
		Compactions:  s.compactions,
		WheelPending: s.wheel.Pending(),
	}
	return snap
}
