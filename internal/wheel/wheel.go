// Package wheel implements the level geometry of a hierarchical timing wheel:
// absolute slot arithmetic, level selection and bucket identity.
//
// Level l has Len(l) slots of duration Span(l) each, so it covers
// Horizon(l) = Len(l)*Span(l) of time. The hierarchy is strict: the span of
// the next coarser slot must equal the total horizon of the finer level,
// Span(l+1) = Horizon(l). This is what lets every level's bucket be
// redistributed into the level below at the moment the bucket starts exactly
// one revolution of the finer level ahead, so a message is never held past its
// due time.
//
// Slots are aligned to the wheel epoch and identified by their absolute start
// time rather than by a slot index modulo Len. Absolute identity makes
// revolutions irrelevant: a bucket from an earlier revolution can never be
// confused with the bucket currently under the pointer, which in turn makes the
// on-disk slot files safe to load across restarts and long clock jumps.
package wheel

import (
	"errors"
	"fmt"
	"time"

	"kitewheel/internal/model"
)

// Class separates real-time messages from deterministic calendar occurrences.
// It is defined in the model package so storage and scheduling agree on it.
type Class = model.Class

// The ordering classes.
const (
	ClassRealtime = model.ClassRealtime
	ClassCalendar = model.ClassCalendar
)

// Message is a delayed message; see model.Message.
type Message = model.Message

// Occurrence links a calendar occurrence to its rule and index.
type Occurrence = model.Occurrence

// LevelSpec configures one level. Levels are given finest first.
type LevelSpec struct {
	Name string
	Len  int
	Span time.Duration
}

// Level is the resolved description of one level.
type Level struct {
	Index int    `json:"index"`
	Name  string `json:"name"`
	Len   int64  `json:"len"`
	Span  int64  `json:"span_ns"` // slot duration, nanoseconds
}

// Horizon returns the total time a level covers in one revolution.
func (l Level) Horizon() int64 { return l.Len * l.Span }

// SlotStart returns the absolute start of the slot containing due.
func (l Level) SlotStart(epoch, due int64) int64 {
	if due <= epoch {
		return epoch
	}
	return epoch + (due-epoch)/l.Span*l.Span
}

// HeadSlotStart returns the absolute start of the slot containing now.
func (l Level) HeadSlotStart(epoch, now int64) int64 {
	return l.SlotStart(epoch, now)
}

// NextBoundary returns the end of the slot starting at start.
func (l Level) NextBoundary(start int64) int64 { return start + l.Span }

// ErrNoLevel is returned when a due time is beyond the coarsest horizon.
var ErrNoLevel = errors.New("wheel: due time is beyond the coarsest level horizon")

// ErrZeroSpan is returned for a non-positive span.
var ErrZeroSpan = errors.New("wheel: level spans must be positive")

// Geometry is the immutable level hierarchy.
type Geometry struct {
	Epoch int64
	ls    []Level
}

// NewGeometry validates and resolves level specs.
//
// Requirements:
//   - at least one level;
//   - positive lengths and spans;
//   - Span(l+1) == Len(l)*Span(l) for every l (strict hierarchy).
func NewGeometry(epoch int64, specs []LevelSpec) (*Geometry, error) {
	if len(specs) == 0 {
		return nil, errors.New("wheel: at least one level required")
	}
	g := &Geometry{Epoch: epoch}
	for i, s := range specs {
		if s.Len < 1 {
			return nil, fmt.Errorf("wheel: level %d (%s) needs a positive slot count", i, s.Name)
		}
		if s.Span <= 0 {
			return nil, fmt.Errorf("wheel: level %d (%s) needs a positive span: %w", i, s.Name, ErrZeroSpan)
		}
		g.ls = append(g.ls, Level{Index: i, Name: s.Name, Len: int64(s.Len), Span: int64(s.Span)})
	}
	for i := 1; i < len(g.ls); i++ {
		fine, coarse := g.ls[i-1], g.ls[i]
		if coarse.Span != fine.Horizon() {
			return nil, fmt.Errorf(
				"wheel: level %d (%s) span %s must equal level %d (%s) horizon %s (a coarse slot must be exactly one revolution of the level below)",
				i, coarse.Name, time.Duration(coarse.Span), i-1, fine.Name, time.Duration(fine.Horizon()))
		}
	}
	return g, nil
}

// Levels returns a copy of the level list, finest first.
func (g *Geometry) Levels() []Level {
	out := make([]Level, len(g.ls))
	copy(out, g.ls)
	return out
}

// Coarsest returns the top level.
func (g *Geometry) Coarsest() Level { return g.ls[len(g.ls)-1] }

// MaxDue returns the latest due time the hierarchy can hold relative to now.
func (g *Geometry) MaxDue(now int64) (int64, bool) {
	h := g.Coarsest().Horizon()
	if now > int64(^uint64(0)>>1)-h {
		return 0, false
	}
	return now + h - 1, true
}

// LevelForDue returns the finest level whose horizon reaches due relative to
// now. A due no later than now maps to level 0 and must be delivered at once.
//
// The comparison is strict on the horizon: a due exactly Horizon(l) after now
// does not fit level l, so it goes to level l+1 and is cascaded at the
// boundary. That is what keeps boundary messages from being delivered one
// slot early or late.
func (g *Geometry) LevelForDue(now, due int64) (int, bool) {
	if due <= now {
		return 0, true
	}
	rel := due - now
	for _, l := range g.ls {
		if rel < l.Horizon() {
			return l.Index, true
		}
	}
	return 0, false
}

// BucketID identifies a bucket by level and absolute slot start.
type BucketID struct {
	Level int   `json:"level"`
	Start int64 `json:"start"` // absolute slot start, unix nanoseconds
}

// BucketKey builds the identity of the bucket holding due at level.
func (g *Geometry) BucketKey(level int, due int64) BucketID {
	return BucketID{Level: level, Start: g.ls[level].SlotStart(g.Epoch, due)}
}
