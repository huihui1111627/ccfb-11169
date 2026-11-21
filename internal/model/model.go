// Package model holds the value types shared by the wheel, the store and the
// scheduler, so storage and scheduling agree on identity and ordering without
// importing each other.
package model

import (
	"sort"
	"strconv"
)

// Class separates real-time messages from deterministic calendar occurrences.
// It is a tie-break after the due instant and priority, so a real-time message
// and a calendar occurrence due at the same instant with the same priority are
// ordered real-time first regardless of when either was scheduled. Values are
// ascending.
type Class uint8

const (
	// ClassRealtime is a message inserted explicitly by a user or system.
	ClassRealtime Class = iota
	// ClassCalendar is one occurrence materialized from a calendar rule.
	ClassCalendar
)

// String returns the class name.
func (c Class) String() string {
	switch c {
	case ClassRealtime:
		return "realtime"
	case ClassCalendar:
		return "calendar"
	default:
		return "class-" + strconv.Itoa(int(c))
	}
}

// Occurrence links a calendar occurrence to its rule, its position in the
// rule's sequence, and the rule's creation ordinal.
type Occurrence struct {
	RuleID      string `json:"rule_id"`
	RuleOrd     uint64 `json:"rule_ord"` // creation order of the rule
	Index       int64  `json:"index"`    // occurrence index within the rule
	Description string `json:"description,omitempty"`
}

// Message is one delayed message.
type Message struct {
	UID      uint64      `json:"uid"`      // identity of this instance; changes on reschedule
	Seq      uint64      `json:"seq"`      // insertion order; preserved across reschedule
	ID       string      `json:"id"`       // user-facing message number, unique among pending
	Due      int64       `json:"due"`      // unix nanoseconds
	Priority int         `json:"priority"` // larger delivers first within a due instant
	Class    Class       `json:"class"`
	Payload  string      `json:"payload"`
	Rule     *Occurrence `json:"rule,omitempty"` // set for calendar occurrences
	Created  int64       `json:"created"`
}

// Key is the total ordering used inside a bucket and by the reference
// scheduler. Priority comes before insertion order, as required: among messages
// due at the same instant the higher priority goes first, and equal priorities
// go in the order they were added. "Added" means:
//
//   - real-time messages: their scheduling sequence, which is not changed by a
//     reschedule (a rescheduled message keeps its place among messages that
//     were added at the same time);
//   - calendar occurrences: the rule's creation ordinal, then the occurrence
//     index, so the order does not depend on when the scheduler happened to
//     materialize the occurrence.
//
// Class separates real-time from calendar at equal due and priority. UID is
// unique and is the final tie-break, making the order total.
type Key struct {
	Due      int64
	Priority int
	Class    Class
	Tie      uint64 // rule creation ordinal, or 0
	Sub      int64  // occurrence index, or 0
	Seq      uint64
	UID      uint64
}

// KeyOf extracts the ordering key of a message.
func KeyOf(m Message) Key {
	k := Key{Due: m.Due, Priority: m.Priority, Class: m.Class, Seq: m.Seq, UID: m.UID}
	if m.Rule != nil {
		k.Tie = m.Rule.RuleOrd
		k.Sub = m.Rule.Index
	}
	return k
}

// Less reports whether k orders before o.
func (k Key) Less(o Key) bool {
	if k.Due != o.Due {
		return k.Due < o.Due
	}
	if k.Priority != o.Priority {
		return k.Priority > o.Priority
	}
	if k.Class != o.Class {
		return k.Class < o.Class
	}
	if k.Tie != o.Tie {
		return k.Tie < o.Tie
	}
	if k.Sub != o.Sub {
		return k.Sub < o.Sub
	}
	if k.Seq != o.Seq {
		return k.Seq < o.Seq
	}
	return k.UID < o.UID
}

// SortMessages orders a slice by delivery key.
func SortMessages(msgs []Message) {
	sort.Slice(msgs, func(i, j int) bool { return KeyOf(msgs[i]).Less(KeyOf(msgs[j])) })
}
