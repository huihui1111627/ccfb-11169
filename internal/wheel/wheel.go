// Package wheel implements a hierarchical timing wheel (seconds, minutes,
// hours, days by default). Higher layers cascade their buckets down to lower
// layers when they expire. Empty stretches of slots are skipped entirely:
// buckets live in a min-heap keyed by expiration time, so advancing the clock
// jumps directly to the next non-empty slot instead of ticking slot by slot.
package wheel

import (
	"container/heap"
	"sort"
)

// Entry is a schedulable item. Gen is a generation counter bumped by the
// owner on reschedule; stale entries sitting in buckets are skipped by the
// owner at delivery time.
type Entry struct {
	ID       uint64
	Gen      uint64
	Deadline int64 // absolute time, ms since epoch
}

// LayerConfig configures one layer of the wheel.
type LayerConfig struct {
	Name      string
	TickMs    int64 // span covered by one slot
	WheelSize int64 // number of slots; layer interval = TickMs * WheelSize
}

type bucket struct {
	vid     int64 // global slot id = deadline / tick (never aliases)
	key     int64 // when this bucket should be examined (ms)
	entries []*Entry
}

// heapItem orders buckets across ALL layers in one shared heap. Ordering is
// by key asc; on equal keys the HIGHER layer pops first, so cascades always
// land in lower-layer buckets before those buckets are delivered. This is
// what keeps global delivery order exactly (deadline, id).
type heapItem struct {
	key   int64
	vid   int64
	layer int
}

type slotHeap []heapItem

func (h slotHeap) Len() int { return len(h) }
func (h slotHeap) Less(i, j int) bool {
	if h[i].key != h[j].key {
		return h[i].key < h[j].key
	}
	return h[i].layer > h[j].layer // higher layer first on ties
}
func (h slotHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *slotHeap) Push(x interface{}) { *h = append(*h, x.(heapItem)) }
func (h *slotHeap) Pop() interface{} {
	old := *h
	n := len(old)
	it := old[n-1]
	*h = old[:n-1]
	return it
}

type layer struct {
	name     string
	tick     int64
	size     int64
	interval int64
	current  int64 // aligned down to tick
	buckets  map[int64]*bucket
}

// ErrStats accumulates signed timing errors in ms: negative = early,
// positive = late. For layer 0 it is recorded per delivered message
// (delivery time - deadline); for higher layers per cascaded bucket
// (cascade time - bucket expiration).
type ErrStats struct {
	Count int64
	Sum   int64
	Min   int64
	Max   int64
}

func (s *ErrStats) Add(err int64) {
	if s.Count == 0 || err < s.Min {
		s.Min = err
	}
	if s.Count == 0 || err > s.Max {
		s.Max = err
	}
	s.Count++
	s.Sum += err
}

// LayerView is a read-only snapshot of one layer for monitoring.
type LayerView struct {
	Name    string
	TickMs  int64
	Size    int64
	Current int64
	Buckets int // non-empty slots
	Pending int // entries held in this layer
	Stats   ErrStats
}

// Wheel is the hierarchical timing wheel.
type Wheel struct {
	layers []*layer
	stats  []ErrStats
	h      slotHeap // shared across layers
}

// New builds a wheel from layer configs ordered finest to coarsest.
func New(cfgs []LayerConfig, now int64) *Wheel {
	w := &Wheel{}
	for _, c := range cfgs {
		l := &layer{
			name:     c.Name,
			tick:     c.TickMs,
			size:     c.WheelSize,
			interval: c.TickMs * c.WheelSize,
			current:  (now / c.TickMs) * c.TickMs,
			buckets:  map[int64]*bucket{},
		}
		w.layers = append(w.layers, l)
	}
	w.stats = make([]ErrStats, len(cfgs))
	return w
}

// DefaultLayers returns seconds/minutes/hours/days layers.
func DefaultLayers() []LayerConfig {
	return []LayerConfig{
		{Name: "秒", TickMs: 1000, WheelSize: 60},
		{Name: "分", TickMs: 60 * 1000, WheelSize: 60},
		{Name: "时", TickMs: 3600 * 1000, WheelSize: 24},
		{Name: "天", TickMs: 24 * 3600 * 1000, WheelSize: 366},
	}
}

// Add inserts an entry. now is the current time in ms.
func (w *Wheel) Add(e *Entry, now int64) { w.add(0, e, now) }

func (w *Wheel) add(li int, e *Entry, now int64) {
	l := w.layers[li]
	if c := (now / l.tick) * l.tick; c > l.current {
		l.current = c
	}
	if e.Deadline < l.current+l.tick {
		// Falls inside the current tick of this layer: push down; at the
		// bottom layer it belongs to the current slot and is picked up by
		// the current/next Advance.
		if li == 0 {
			w.place(l, li, e)
			return
		}
		w.add(li-1, e, now)
		return
	}
	if li == len(w.layers)-1 || e.Deadline < l.current+l.interval {
		// Boundary-exact deadlines (deadline == current+interval) overflow
		// to the next layer instead of being squeezed into this one, so a
		// message is never placed a slot early or late.
		w.place(l, li, e)
		return
	}
	w.add(li+1, e, now)
}

func (w *Wheel) place(l *layer, li int, e *Entry) {
	vid := e.Deadline / l.tick
	b := l.buckets[vid]
	if b == nil {
		b = &bucket{vid: vid, key: vid * l.tick}
		l.buckets[vid] = b
		heap.Push(&w.h, heapItem{key: b.key, vid: vid, layer: li})
	}
	b.entries = append(b.entries, e)
	if e.Deadline < b.key {
		// A leftover bottom-layer bucket may have been re-keyed to its
		// earliest remaining deadline; a newly cascaded entry can be
		// earlier still. Lower the key so it is never delivered late.
		b.key = e.Deadline
		heap.Push(&w.h, heapItem{key: b.key, vid: vid, layer: li})
	}
}

// Advance moves the wheel to now and returns all entries with
// deadline <= now, ordered by (deadline, id). Empty slots are never
// visited: the shared heap jumps straight to the next non-empty bucket.
func (w *Wheel) Advance(now int64) []*Entry {
	var out []*Entry
	for len(w.h) > 0 && w.h[0].key <= now {
		it := heap.Pop(&w.h).(heapItem)
		l := w.layers[it.layer]
		if c := (now / l.tick) * l.tick; c > l.current {
			l.current = c
		}
		b := l.buckets[it.vid]
		if b == nil || b.key != it.key {
			continue // stale heap entry (bucket re-keyed or gone)
		}
		if it.layer == 0 {
			// Bottom layer: deliver only entries actually due; keep the
			// rest with a re-computed key so nothing is delivered early.
			entries := b.entries
			sort.Slice(entries, func(i, j int) bool {
				if entries[i].Deadline != entries[j].Deadline {
					return entries[i].Deadline < entries[j].Deadline
				}
				return entries[i].ID < entries[j].ID
			})
			cut := sort.Search(len(entries), func(i int) bool { return entries[i].Deadline > now })
			for _, e := range entries[:cut] {
				w.stats[0].Add(now - e.Deadline)
				out = append(out, e)
			}
			if cut < len(entries) {
				b.entries = entries[cut:]
				b.key = b.entries[0].Deadline
				heap.Push(&w.h, heapItem{key: b.key, vid: b.vid, layer: 0})
			} else {
				delete(l.buckets, b.vid)
			}
		} else {
			// Higher layer bucket expired: cascade every entry down.
			w.stats[it.layer].Add(now - b.key)
			delete(l.buckets, b.vid)
			for _, e := range b.entries {
				w.add(it.layer-1, e, now)
			}
		}
	}
	return out
}

// Views returns a snapshot of every layer for monitoring.
func (w *Wheel) Views() []LayerView {
	views := make([]LayerView, len(w.layers))
	for i, l := range w.layers {
		v := LayerView{
			Name:    l.name,
			TickMs:  l.tick,
			Size:    l.size,
			Current: l.current,
			Buckets: len(l.buckets),
			Stats:   w.stats[i],
		}
		for _, b := range l.buckets {
			v.Pending += len(b.entries)
		}
		views[i] = v
	}
	return views
}

// Pending returns the total number of entries held in the wheel.
func (w *Wheel) Pending() int64 {
	var n int64
	for _, l := range w.layers {
		for _, b := range l.buckets {
			n += int64(len(b.entries))
		}
	}
	return n
}
