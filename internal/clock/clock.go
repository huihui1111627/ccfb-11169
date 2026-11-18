// Package clock provides real and virtual (fast-forwardable) time sources.
package clock

import (
	"sync/atomic"
	"time"
)

// Clock returns the current time in milliseconds since the epoch.
type Clock interface {
	Now() int64
}

// Real is the wall clock.
type Real struct{}

func (Real) Now() int64 { return time.Now().UnixMilli() }

// Virtual is a manually advanced clock, used to fast-forward time in tests
// and demos. It is safe for concurrent use.
type Virtual struct {
	now int64
}

func NewVirtual(startMs int64) *Virtual {
	return &Virtual{now: startMs}
}

func (v *Virtual) Now() int64 { return atomic.LoadInt64(&v.now) }

func (v *Virtual) Set(ms int64) { atomic.StoreInt64(&v.now, ms) }

// Advance moves the clock forward by deltaMs and returns the new value.
func (v *Virtual) Advance(deltaMs int64) int64 {
	return atomic.AddInt64(&v.now, deltaMs)
}
