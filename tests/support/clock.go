package support

import (
	"sync"
	"time"
)

// FakeClock is a goroutine-safe, manually advanced clock for detector,
// orphan-sweep, and repair-backoff tests. It is not a scheduler: advancing the
// clock changes the time reported to injected Now functions but does not fire
// timers. Pair it with WaitFor to synchronize on observable state.
type FakeClock struct {
	mu  sync.RWMutex
	now time.Time
}

// NewFakeClock returns a clock frozen at start. A zero start time is replaced
// with a fixed, non-zero instant so zero-value checks in production code remain
// meaningful.
func NewFakeClock(start time.Time) *FakeClock {
	if start.IsZero() {
		start = time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC)
	}
	return &FakeClock{now: start}
}

// Now returns the current fake time and is safe to pass to DetectorOptions.Now.
func (c *FakeClock) Now() time.Time {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.now
}

// Advance moves the clock forward by d.
func (c *FakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(d)
	c.mu.Unlock()
}

// Set replaces the current fake time.
func (c *FakeClock) Set(t time.Time) {
	c.mu.Lock()
	c.now = t
	c.mu.Unlock()
}
