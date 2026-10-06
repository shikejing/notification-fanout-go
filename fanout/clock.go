package fanout

import (
	"context"
	"sort"
	"sync"
	"time"
)

// Clock abstracts time so scheduling and backoff can be tested
// deterministically.
type Clock interface {
	Now() time.Time
	// After returns a channel that fires after d. The returned Timer can be
	// stopped; a stopped timer never fires.
	After(d time.Duration) Timer
}

// Timer is a cancellable timer returned by Clock.After.
type Timer interface {
	C() <-chan time.Time
	Stop() bool
}

// ---------------------------------------------------------------------------
// Real clock
// ---------------------------------------------------------------------------

// RealClock uses the wall clock.
type RealClock struct{}

// NewRealClock returns a wall-clock backed Clock.
func NewRealClock() *RealClock { return &RealClock{} }

func (c *RealClock) Now() time.Time { return time.Now() }

func (c *RealClock) After(d time.Duration) Timer {
	return &realTimer{t: time.NewTimer(d)}
}

type realTimer struct{ t *time.Timer }

func (r *realTimer) C() <-chan time.Time { return r.t.C }
func (r *realTimer) Stop() bool          { return r.t.Stop() }

// ---------------------------------------------------------------------------
// Fake clock
// ---------------------------------------------------------------------------

type fakeTimer struct {
	deadline time.Time
	ch       chan time.Time
	fc       *FakeClock
	stopped  bool
	fired    bool
}

func (t *fakeTimer) C() <-chan time.Time { return t.ch }

func (t *fakeTimer) Stop() bool {
	t.fc.mu.Lock()
	defer t.fc.mu.Unlock()
	wasActive := !t.stopped && !t.fired
	t.stopped = true
	return wasActive
}

// FakeClock is a manually controlled, goroutine-safe clock for tests. Timers
// only fire when the clock is Advanced, making scheduling fully deterministic.
type FakeClock struct {
	mu     sync.Mutex
	now    time.Time
	timers []*fakeTimer
}

// NewFakeClock returns a FakeClock frozen at start.
func NewFakeClock(start time.Time) *FakeClock {
	return &FakeClock{now: start}
}

func (c *FakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *FakeClock) After(d time.Duration) Timer {
	c.mu.Lock()
	defer c.mu.Unlock()
	t := &fakeTimer{
		deadline: c.now.Add(d),
		// Buffer 1 so firing never blocks the clock even if nobody reads.
		ch: make(chan time.Time, 1),
		fc: c,
	}
	c.timers = append(c.timers, t)
	return t
}

// Advance moves the clock forward by d and fires every timer whose deadline
// is within [oldNow, newNow], in deadline order. Timers scheduled during
// firing with a deadline <= newNow fire in the same advance.
func (c *FakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	target := c.now.Add(d)
	c.now = target
	for {
		// Collect due, non-stopped, non-fired timers in deadline order.
		pending := make([]*fakeTimer, 0, len(c.timers))
		for _, t := range c.timers {
			if !t.stopped && !t.fired && !t.deadline.After(target) {
				pending = append(pending, t)
			}
		}
		if len(pending) == 0 {
			break
		}
		sort.Slice(pending, func(i, j int) bool {
			return pending[i].deadline.Before(pending[j].deadline)
		})
		next := pending[0]
		next.fired = true
		// Deliver without holding the lock: timer callbacks may re-enter
		// the clock to schedule more work.
		c.mu.Unlock()
		next.ch <- next.deadline
		c.mu.Lock()
	}
	c.mu.Unlock()
}

// Set moves the clock to an absolute instant and fires due timers. It exists
// for tests that think in absolute (NotBefore) times.
func (c *FakeClock) Set(t time.Time) {
	c.mu.Lock()
	if t.Before(c.now) {
		c.mu.Unlock()
		return
	}
	d := t.Sub(c.now)
	c.mu.Unlock()
	c.Advance(d)
}

// Sleep blocks until d elapses on clk, or ctx is cancelled. With a FakeClock
// another goroutine must Advance the clock for Sleep to return.
func Sleep(ctx context.Context, clk Clock, d time.Duration) error {
	t := clk.After(d)
	select {
	case <-ctx.Done():
		t.Stop()
		return ctx.Err()
	case <-t.C():
		return nil
	}
}
