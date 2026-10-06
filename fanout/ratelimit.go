package fanout

import (
	"context"
	"sync"
)

// RateLimiter is a per-channel token-bucket limiter with strict FIFO
// admission: callers that exceed the rate queue up in arrival order and wait
// until a token is available; they are never rejected or dropped.
type RateLimiter struct {
	rate float64 // tokens per second; <= 0 means unlimited
	clk  Clock

	mu     sync.Mutex
	tokens float64
	maxTok float64
	queue  []chan struct{} // FIFO of waiters waiting for a token
	stop   chan struct{}
	done   chan struct{}
}

// NewRateLimiter builds a limiter. ratePerSecond <= 0 means unlimited;
// burst is the bucket capacity (clamped to at least 1).
func NewRateLimiter(ratePerSecond float64, burst int, clk Clock) *RateLimiter {
	if clk == nil {
		clk = NewRealClock()
	}
	if burst < 1 {
		burst = 1
	}
	l := &RateLimiter{
		rate:   ratePerSecond,
		clk:    clk,
		tokens: float64(burst),
		maxTok: float64(burst),
		stop:   make(chan struct{}),
		done:   make(chan struct{}),
	}
	if ratePerSecond > 0 {
		go l.refill()
	}
	return l
}

// refill adds tokens at a steady 10Hz cadence scaled to the configured rate.
// Using the injectable Clock makes the limiter deterministic under
// FakeClock.
func (l *RateLimiter) refill() {
	defer close(l.done)
	const stepsPerSecond = 10
	interval := nsPerSecond / stepsPerSecond
	add := l.rate / float64(stepsPerSecond)
	t := l.clk.After(interval)
	for {
		select {
		case <-l.stop:
			return
		case <-t.C():
			l.mu.Lock()
			if l.tokens < l.maxTok {
				l.tokens += add
				if l.tokens > l.maxTok {
					l.tokens = l.maxTok
				}
			}
			l.deliverLocked()
			l.mu.Unlock()
			t = l.clk.After(interval)
		}
	}
}

// deliverLocked hands a token to queued waiters in FIFO order while tokens
// remain.
func (l *RateLimiter) deliverLocked() {
	for len(l.queue) > 0 && l.tokens >= 1 {
		w := l.queue[0]
		l.queue = l.queue[1:]
		l.tokens--
		close(w)
	}
}

// Wait blocks until a token is acquired, ctx is cancelled, or the limiter is
// closed. Cancelled callers leave the queue without consuming a token.
func (l *RateLimiter) Wait(ctx context.Context) error {
	if l.rate <= 0 {
		return ctx.Err()
	}
	l.mu.Lock()
	if len(l.queue) == 0 && l.tokens >= 1 {
		l.tokens--
		l.mu.Unlock()
		return nil
	}
	w := make(chan struct{})
	l.queue = append(l.queue, w)
	l.mu.Unlock()

	select {
	case <-ctx.Done():
		l.remove(w)
		return ctx.Err()
	case <-l.stop:
		return errLimiterStopped
	case <-w:
		return nil
	}
}

// remove drops a cancelled waiter from the queue.
func (l *RateLimiter) remove(w chan struct{}) {
	l.mu.Lock()
	defer l.mu.Unlock()
	for i, q := range l.queue {
		if q == w {
			l.queue = append(l.queue[:i], l.queue[i+1:]...)
			return
		}
	}
}

// Waiting returns the number of queued callers (observability).
func (l *RateLimiter) Waiting() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.queue)
}

// Close stops the refill loop.
func (l *RateLimiter) Close() {
	if l.rate <= 0 {
		return
	}
	select {
	case <-l.stop:
	default:
		close(l.stop)
	}
	<-l.done
}
