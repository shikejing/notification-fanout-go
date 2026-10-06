package notify

import (
	"context"
	"errors"
	"sync"
	"time"
)

// runnerHooks connect a channelRunner to the dispatcher's persisted state
// machine. Every transition is guarded and persisted by the dispatcher.
type runnerHooks struct {
	// get loads the current delivery state.
	get func(ctx context.Context, id string) (Delivery, error)
	// claim atomically moves queued -> in_flight. ok=false means the
	// delivery is no longer claimable (already done/canceled) and the
	// worker must skip it.
	claim func(ctx context.Context, id string) (Delivery, bool, error)
	// succeed records a successful attempt and marks the delivery succeeded.
	succeed func(ctx context.Context, d Delivery, started, finished time.Time) error
	// fail records a failed attempt; the dispatcher decides retry
	// (re-scheduling with backoff) or dead-lettering.
	fail func(ctx context.Context, d Delivery, cause error, started, finished time.Time) error
	// interrupt records an interrupted attempt (worker shutdown / caller
	// cancel) and returns the delivery to queued so it can resume later.
	interrupt func(ctx context.Context, d Delivery, reason string, started, finished time.Time) error
}

// tokenBucket is a minimal rate limiter (no external deps).
type tokenBucket struct {
	mu       sync.Mutex
	rate     float64 // tokens per second
	tokens   float64
	last     time.Time
	capacity float64
}

func newTokenBucket(rate float64) *tokenBucket {
	return &tokenBucket{rate: rate, tokens: 1, capacity: 1, last: time.Now()}
}

// wait blocks until a token is available or ctx is done.
func (b *tokenBucket) wait(ctx context.Context) error {
	for {
		b.mu.Lock()
		now := time.Now()
		b.tokens += now.Sub(b.last).Seconds() * b.rate
		if b.tokens > b.capacity {
			b.tokens = b.capacity
		}
		b.last = now
		if b.tokens >= 1 {
			b.tokens--
			b.mu.Unlock()
			return nil
		}
		deficit := (1 - b.tokens) / b.rate
		b.mu.Unlock()
		t := time.NewTimer(time.Duration(deficit * float64(time.Second)))
		select {
		case <-ctx.Done():
			t.Stop()
			return ctx.Err()
		case <-t.C:
		}
	}
}

// channelRunner is the per-channel delivery pipeline: a bounded worker pool
// with a rate limiter. Over-limit deliveries wait in the queue; they are
// never dropped. Retries use exponential backoff and exhausted deliveries
// go to the dead letter set (decided by the dispatcher via hooks).
type channelRunner struct {
	cfg     ChannelConfig
	ch      Channel
	hooks   runnerHooks
	queue   chan string
	limiter *tokenBucket
}

func newChannelRunner(ch Channel, cfg ChannelConfig, hooks runnerHooks) *channelRunner {
	cfg = cfg.withDefaults()
	cap := cfg.QueueCapacity
	if cap <= 0 {
		cap = 1 << 16 // effectively unlimited; enqueue never drops
	}
	r := &channelRunner{
		cfg:   cfg,
		ch:    ch,
		hooks: hooks,
		queue: make(chan string, cap),
	}
	if cfg.RatePerSec > 0 {
		r.limiter = newTokenBucket(cfg.RatePerSec)
	}
	return r
}

// enqueue offers a due delivery to this channel's queue. It blocks while
// the queue is full (backpressure) instead of dropping the delivery, and
// returns when the delivery is queued or ctx is done.
func (r *channelRunner) enqueue(ctx context.Context, deliveryID string) error {
	select {
	case r.queue <- deliveryID:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// run starts the worker pool and returns when ctx is done and in-flight
// attempts have been settled (no task is left in an intermediate state).
func (r *channelRunner) run(ctx context.Context) {
	var wg sync.WaitGroup
	for i := 0; i < r.cfg.Concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r.worker(ctx)
		}()
	}
	wg.Wait()
}

func (r *channelRunner) worker(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case id := <-r.queue:
			r.process(ctx, id)
		}
	}
}

// process runs one delivery attempt end to end.
func (r *channelRunner) process(ctx context.Context, id string) {
	if r.limiter != nil {
		if err := r.limiter.wait(ctx); err != nil {
			r.requeue(id) // shutdown while waiting for a token: put it back
			return
		}
	}
	d, ok, err := r.hooks.claim(ctx, id)
	if err != nil || !ok {
		return // gone, or already terminal: skip, nothing to do
	}
	started := time.Now()

	attemptCtx := ctx
	cancel := func() {}
	if r.cfg.DeliverTimeout > 0 {
		attemptCtx, cancel = context.WithTimeout(ctx, r.cfg.DeliverTimeout)
	}
	deliverErr := r.ch.Deliver(attemptCtx, d)
	finished := time.Now()
	cancel()

	if deliverErr == nil {
		_ = r.hooks.succeed(context.Background(), d, started, finished)
		return
	}
	// Distinguish "our side stopped" (shutdown/cancel) from a real failure:
	// an interrupted attempt must not consume a retry budget nor leave the
	// delivery in an intermediate state.
	if ctx.Err() != nil && !isTimeoutOnly(ctx, attemptCtx) {
		_ = r.hooks.interrupt(context.Background(), d, "worker shutdown: attempt interrupted", started, finished)
		return
	}
	_ = r.hooks.fail(context.Background(), d, deliverErr, started, finished)
}

// requeue puts an id back at the head of the queue (best effort, used on
// shutdown paths; recovery also re-queues from the store, so nothing is lost).
func (r *channelRunner) requeue(id string) {
	select {
	case r.queue <- id:
	default:
	}
}

// isTimeoutOnly reports whether the attempt ctx hit its own deadline while
// the parent worker ctx is still alive (i.e. a real per-attempt timeout).
func isTimeoutOnly(parent, attempt context.Context) bool {
	return parent.Err() == nil && errors.Is(attempt.Err(), context.DeadlineExceeded)
}

// backoffDelay computes the retry delay for the given finished-attempt
// count: exponential growth from BaseBackoff, capped at MaxBackoff.
func backoffDelay(cfg ChannelConfig, attempts int) time.Duration {
	d := cfg.BaseBackoff
	for i := 1; i < attempts; i++ {
		d *= 2
		if d >= cfg.MaxBackoff {
			return cfg.MaxBackoff
		}
	}
	if d > cfg.MaxBackoff {
		d = cfg.MaxBackoff
	}
	return d
}
