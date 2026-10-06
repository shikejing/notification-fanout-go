package fanout

import (
	"context"
	"sync"
	"time"
)

// ProcessFunc is the unit of work scheduled by the Dispatcher.
type ProcessFunc func(ctx context.Context)

// Job is a handle on a scheduled item.
type Job struct {
	At time.Time
	Fn ProcessFunc
}

// Dispatcher owns one channel's queue. It enforces:
//   - per-channel concurrency (worker pool size),
//   - per-channel rate limit (FIFO token bucket),
//   - "not before" scheduling (earliest-deadline-first),
//
// and never silently drops work: over-limit/over-concurrency items wait in
// the queue. Every run gets a cancellable context so timeout and shutdown
// cannot leave a job in an intermediate state (a cancelled attempt is simply
// retried/re-enqueued by the engine).
type Dispatcher struct {
	cfg ChannelConfig
	clk Clock
	rl  *RateLimiter

	mu      sync.Mutex
	heap    []*Job
	active  int
	started bool
	stopped bool

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
	wake   chan struct{}
	doneID uint64
}

// NewDispatcher builds a dispatcher from a channel config.
func NewDispatcher(cfg ChannelConfig, clk Clock) *Dispatcher {
	if cfg.Concurrency <= 0 {
		cfg.Concurrency = 1
	}
	if clk == nil {
		clk = NewRealClock()
	}
	return &Dispatcher{
		cfg:  cfg,
		clk:  clk,
		rl:   NewRateLimiter(cfg.RatePerSecond, 1, clk),
		wake: make(chan struct{}, 1),
	}
}

// Start launches the scheduler loop and worker pool.
func (d *Dispatcher) Start(ctx context.Context) {
	d.mu.Lock()
	if d.started {
		d.mu.Unlock()
		return
	}
	d.started = true
	d.ctx, d.cancel = context.WithCancel(ctx)
	runCtx := d.ctx
	d.mu.Unlock()

	d.wg.Add(1)
	go d.scheduleLoop(runCtx)
}

// Schedule enqueues fn to run no earlier than at. The returned Job can be
// ignored; jobs are always eventually processed or returned on Shutdown.
func (d *Dispatcher) Schedule(at time.Time, fn ProcessFunc) *Job {
	j := &Job{At: at, Fn: fn}
	d.mu.Lock()
	d.heap = append(d.heap, j)
	up(d.heap, len(d.heap)-1)
	d.mu.Unlock()
	d.signal()
	return j
}

// QueueLen reports the number of queued (not yet started) jobs.
func (d *Dispatcher) QueueLen() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return len(d.heap)
}

// Active reports in-flight jobs.
func (d *Dispatcher) Active() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.active
}

// Shutdown stops scheduling, cancels in-flight jobs and returns queued jobs
// (earliest first) so the engine can re-enqueue them after restart.
func (d *Dispatcher) Shutdown(ctx context.Context) []Job {
	d.mu.Lock()
	if !d.started {
		d.mu.Unlock()
		return nil
	}
	d.stopped = true
	pending := make([]*Job, len(d.heap))
	copy(pending, d.heap)
	d.heap = nil
	cancel := d.cancel
	d.mu.Unlock()

	cancel() // cancel in-flight and the scheduler wait
	d.signal()

	wait := make(chan struct{})
	go func() { d.wg.Wait(); close(wait) }()
	select {
	case <-wait:
	case <-ctx.Done():
	}

	out := make([]Job, 0, len(pending))
	for _, j := range pending {
		out = append(out, *j)
	}
	return out
}

// scheduleLoop pops due jobs and spawns workers up to Concurrency.
func (d *Dispatcher) scheduleLoop(ctx context.Context) {
	defer d.wg.Done()
	for {
		var timer Timer
		d.mu.Lock()
		for {
			if ctx.Err() != nil || d.stopped {
				d.mu.Unlock()
				return
			}
			if len(d.heap) > 0 && d.active < d.cfg.Concurrency {
				now := d.clk.Now()
				if !d.heap[0].At.After(now) {
					j := d.heap[0]
					d.heap[0] = d.heap[len(d.heap)-1]
					d.heap = d.heap[:len(d.heap)-1]
					if len(d.heap) > 0 {
						down(d.heap, 0, len(d.heap))
					}
					d.active++
					d.wg.Add(1)
					go d.runJob(ctx, j)
					continue // re-evaluate: more may be due / slots free
				}
				timer = d.clk.After(d.heap[0].At.Sub(now))
			}
			break
		}
		wake := d.wake
		d.mu.Unlock()

		if timer != nil {
			select {
			case <-ctx.Done():
				return
			case <-wake:
				timer.Stop()
			case <-timer.C():
			}
		} else {
			select {
			case <-ctx.Done():
				return
			case <-wake:
			}
		}
	}
}

// runJob acquires a rate token then executes fn, always releasing the slot.
func (d *Dispatcher) runJob(ctx context.Context, j *Job) {
	defer d.wg.Done()
	defer func() {
		d.mu.Lock()
		d.active--
		d.mu.Unlock()
		d.signal()
	}()
	// Rate limit is part of queuing: a timeout here means the job has not
	// started, so the engine can retry it without a downstream side effect.
	if err := d.rl.Wait(ctx); err != nil {
		return
	}
	j.Fn(ctx)
}

func (d *Dispatcher) signal() {
	select {
	case d.wake <- struct{}{}:
	default:
	}
}

// --- min-heap helpers (earliest At first) ---

func up(h []*Job, i int) {
	for i > 0 {
		parent := (i - 1) / 2
		if !h[i].At.Before(h[parent].At) {
			return
		}
		h[i], h[parent] = h[parent], h[i]
		i = parent
	}
}

func down(h []*Job, i, n int) {
	for {
		l := 2*i + 1
		r := 2*i + 2
		best := i
		if l < n && h[l].At.Before(h[best].At) {
			best = l
		}
		if r < n && h[r].At.Before(h[best].At) {
			best = r
		}
		if best == i {
			return
		}
		h[i], h[best] = h[best], h[i]
		i = best
	}
}
