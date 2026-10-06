package notify

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

// Dispatcher fans events out to per-channel pipelines, persists every state
// transition, and recovers unfinished work after a restart.
//
// Delivery semantics: for each (event, channel) pair the downstream
// observes the side effect at most once, provided channels are idempotent
// by Delivery.ID (the SimChannel is). State transitions are persisted
// before they take effect, so a crash never causes a committed delivery to
// be repeated and never strands a task in an intermediate state.
type Dispatcher struct {
	store Store
	now   func() time.Time

	mu      sync.Mutex // guards in-memory structures and state transitions
	sched   *scheduler
	runners map[string]*channelRunner
	cfgs    map[string]ChannelConfig

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
	closed bool
}

// NewDispatcher builds a dispatcher over the given store and starts its
// scheduler loop. Call Recover once after registering channels.
func NewDispatcher(store Store) *Dispatcher {
	ctx, cancel := context.WithCancel(context.Background())
	d := &Dispatcher{
		store:   store,
		now:     time.Now,
		sched:   newScheduler(),
		runners: map[string]*channelRunner{},
		cfgs:    map[string]ChannelConfig{},
		ctx:     ctx,
		cancel:  cancel,
	}
	d.wg.Add(1)
	go func() {
		defer d.wg.Done()
		d.sched.run(ctx, d.fire)
	}()
	return d
}

// RegisterChannel attaches a downstream channel with its config and starts
// its worker pool.
func (d *Dispatcher) RegisterChannel(ch Channel, cfg ChannelConfig) {
	cfg = cfg.withDefaults()
	d.mu.Lock()
	defer d.mu.Unlock()
	hooks := runnerHooks{
		get:       d.getDelivery,
		claim:     d.claim,
		succeed:   d.succeed,
		fail:      d.fail,
		interrupt: d.interrupt,
	}
	r := newChannelRunner(ch, cfg, hooks)
	d.runners[ch.Name()] = r
	d.cfgs[ch.Name()] = cfg
	d.wg.Add(1)
	go func() {
		defer d.wg.Done()
		r.run(d.ctx)
	}()
}

// Submit fans one event out to its targets. Re-submitting the same event
// ID is a no-op (idempotent), so duplicates never cause duplicate effects.
func (d *Dispatcher) Submit(ctx context.Context, e Event) (SubmitResult, error) {
	if e.ID == "" {
		return SubmitResult{}, errors.New("notify: event id required")
	}
	if len(e.Targets) == 0 {
		return SubmitResult{}, errors.New("notify: event targets required")
	}
	d.mu.Lock()
	for _, t := range e.Targets {
		if _, ok := d.runners[t]; !ok {
			d.mu.Unlock()
			return SubmitResult{}, fmt.Errorf("notify: unknown channel %q", t)
		}
	}
	d.mu.Unlock()

	inserted, err := d.store.SaveEvent(ctx, e)
	if err != nil {
		return SubmitResult{}, err
	}
	if !inserted {
		// Duplicate logical notification: report existing deliveries, do nothing.
		existing, err := d.store.ListDeliveries(ctx, "", "")
		if err != nil {
			return SubmitResult{}, err
		}
		res := SubmitResult{EventID: e.ID, Duplicated: true}
		for _, dl := range existing {
			if dl.EventID == e.ID {
				res.Deliveries = append(res.Deliveries, dl)
			}
		}
		return res, nil
	}

	notBefore := e.NotBefore
	if notBefore.IsZero() {
		notBefore = d.now()
	}
	res := SubmitResult{EventID: e.ID}
	for _, target := range e.Targets {
		dl := Delivery{
			ID:        DeliveryID(e.ID, target),
			EventID:   e.ID,
			Channel:   target,
			Payload:   e.Payload,
			Status:    StatusPending,
			NotBefore: notBefore,
			CreatedAt: d.now(),
			UpdatedAt: d.now(),
		}
		if _, err := d.store.SaveDelivery(ctx, dl); err != nil {
			return SubmitResult{}, err
		}
		d.sched.schedule(dl.ID, notBefore)
		res.Deliveries = append(res.Deliveries, dl)
	}
	return res, nil
}

// fire is called by the scheduler when a delivery becomes due. The
// pending -> queued transition is guarded, so a late or duplicate trigger
// is recorded as a skipped outcome and never causes a duplicate delivery.
func (d *Dispatcher) fire(deliveryID string) {
	dl, err := d.store.GetDelivery(d.ctx, deliveryID)
	if err != nil {
		return
	}
	d.mu.Lock()
	if dl.Status != StatusPending {
		reason := fmt.Sprintf("late or duplicate trigger ignored: delivery already %s", dl.Status)
		d.appendAttemptLocked(Attempt{
			DeliveryID: dl.ID,
			Outcome:    OutcomeSkipped,
			Reason:     reason,
			StartedAt:  d.now(),
			FinishedAt: d.now(),
		})
		d.mu.Unlock()
		return
	}
	dl.Status = StatusQueued
	dl.UpdatedAt = d.now()
	if err := d.store.UpdateDelivery(d.ctx, dl); err != nil {
		d.mu.Unlock()
		return
	}
	d.mu.Unlock()

	d.mu.Lock()
	r := d.runners[dl.Channel]
	d.mu.Unlock()
	if r == nil {
		return
	}
	// Enqueue in a separate goroutine: a full queue applies backpressure
	// here without stalling the scheduler loop of other channels.
	d.wg.Add(1)
	go func() {
		defer d.wg.Done()
		_ = r.enqueue(d.ctx, deliveryID)
	}()
}

// claim implements the queued -> in_flight transition (runner hook).
func (d *Dispatcher) claim(ctx context.Context, id string) (Delivery, bool, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	dl, err := d.store.GetDelivery(ctx, id)
	if err != nil {
		return Delivery{}, false, err
	}
	if dl.Status != StatusQueued {
		return dl, false, nil // canceled or already handled: skip
	}
	dl.Status = StatusInFlight
	dl.UpdatedAt = d.now()
	if err := d.store.UpdateDelivery(ctx, dl); err != nil {
		return Delivery{}, false, err
	}
	return dl, true, nil
}

// succeed records a successful attempt and marks the delivery succeeded.
func (d *Dispatcher) succeed(ctx context.Context, dl Delivery, started, finished time.Time) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	cur, err := d.store.GetDelivery(ctx, dl.ID)
	if err != nil {
		return err
	}
	if cur.Status != StatusInFlight {
		return nil // superseded (e.g. canceled meanwhile)
	}
	cur.Attempts++
	d.appendAttemptLocked(Attempt{
		DeliveryID: cur.ID, Seq: cur.Attempts,
		Outcome: OutcomeSuccess, Reason: "delivered",
		StartedAt: started, FinishedAt: finished,
	})
	cur.Status = StatusSucceeded
	cur.UpdatedAt = d.now()
	return d.store.UpdateDelivery(ctx, cur)
}

// fail records a failed attempt and decides retry vs dead letter.
func (d *Dispatcher) fail(ctx context.Context, dl Delivery, cause error, started, finished time.Time) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	cur, err := d.store.GetDelivery(ctx, dl.ID)
	if err != nil {
		return err
	}
	if cur.Status != StatusInFlight {
		return nil
	}
	cur.Attempts++
	reason := fmt.Sprintf("attempt %d failed: %v", cur.Attempts, cause)
	d.appendAttemptLocked(Attempt{
		DeliveryID: cur.ID, Seq: cur.Attempts,
		Outcome: OutcomeFailure, Reason: reason,
		StartedAt: started, FinishedAt: finished,
	})
	cur.LastError = cause.Error()
	cur.UpdatedAt = d.now()

	cfg := d.cfgs[cur.Channel]
	var perm PermanentError
	permanent := errors.As(cause, &perm)
	if permanent || cur.Attempts >= cfg.MaxAttempts {
		cur.Status = StatusDead
		if permanent {
			cur.LastError = "permanent: " + cur.LastError
		}
		return d.store.UpdateDelivery(ctx, cur)
	}
	// Retry with exponential backoff: back to pending, re-scheduled.
	cur.Status = StatusPending
	cur.NotBefore = d.now().Add(backoffDelay(cfg, cur.Attempts))
	if err := d.store.UpdateDelivery(ctx, cur); err != nil {
		return err
	}
	d.sched.schedule(cur.ID, cur.NotBefore)
	return nil
}

// interrupt settles an attempt cut short by shutdown/cancel: the attempt
// is recorded as skipped and the delivery returns to queued, so no task is
// left in an intermediate state and no retry budget is consumed.
func (d *Dispatcher) interrupt(ctx context.Context, dl Delivery, reason string, started, finished time.Time) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	cur, err := d.store.GetDelivery(ctx, dl.ID)
	if err != nil {
		return err
	}
	if cur.Status != StatusInFlight {
		return nil
	}
	d.appendAttemptLocked(Attempt{
		DeliveryID: cur.ID,
		Outcome:    OutcomeSkipped, Reason: reason,
		StartedAt: started, FinishedAt: finished,
	})
	cur.Status = StatusQueued
	cur.UpdatedAt = d.now()
	return d.store.UpdateDelivery(ctx, cur)
}

// Recover re-drives unfinished work after a restart: in-flight deliveries
// (interrupted by the crash) are reset to queued, queued ones are
// re-enqueued, pending ones are re-scheduled. Already-succeeded or dead
// deliveries are untouched, so nothing committed is ever repeated.
func (d *Dispatcher) Recover(ctx context.Context) error {
	all, err := d.store.ListDeliveries(ctx, "", "")
	if err != nil {
		return err
	}
	for _, dl := range all {
		switch dl.Status {
		case StatusInFlight:
			d.mu.Lock()
			d.appendAttemptLocked(Attempt{
				DeliveryID: dl.ID,
				Outcome:    OutcomeSkipped,
				Reason:     "recovered after restart: interrupted attempt discarded",
				StartedAt:  d.now(), FinishedAt: d.now(),
			})
			dl.Status = StatusQueued
			dl.UpdatedAt = d.now()
			if err := d.store.UpdateDelivery(ctx, dl); err != nil {
				d.mu.Unlock()
				return err
			}
			d.mu.Unlock()
			d.enqueueTo(dl)
		case StatusQueued:
			d.enqueueTo(dl)
		case StatusPending:
			d.sched.schedule(dl.ID, dl.NotBefore)
		default:
			// terminal states: nothing to do, never repeated.
		}
	}
	return nil
}

func (d *Dispatcher) enqueueTo(dl Delivery) {
	d.mu.Lock()
	r := d.runners[dl.Channel]
	d.mu.Unlock()
	if r == nil {
		return
	}
	d.wg.Add(1)
	go func() {
		defer d.wg.Done()
		_ = r.enqueue(d.ctx, dl.ID)
	}()
}

// Cancel cancels a non-terminal delivery; terminal deliveries are untouched.
func (d *Dispatcher) Cancel(ctx context.Context, deliveryID string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	dl, err := d.store.GetDelivery(ctx, deliveryID)
	if err != nil {
		return err
	}
	if dl.Status.Terminal() {
		return nil
	}
	d.sched.cancel(deliveryID)
	d.appendAttemptLocked(Attempt{
		DeliveryID: dl.ID,
		Outcome:    OutcomeSkipped, Reason: "canceled by caller",
		StartedAt: d.now(), FinishedAt: d.now(),
	})
	dl.Status = StatusCanceled
	dl.UpdatedAt = d.now()
	return d.store.UpdateDelivery(ctx, dl)
}

// Delivery returns the current state of one delivery.
func (d *Dispatcher) Delivery(ctx context.Context, id string) (Delivery, error) {
	return d.store.GetDelivery(ctx, id)
}

// Attempts returns the audit trail of one delivery.
func (d *Dispatcher) Attempts(ctx context.Context, deliveryID string) ([]Attempt, error) {
	return d.store.ListAttempts(ctx, deliveryID)
}

// DeadLetters lists deliveries that exhausted their retries.
func (d *Dispatcher) DeadLetters(ctx context.Context) ([]Delivery, error) {
	return d.store.ListDeliveries(ctx, "", StatusDead)
}

// Close stops background work, settles in-flight attempts, and flushes the store.
func (d *Dispatcher) Close() error {
	d.mu.Lock()
	if d.closed {
		d.mu.Unlock()
		return nil
	}
	d.closed = true
	d.mu.Unlock()
	d.cancel()
	d.wg.Wait()
	return d.store.Close()
}

func (d *Dispatcher) getDelivery(ctx context.Context, id string) (Delivery, error) {
	return d.store.GetDelivery(ctx, id)
}

// appendAttemptLocked records an attempt, assigning the next sequence
// number when unset (must hold d.mu).
func (d *Dispatcher) appendAttemptLocked(a Attempt) {
	if a.Seq == 0 {
		existing, _ := d.store.ListAttempts(context.Background(), a.DeliveryID)
		a.Seq = len(existing) + 1
	}
	_ = d.store.AppendAttempt(context.Background(), a)
}
