package fanout

import (
	"context"
	"fmt"
	"time"
)

// scheduleDelivery enqueues one delivery attempt sequence on the channel's
// dispatcher. dueOverride > 0 forces the first run time (used after recovery
// to honour not-yet-elapsed backoff); otherwise the event's NotBefore is
// used.
func (e *Engine) scheduleDelivery(
	reg *channelReg,
	d *Delivery,
	ch Channel,
	cfg ChannelConfig,
	disp *Dispatcher,
	payload []byte,
	notBefore time.Time,
	dueOverride time.Time,
) {
	due := dueOverride
	if due.IsZero() {
		due = notBefore
	}
	now := e.clk.Now()
	if !due.After(now) {
		due = now
	}
	disp.Schedule(due, func(ctx context.Context) {
		e.runAttempt(ctx, reg, d, ch, cfg, payload, notBefore)
	})
}

// rescheduleAfterRecovery continues an unfinished delivery after a WAL
// replay. An attempt whose "start" marker was persisted but whose "finish"
// was not (crash mid-attempt) is treated as interrupted and retried with
// normal backoff; the downstream idempotency key prevents a double side
// effect.
func (e *Engine) rescheduleAfterRecovery(d *Delivery) {
	e.mu.Lock()
	reg, ok := e.channels[d.Channel]
	if !ok {
		e.mu.Unlock()
		return
	}
	ch := reg.ch
	cfg := reg.cfg
	payload := e.payloads[d.EventID]
	notBefore := d.NotBefore
	status := d.Status
	attemptCount := d.AttemptCount
	e.mu.Unlock()

	due := notBefore
	switch status {
	case StatusInFlight:
		// The last attempt only has a "start" marker: its result is unknown
		// because the process crashed mid-attempt. It must not consume the
		// retry budget; annotate it as interrupted (keeping it in the
		// explainable history) and roll the counter back so it is retried.
		e.mu.Lock()
		if attemptCount > 0 {
			idx := attemptCount - 1
			for len(d.Attempts) < attemptCount {
				d.Attempts = append(d.Attempts, Attempt{Number: len(d.Attempts) + 1})
			}
			last := &d.Attempts[idx]
			if !last.StartedAt.IsZero() && last.FinishedAt.IsZero() {
				last.FinishedAt = e.clk.Now()
				last.Error = "interrupted by process crash before result was recorded"
			}
			d.AttemptCount = idx // re-run this attempt number
			d.Status = StatusPending
			d.Reason = "resuming interrupted attempt after recovery"
		}
		e.mu.Unlock()
		// Restart promptly; NotBefore still applies as the floor.
	case StatusPending:
		// Waiting for a scheduled first run or a retry: honour whichever is
		// later, the original NotBefore or (now + remaining backoff).
		if attemptCount > 0 {
			next := attemptCount + 1
			wait := nextBackoff(cfg.InitialBackoff, cfg.MaxBackoff, next)
			t := e.clk.Now().Add(wait)
			if t.After(due) {
				due = t
			}
		}
	}
	reg.disp.Schedule(due, func(ctx context.Context) {
		e.runAttempt(ctx, reg, d, ch, cfg, payload, notBefore)
	})
}

// runAttempt executes a single attempt, persists its boundaries and drives
// the delivery to success, retry-with-backoff, dead-letter or (on context
// cancellation) a clean requeue. It never leaves the delivery in an
// intermediate state.
func (e *Engine) runAttempt(
	ctx context.Context,
	reg *channelReg,
	d *Delivery,
	ch Channel,
	cfg ChannelConfig,
	payload []byte,
	notBefore time.Time,
) {
	// A late scheduler trigger must never cause a duplicate delivery: if the
	// delivery is already terminal (e.g. succeeded via another path), stop.
	e.mu.Lock()
	// Do not start a new attempt once shutdown has begun: the durable
	// pending/in-flight state is resumed by recovery after a later Start.
	if e.stopped {
		d.Status = StatusPending
		d.Reason = "not started: engine shutting down (resumed on restart)"
		e.mu.Unlock()
		return
	}
	if isTerminal(d.Status) {
		e.mu.Unlock()
		return
	}
	attemptNo := d.AttemptCount + 1
	d.AttemptCount = attemptNo
	d.Status = StatusInFlight
	for len(d.Attempts) < attemptNo {
		d.Attempts = append(d.Attempts, Attempt{Number: len(d.Attempts) + 1})
	}
	d.Attempts[attemptNo-1].StartedAt = e.clk.Now()
	e.mu.Unlock()

	if _, err := e.store.Append(ctx, Entry{
		Kind: EntryAttempt, EventID: d.EventID, DeliveryID: d.ID,
		Channel: d.Channel, AttemptNo: attemptNo, Starting: true,
		Status: StatusInFlight,
	}); err != nil {
		// If persistence fails we cannot safely claim an attempt started;
		// requeue shortly and let the durable boundary protect idempotency.
		e.requeue(reg, d, ch, cfg, payload, notBefore, "persist attempt start failed: "+err.Error())
		return
	}

	// Per-attempt timeout, if configured, nested under the dispatcher ctx.
	attemptCtx := ctx
	var cancel context.CancelFunc
	if cfg.AttemptTimeout > 0 {
		attemptCtx, cancel = context.WithTimeout(ctx, cfg.AttemptTimeout)
	}
	err := ch.Send(attemptCtx, d.ID, payload)
	if cancel != nil {
		cancel()
	}

	finishedAt := e.clk.Now()
	failReason := ""
	if err != nil {
		failReason = err.Error()
	}

	// Persist the attempt finish before any further state transition, so a
	// crash here is recoverable: on replay this attempt is visible and the
	// delivery resumes consistently.
	if _, perr := e.store.Append(context.Background(), Entry{
		Kind: EntryAttempt, EventID: d.EventID, DeliveryID: d.ID,
		Channel: d.Channel, AttemptNo: attemptNo, Starting: false,
		Status: map[bool]DeliveryStatus{true: StatusPending, false: StatusSuccess}[err == nil],
		Reason: failReason,
	}); perr != nil {
		// Cannot durably record the result; surface as a retriable failure
		// using the in-memory record, and requeue. The downstream key keeps
		// the effect at most once.
		failReason = joinReason(failReason, "persist attempt finish failed: "+perr.Error())
		err = fmt.Errorf("%s", failReason)
	}

	e.mu.Lock()
	a := &d.Attempts[attemptNo-1]
	a.FinishedAt = finishedAt
	a.Error = failReason
	d.UpdatedAt = finishedAt
	remaining := cfg.MaxAttempts - attemptNo
	e.mu.Unlock()

	switch {
	case err == nil:
		e.completeSuccess(ctx, d)
	case remaining > 0 && ctx.Err() == nil:
		e.retry(reg, d, ch, cfg, payload, notBefore, attemptNo, failReason)
	case ctx.Err() != nil:
		// Shutdown/timeout cancellation before completion: leave the
		// delivery pending and let recovery resume it. No terminal state,
		// no dead-letter consumption.
		e.markPendingAfterCancel(d, "cancelled before completion: "+ctx.Err().Error())
	default:
		e.deadLetter(ctx, d, failReason)
	}
}

// completeSuccess persists and publishes the success terminal state.
func (e *Engine) completeSuccess(ctx context.Context, d *Delivery) {
	e.mu.Lock()
	d.Status = StatusSuccess
	d.Reason = "delivered"
	d.UpdatedAt = e.clk.Now()
	e.mu.Unlock()

	if _, err := e.store.Append(context.Background(), Entry{
		Kind: EntryTerminal, EventID: d.EventID, DeliveryID: d.ID,
		Channel: d.Channel, Status: StatusSuccess, Reason: "delivered",
	}); err != nil {
		// Even if persistence of the terminal marker fails, the attempt
		// finish was already durably recorded with no error, so recovery
		// converges to success. Keep status consistent in memory.
		_ = err
	}
	e.publishTerminal(d)
}

// retry schedules the next attempt after exponential backoff.
func (e *Engine) retry(
	reg *channelReg,
	d *Delivery,
	ch Channel,
	cfg ChannelConfig,
	payload []byte,
	notBefore time.Time,
	lastAttempt int,
	reason string,
) {
	wait := nextBackoff(cfg.InitialBackoff, cfg.MaxBackoff, lastAttempt+1)
	due := e.clk.Now().Add(wait)
	if notBefore.After(due) {
		due = notBefore
	}
	e.mu.Lock()
	d.Status = StatusPending
	d.Reason = "retrying after: " + reason
	d.UpdatedAt = e.clk.Now()
	e.mu.Unlock()
	reg.disp.Schedule(due, func(ctx context.Context) {
		e.runAttempt(ctx, reg, d, ch, cfg, payload, notBefore)
	})
}

// requeue is used when an attempt could not even be safely recorded as
// started; it retries shortly like retry but does not consume the attempt
// budget beyond what was already incremented.
func (e *Engine) requeue(
	reg *channelReg,
	d *Delivery,
	ch Channel,
	cfg ChannelConfig,
	payload []byte,
	notBefore time.Time,
	reason string,
) {
	wait := nextBackoff(cfg.InitialBackoff, cfg.MaxBackoff, d.AttemptCount+1)
	if wait <= 0 {
		wait = 10 * time.Millisecond
	}
	due := e.clk.Now().Add(wait)
	if notBefore.After(due) {
		due = notBefore
	}
	e.mu.Lock()
	d.Status = StatusPending
	d.Reason = "requeued: " + reason
	d.UpdatedAt = e.clk.Now()
	e.mu.Unlock()
	reg.disp.Schedule(due, func(ctx context.Context) {
		e.runAttempt(ctx, reg, d, ch, cfg, payload, notBefore)
	})
}

// markPendingAfterCancel records a readable pending reason after a
// cancellation; the delivery is resumed by recovery/reschedule.
func (e *Engine) markPendingAfterCancel(d *Delivery, reason string) {
	e.mu.Lock()
	if !isTerminal(d.Status) {
		d.Status = StatusPending
	}
	d.Reason = reason
	d.UpdatedAt = e.clk.Now()
	e.mu.Unlock()
}

// deadLetter exhausts attempts: the delivery becomes failed, is recorded in
// the dead-letter set and a dead entry is persisted.
func (e *Engine) deadLetter(ctx context.Context, d *Delivery, reason string) {
	full := fmt.Sprintf("max attempts exhausted (%d); last error: %s", d.AttemptCount, reason)
	e.mu.Lock()
	d.Status = StatusFailed
	d.Reason = full
	d.UpdatedAt = e.clk.Now()
	e.dead[d.ID] = true
	e.mu.Unlock()

	if _, err := e.store.Append(context.Background(), Entry{
		Kind: EntryTerminal, EventID: d.EventID, DeliveryID: d.ID,
		Channel: d.Channel, Status: StatusFailed, Reason: full,
	}); err != nil {
		_ = err
	}
	if _, err := e.store.Append(context.Background(), Entry{
		Kind: EntryDead, EventID: d.EventID, DeliveryID: d.ID,
		Channel: d.Channel, Reason: full,
	}); err != nil {
		_ = err
	}
	e.publishTerminal(d)
}

// skipDelivery persists a skipped terminal state without side effects.
func (e *Engine) skipDelivery(ctx context.Context, d *Delivery, reason string) {
	e.mu.Lock()
	d.Status = StatusSkipped
	d.Reason = reason
	d.UpdatedAt = e.clk.Now()
	e.mu.Unlock()
	if _, err := e.store.Append(ctx, Entry{
		Kind: EntryCreated, EventID: d.EventID, DeliveryID: d.ID,
		Channel: d.Channel, Status: StatusSkipped, Reason: reason,
		NotBefore: d.NotBefore,
	}); err != nil {
		_ = err
	}
	e.publishTerminal(d)
}

// publishTerminal finalizes counters and fans out the readable result.
func (e *Engine) publishTerminal(d *Delivery) {
	e.mu.Lock()
	e.markFinishedLocked(d)
	res := toResult(d)
	ch := e.notifyCh
	e.mu.Unlock()
	if ch != nil {
		select {
		case ch <- res:
		default:
		}
	}
	select {
	case e.wake <- struct{}{}:
	default:
	}
}

func isTerminal(s DeliveryStatus) bool {
	return s == StatusSuccess || s == StatusFailed || s == StatusSkipped
}

func joinReason(a, b string) string {
	if a == "" {
		return b
	}
	return a + "; " + b
}
