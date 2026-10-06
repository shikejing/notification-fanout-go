package fanout

import (
	"context"
	"sort"
	"time"
)

// Report returns the explainable state of one event, or nil if the event is
// unknown to the engine.
func (e *Engine) Report(eventID string) *EventReport {
	e.mu.Lock()
	defer e.mu.Unlock()
	es, ok := e.events[eventID]
	if !ok {
		return nil
	}
	e.refreshReportLocked(es)
	return cloneReport(es.report)
}

// Delivery returns a snapshot copy of one (event, channel) delivery state.
func (e *Engine) Delivery(eventID, channel string) *Delivery {
	did := deliveryID(eventID, channel)
	e.mu.Lock()
	defer e.mu.Unlock()
	d := e.dels[did]
	if d == nil {
		return nil
	}
	cp := *d
	cp.Attempts = append([]Attempt(nil), d.Attempts...)
	return &cp
}

// WaitForEvent blocks until every delivery of an event is terminal or ctx is
// cancelled, then returns the report.
func (e *Engine) WaitForEvent(ctx context.Context, eventID string) (*EventReport, error) {
	for {
		e.mu.Lock()
		es, ok := e.events[eventID]
		if !ok {
			e.mu.Unlock()
			// The event may appear shortly (concurrent Submit); poll once
			// briefly via wake rather than spin.
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-e.wake:
			case <-time.After(5 * time.Millisecond):
			}
			continue
		}
		done := es.done
		e.refreshReportLocked(es)
		rep := cloneReport(es.report)
		finished := es.finished >= es.total
		e.mu.Unlock()
		if finished {
			return rep, nil
		}
		select {
		case <-ctx.Done():
			return rep, ctx.Err()
		case <-done:
		case <-e.wake:
		}
	}
}

// DeadLetters returns all deliveries that exhausted their attempts, in
// creation order. Each carries the full attempt history and final reason.
func (e *Engine) DeadLetters() []Delivery {
	e.mu.Lock()
	defer e.mu.Unlock()
	out := make([]Delivery, 0, len(e.dead))
	for _, id := range e.order {
		if !e.dead[id] {
			continue
		}
		d := e.dels[id]
		if d == nil {
			continue
		}
		cp := *d
		cp.Attempts = append([]Attempt(nil), d.Attempts...)
		out = append(out, cp)
	}
	sort.SliceStable(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

// Shutdown stops accepting work, cancels and drains in-flight attempts and
// closes the durable store. Every delivery is left either terminal or
// durably recorded as unfinished (pending/in-flight with persisted attempt
// boundaries), so a later Start resumes it without duplication.
func (e *Engine) Shutdown(ctx context.Context) error {
	e.mu.Lock()
	if e.stopped {
		e.mu.Unlock()
		return nil
	}
	e.stopped = true
	regs := make([]*channelReg, 0, len(e.channels))
	for _, reg := range e.channels {
		regs = append(regs, reg)
	}
	cancel := e.schedCancel
	e.mu.Unlock()

	// Cancel all schedulers (in-flight attempts get a cancelled ctx) and
	// collect queued jobs; they need not be re-enqueued now because their
	// state is already durable in the WAL and recovery re-derives the queue.
	for _, reg := range regs {
		if reg.disp != nil {
			reg.disp.Shutdown(ctx)
		}
	}
	if cancel != nil {
		cancel()
	}
	return e.store.Close()
}

// Notifier returns a channel that receives each delivery's terminal result.
// This is best-effort/observability only; durability comes from the store.
func (e *Engine) Notifier() <-chan DeliveryResult {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.notifyCh == nil {
		e.notifyCh = make(chan DeliveryResult, 256)
	}
	return e.notifyCh
}
