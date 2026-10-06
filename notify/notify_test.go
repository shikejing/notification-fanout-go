package notify

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

// waitFor polls cond until it holds or the deadline passes.
func waitFor(t *testing.T, what string, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for: %s", what)
}

func newTestDispatcher(t *testing.T) (*Dispatcher, *FileStore) {
	t.Helper()
	store, err := OpenFileStore(t.TempDir() + "/store.jsonl")
	if err != nil {
		t.Fatal(err)
	}
	return NewDispatcher(store), store
}

func TestFanoutChannelsAreIndependent(t *testing.T) {
	d, _ := newTestDispatcher(t)
	defer d.Close()

	gate := make(chan struct{})
	slow := NewSimChannel("slow").WithGate(gate)
	fast := NewSimChannel("fast")
	d.RegisterChannel(slow, ChannelConfig{})
	d.RegisterChannel(fast, ChannelConfig{})

	_, err := d.Submit(context.Background(), Event{
		ID: "e1", Payload: []byte("hello"), Targets: []string{"slow", "fast"},
	})
	if err != nil {
		t.Fatal(err)
	}

	// The blocked channel must not hold back the fast one.
	waitFor(t, "fast channel delivery", 2*time.Second, func() bool {
		return len(fast.Received()) == 1
	})
	if got := len(slow.Received()); got != 0 {
		t.Fatalf("slow channel should still be blocked, got %d deliveries", got)
	}

	close(gate)
	waitFor(t, "slow channel delivery after gate opens", 2*time.Second, func() bool {
		return len(slow.Received()) == 1
	})
}

func TestDuplicateSubmitIsIdempotent(t *testing.T) {
	d, _ := newTestDispatcher(t)
	defer d.Close()

	ch := NewSimChannel("a")
	d.RegisterChannel(ch, ChannelConfig{})

	ev := Event{ID: "dup-1", Payload: []byte("x"), Targets: []string{"a"}}
	r1, err := d.Submit(context.Background(), ev)
	if err != nil {
		t.Fatal(err)
	}
	if r1.Duplicated {
		t.Fatal("first submit must not be marked duplicated")
	}
	waitFor(t, "first delivery", 2*time.Second, func() bool { return len(ch.Received()) == 1 })

	r2, err := d.Submit(context.Background(), ev)
	if err != nil {
		t.Fatal(err)
	}
	if !r2.Duplicated {
		t.Fatal("second submit of same id must be a no-op marked duplicated")
	}
	// Give the system a chance to misbehave, then verify no extra effect.
	time.Sleep(100 * time.Millisecond)
	if got := len(ch.Received()); got != 1 {
		t.Fatalf("downstream must observe exactly one effect, got %d", got)
	}
}

func TestScheduledAndLateTrigger(t *testing.T) {
	d, _ := newTestDispatcher(t)
	defer d.Close()

	ch := NewSimChannel("a")
	d.RegisterChannel(ch, ChannelConfig{})

	when := time.Now().Add(150 * time.Millisecond)
	_, err := d.Submit(context.Background(), Event{
		ID: "sched-1", Payload: []byte("x"), Targets: []string{"a"}, NotBefore: when,
	})
	if err != nil {
		t.Fatal(err)
	}

	// Not delivered before the scheduled time.
	time.Sleep(60 * time.Millisecond)
	if got := len(ch.Received()); got != 0 {
		t.Fatalf("delivered too early: %d", got)
	}
	waitFor(t, "scheduled delivery", 2*time.Second, func() bool { return len(ch.Received()) == 1 })

	// A late/duplicate trigger must not cause a duplicate delivery and must
	// be recorded as a skipped outcome with a readable reason.
	d.fire(DeliveryID("sched-1", "a"))
	time.Sleep(50 * time.Millisecond)
	if got := len(ch.Received()); got != 1 {
		t.Fatalf("late trigger caused duplicate delivery: %d", got)
	}
	attempts, err := d.Attempts(context.Background(), DeliveryID("sched-1", "a"))
	if err != nil {
		t.Fatal(err)
	}
	var sawSuccess, sawSkip bool
	for _, a := range attempts {
		if a.Outcome == OutcomeSuccess {
			sawSuccess = true
		}
		if a.Outcome == OutcomeSkipped {
			sawSkip = true
			if a.Reason == "" {
				t.Fatal("skipped attempt must carry a readable reason")
			}
		}
	}
	if !sawSuccess || !sawSkip {
		t.Fatalf("expected success+skipped attempts, got %+v", attempts)
	}
}

func TestRetryBackoffAndSuccess(t *testing.T) {
	d, _ := newTestDispatcher(t)
	defer d.Close()

	ch := NewSimChannel("a").WithFailures(2) // fail twice, then succeed
	d.RegisterChannel(ch, ChannelConfig{
		MaxAttempts: 5,
		BaseBackoff: 40 * time.Millisecond,
		MaxBackoff:  200 * time.Millisecond,
	})

	_, err := d.Submit(context.Background(), Event{ID: "retry-1", Targets: []string{"a"}})
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, "eventual success", 3*time.Second, func() bool { return len(ch.Received()) == 1 })

	dl, err := d.Delivery(context.Background(), DeliveryID("retry-1", "a"))
	if err != nil {
		t.Fatal(err)
	}
	if dl.Status != StatusSucceeded || dl.Attempts != 3 {
		t.Fatalf("want succeeded after 3 attempts, got %s after %d", dl.Status, dl.Attempts)
	}
	attempts, _ := d.Attempts(context.Background(), dl.ID)
	if len(attempts) != 3 {
		t.Fatalf("audit trail must have 3 attempts, got %d", len(attempts))
	}
	for i, a := range attempts[:2] {
		if a.Outcome != OutcomeFailure || a.Reason == "" {
			t.Fatalf("attempt %d must be a failure with reason: %+v", i, a)
		}
	}
}

func TestDeadLetterAfterMaxAttempts(t *testing.T) {
	d, _ := newTestDispatcher(t)
	defer d.Close()

	ch := NewSimChannel("a").WithFailures(10) // always fails
	d.RegisterChannel(ch, ChannelConfig{
		MaxAttempts: 3,
		BaseBackoff: 20 * time.Millisecond,
		MaxBackoff:  50 * time.Millisecond,
	})

	_, err := d.Submit(context.Background(), Event{ID: "dead-1", Targets: []string{"a"}})
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, "dead letter", 3*time.Second, func() bool {
		dl, err := d.Delivery(context.Background(), DeliveryID("dead-1", "a"))
		return err == nil && dl.Status == StatusDead
	})

	dead, err := d.DeadLetters(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(dead) != 1 || dead[0].ID != DeliveryID("dead-1", "a") {
		t.Fatalf("dead letter set wrong: %+v", dead)
	}
	if dead[0].LastError == "" {
		t.Fatal("dead letter must keep the failure cause")
	}
	if got := len(ch.Received()); got != 0 {
		t.Fatalf("failed delivery must not produce side effects, got %d", got)
	}
	if ch.Calls() != 3 {
		t.Fatalf("want exactly 3 attempts, got %d", ch.Calls())
	}
}

func TestPermanentErrorGoesStraightToDeadLetter(t *testing.T) {
	d, _ := newTestDispatcher(t)
	defer d.Close()

	ch := NewSimChannel("a").WithPermanentFailures(1)
	d.RegisterChannel(ch, ChannelConfig{MaxAttempts: 5})

	_, err := d.Submit(context.Background(), Event{ID: "perm-1", Targets: []string{"a"}})
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, "immediate dead letter", 2*time.Second, func() bool {
		dl, err := d.Delivery(context.Background(), DeliveryID("perm-1", "a"))
		return err == nil && dl.Status == StatusDead
	})
	if ch.Calls() != 1 {
		t.Fatalf("permanent error must not be retried, got %d calls", ch.Calls())
	}
}

// concTracker is a Channel that records the maximum observed concurrency.
type concTracker struct {
	name     string
	latency  time.Duration
	mu       sync.Mutex
	cur, max int
}

func (c *concTracker) Name() string { return c.name }
func (c *concTracker) Deliver(ctx context.Context, d Delivery) error {
	c.mu.Lock()
	c.cur++
	if c.cur > c.max {
		c.max = c.cur
	}
	c.mu.Unlock()
	time.Sleep(c.latency)
	c.mu.Lock()
	c.cur--
	c.mu.Unlock()
	return nil
}
func (c *concTracker) maxConc() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.max
}

func TestConcurrencyLimit(t *testing.T) {
	d, _ := newTestDispatcher(t)
	defer d.Close()

	ch := &concTracker{name: "c", latency: 50 * time.Millisecond}
	d.RegisterChannel(ch, ChannelConfig{Concurrency: 3})

	for i := 0; i < 10; i++ {
		if _, err := d.Submit(context.Background(), Event{
			ID: fmt.Sprintf("conc-%d", i), Targets: []string{"c"},
		}); err != nil {
			t.Fatal(err)
		}
	}
	waitFor(t, "all deliveries", 5*time.Second, func() bool {
		all, _ := d.store.ListDeliveries(context.Background(), "c", StatusSucceeded)
		return len(all) == 10
	})
	if m := ch.maxConc(); m > 3 {
		t.Fatalf("concurrency limit violated: max in-flight = %d", m)
	}
}

func TestRateLimit(t *testing.T) {
	d, _ := newTestDispatcher(t)
	defer d.Close()

	ch := NewSimChannel("r")
	const rate = 50.0
	const n = 8
	d.RegisterChannel(ch, ChannelConfig{Concurrency: 4, RatePerSec: rate})

	start := time.Now()
	for i := 0; i < n; i++ {
		if _, err := d.Submit(context.Background(), Event{
			ID: fmt.Sprintf("rate-%d", i), Targets: []string{"r"},
		}); err != nil {
			t.Fatal(err)
		}
	}
	waitFor(t, "all deliveries", 5*time.Second, func() bool { return len(ch.Received()) == n })
	elapsed := time.Since(start)

	// With a token bucket of capacity 1 at `rate` tokens/sec, n deliveries
	// need at least (n-1)/rate seconds. Allow generous slack for CI jitter.
	min := time.Duration(0.6 * float64(n-1) / rate * float64(time.Second))
	if elapsed < min {
		t.Fatalf("rate limit violated: %d deliveries in %v, want >= %v", n, elapsed, min)
	}
	// And nothing was dropped: all n arrived.
	if got := len(ch.Received()); got != n {
		t.Fatalf("deliveries dropped: %d/%d", got, n)
	}
}

func TestAttemptTimeoutAndCancel(t *testing.T) {
	d, _ := newTestDispatcher(t)
	defer d.Close()

	// Channel that always hangs longer than the attempt timeout.
	hang := NewSimChannel("hang").WithGate(make(chan struct{}))
	d.RegisterChannel(hang, ChannelConfig{
		MaxAttempts:    2,
		BaseBackoff:    20 * time.Millisecond,
		MaxBackoff:     50 * time.Millisecond,
		DeliverTimeout: 40 * time.Millisecond,
	})
	_, err := d.Submit(context.Background(), Event{ID: "to-1", Targets: []string{"hang"}})
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, "timeout-driven dead letter", 3*time.Second, func() bool {
		dl, err := d.Delivery(context.Background(), DeliveryID("to-1", "hang"))
		return err == nil && dl.Status == StatusDead
	})

	// Cancel a scheduled delivery before it fires.
	ch := NewSimChannel("ok")
	d.RegisterChannel(ch, ChannelConfig{})
	_, err = d.Submit(context.Background(), Event{
		ID: "cancel-1", Targets: []string{"ok"}, NotBefore: time.Now().Add(200 * time.Millisecond),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := d.Cancel(context.Background(), DeliveryID("cancel-1", "ok")); err != nil {
		t.Fatal(err)
	}
	time.Sleep(300 * time.Millisecond)
	if got := len(ch.Received()); got != 0 {
		t.Fatalf("canceled delivery must not fire, got %d", got)
	}
	dl, _ := d.Delivery(context.Background(), DeliveryID("cancel-1", "ok"))
	if dl.Status != StatusCanceled {
		t.Fatalf("want canceled, got %s", dl.Status)
	}
}

func TestRestartRecovery(t *testing.T) {
	dir := t.TempDir()
	path := dir + "/store.jsonl"

	// Phase 1: submit one event that succeeds and one scheduled in the
	// future, then "crash" (close) before the scheduled one fires.
	store1, err := OpenFileStore(path)
	if err != nil {
		t.Fatal(err)
	}
	d1 := NewDispatcher(store1)
	ch1 := NewSimChannel("a")
	d1.RegisterChannel(ch1, ChannelConfig{})

	if _, err := d1.Submit(context.Background(), Event{ID: "done-1", Targets: []string{"a"}}); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "first delivery", 2*time.Second, func() bool { return len(ch1.Received()) == 1 })
	if _, err := d1.Submit(context.Background(), Event{
		ID: "later-1", Targets: []string{"a"}, NotBefore: time.Now().Add(150 * time.Millisecond),
	}); err != nil {
		t.Fatal(err)
	}
	if err := d1.Close(); err != nil {
		t.Fatal(err)
	}

	// Phase 2: restart with a fresh channel (no memory of phase 1), recover,
	// and verify: the committed delivery is NOT repeated, the pending
	// scheduled one continues and fires exactly once.
	store2, err := OpenFileStore(path)
	if err != nil {
		t.Fatal(err)
	}
	d2 := NewDispatcher(store2)
	defer d2.Close()
	ch2 := NewSimChannel("a")
	d2.RegisterChannel(ch2, ChannelConfig{})
	if err := d2.Recover(context.Background()); err != nil {
		t.Fatal(err)
	}

	waitFor(t, "recovered scheduled delivery", 3*time.Second, func() bool {
		return len(ch2.Received()) == 1
	})
	if got := ch2.Received()[0]; got != DeliveryID("later-1", "a") {
		t.Fatalf("unexpected delivery after recovery: %s", got)
	}
	// Re-submitting the old event after restart is still a no-op.
	r, err := d2.Submit(context.Background(), Event{ID: "done-1", Targets: []string{"a"}})
	if err != nil {
		t.Fatal(err)
	}
	if !r.Duplicated {
		t.Fatal("dedup state must survive restart")
	}
	time.Sleep(100 * time.Millisecond)
	if got := len(ch2.Received()); got != 1 {
		t.Fatalf("restart caused duplicate delivery: %d", got)
	}
	// The recovered delivery's audit trail explains what happened.
	attempts, _ := d2.Attempts(context.Background(), DeliveryID("later-1", "a"))
	if len(attempts) == 0 {
		t.Fatal("recovered delivery must have an audit trail")
	}
}

func TestRecoveryResetsInterruptedInFlight(t *testing.T) {
	dir := t.TempDir()
	path := dir + "/store.jsonl"

	// Simulate a crash that left a delivery in_flight: write the state
	// directly, then recover with a fresh dispatcher.
	store1, err := OpenFileStore(path)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if _, err := store1.SaveEvent(ctx, Event{ID: "crash-1", Targets: []string{"a"}}); err != nil {
		t.Fatal(err)
	}
	if _, err := store1.SaveDelivery(ctx, Delivery{
		ID: DeliveryID("crash-1", "a"), EventID: "crash-1", Channel: "a",
		Status: StatusInFlight, NotBefore: time.Now(), CreatedAt: time.Now(), UpdatedAt: time.Now(),
	}); err != nil {
		t.Fatal(err)
	}
	store1.Close()

	store2, err := OpenFileStore(path)
	if err != nil {
		t.Fatal(err)
	}
	d2 := NewDispatcher(store2)
	defer d2.Close()
	ch := NewSimChannel("a")
	d2.RegisterChannel(ch, ChannelConfig{})
	if err := d2.Recover(ctx); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "in-flight recovery", 2*time.Second, func() bool { return len(ch.Received()) == 1 })

	attempts, _ := d2.Attempts(ctx, DeliveryID("crash-1", "a"))
	var sawRecoveryNote bool
	for _, a := range attempts {
		if a.Outcome == OutcomeSkipped && a.Reason != "" {
			sawRecoveryNote = true
		}
	}
	if !sawRecoveryNote {
		t.Fatalf("recovery must leave an explainable audit note, got %+v", attempts)
	}
}
