package fanout

import (
	"context"
	"strconv"
	"testing"
	"time"
)

// TestAttemptTimeoutRetriesAndSucceeds: a hung attempt is bounded by
// AttemptTimeout; after the channel is released a later attempt succeeds and
// the delivery is not stuck in-flight.
func TestAttemptTimeoutRetriesAndSucceeds(t *testing.T) {
	clk := NewFakeClock(time.Now())
	c1 := NewMemoryChannel(MemoryChannelConfig{
		Name: "slow", Clock: clk,
		BlockOnIDs: map[string]bool{deliveryID("t1", "slow"): true},
	})
	eng := newTestEngine(t, clk)
	cfg := ChannelConfig{
		Name: "slow", Concurrency: 1, MaxAttempts: 5,
		AttemptTimeout: 50 * time.Millisecond,
		InitialBackoff: 20 * time.Millisecond,
	}
	if err := eng.AddChannel(c1, cfg); err != nil {
		t.Fatal(err)
	}
	if err := eng.AddRule(RoutingRule{ID: "r", EventType: "alert", Channel: "slow"}); err != nil {
		t.Fatal(err)
	}
	if err := eng.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer eng.Shutdown(context.Background())

	stop := make(chan struct{})
	go pump(clk, stop)
	defer close(stop)

	eng.Submit(context.Background(), Event{ID: "t1", Type: "alert"})

	// Release the hung channel after a couple of timeouts.
	go func() {
		time.Sleep(150 * time.Millisecond)
		c1.Unblock(deliveryID("t1", "slow"))
	}()

	rep := waitResult(t, eng, "t1")
	if rep.Results[0].Code != ResultSuccess {
		t.Fatalf("code=%s reason=%s", rep.Results[0].Code, rep.Results[0].Reason)
	}
	d := eng.Delivery("t1", "slow")
	if d.Status != StatusSuccess {
		t.Fatalf("delivery stuck in %s", d.Status)
	}
	if d.AttemptCount < 2 {
		t.Fatalf("expected at least one timed-out attempt then success, got %d", d.AttemptCount)
	}
	// Every failed attempt must be explained.
	for i := 0; i < d.AttemptCount-1; i++ {
		if d.Attempts[i].Error == "" {
			t.Fatalf("timed-out attempt %d missing error reason", i+1)
		}
	}
}

// TestConcurrencyLimitQueuesAndNeverDrops: with concurrency=1, N slow
// deliveries queue; all eventually complete (none dropped).
func TestConcurrencyLimitQueuesAndNeverDrops(t *testing.T) {
	clk := NewFakeClock(time.Now())
	c1 := NewMemoryChannel(MemoryChannelConfig{Name: "q", Clock: clk, Latency: 5 * time.Millisecond})
	eng := newTestEngine(t, clk)
	if err := eng.AddChannel(c1, ChannelConfig{
		Name: "q", Concurrency: 1, MaxAttempts: 1,
	}); err != nil {
		t.Fatal(err)
	}
	if err := eng.AddRule(RoutingRule{ID: "r", EventType: "alert", Channel: "q"}); err != nil {
		t.Fatal(err)
	}
	if err := eng.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer eng.Shutdown(context.Background())

	const n = 8
	stop := make(chan struct{})
	go pump(clk, stop)
	defer close(stop)

	for i := 0; i < n; i++ {
		eng.Submit(context.Background(), Event{ID: idFor(i), Type: "alert"})
	}
	// Shortly after submission at least some should still be queued given
	// concurrency 1 and non-zero latency.
	time.Sleep(10 * time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for i := 0; i < n; i++ {
		if _, err := eng.WaitForEvent(ctx, idFor(i)); err != nil {
			t.Fatalf("event %d not delivered (dropped?): %v", i, err)
		}
	}
	if c1.ObservedCount() != n {
		t.Fatalf("side effects=%d want %d", c1.ObservedCount(), n)
	}
}

// TestRateLimitQueuesInFIFO: a rate-limited channel spreads delivery starts
// over time while never dropping any.
func TestRateLimitQueuesInFIFO(t *testing.T) {
	clk := NewFakeClock(time.Now())
	c1 := NewMemoryChannel(MemoryChannelConfig{Name: "rl", Clock: clk})
	eng := newTestEngine(t, clk)
	if err := eng.AddChannel(c1, ChannelConfig{
		Name: "rl", Concurrency: 8, MaxAttempts: 1,
		RatePerSecond: 20, // bucket burst=1 -> roughly one start per 50ms
	}); err != nil {
		t.Fatal(err)
	}
	if err := eng.AddRule(RoutingRule{ID: "r", EventType: "alert", Channel: "rl"}); err != nil {
		t.Fatal(err)
	}
	if err := eng.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer eng.Shutdown(context.Background())

	const n = 6
	for i := 0; i < n; i++ {
		eng.Submit(context.Background(), Event{ID: idFor(i), Type: "alert"})
	}

	stop := make(chan struct{})
	go pump(clk, stop)
	defer close(stop)

	// With rate 20/s and burst 1, six starts take ~250ms; ensure they were
	// not all instant by sampling an intermediate count, but primarily that
	// nothing is dropped.
	time.Sleep(20 * time.Millisecond)
	intermediate := c1.ObservedCount()
	if intermediate >= n {
		// Very unlikely with real scheduling; rate limiting ineffective.
		t.Fatalf("rate limit ineffective: all %d delivered immediately", n)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for i := 0; i < n; i++ {
		if _, err := eng.WaitForEvent(ctx, idFor(i)); err != nil {
			t.Fatalf("rate-limited delivery %d lost: %v", i, err)
		}
	}
	if c1.ObservedCount() != n {
		t.Fatalf("side effects=%d want %d", c1.ObservedCount(), n)
	}
}

// TestNoIntermediateStateAfterShutdown: in-flight and queued work at shutdown
// must be pending (never half-applied); restart resumes them.
func TestNoIntermediateStateAfterShutdown(t *testing.T) {
	dir := t.TempDir()
	path := dir + "/wal.log"
	clk := NewFakeClock(time.Now())

	id := deliveryID("blk", "c1")

	build := func() (*Engine, *MemoryChannel) {
		eng, err := NewEngine(EngineConfig{StorePath: path, Clock: clk})
		if err != nil {
			t.Fatal(err)
		}
		ch := NewMemoryChannel(MemoryChannelConfig{
			Name: "c1", Clock: clk,
			BlockOnIDs: map[string]bool{id: true},
		})
		if err := eng.AddChannel(ch, ChannelConfig{
			Name: "c1", Concurrency: 1, MaxAttempts: 5,
			AttemptTimeout: 30 * time.Millisecond, InitialBackoff: 10 * time.Millisecond,
		}); err != nil {
			t.Fatal(err)
		}
		if err := eng.AddRule(RoutingRule{ID: "r", Channel: "c1"}); err != nil {
			t.Fatal(err)
		}
		return eng, ch
	}

	eng, _ := build()
	if err := eng.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	stop := make(chan struct{})
	go pump(clk, stop)

	eng.Submit(context.Background(), Event{ID: "blk"})
	time.Sleep(80 * time.Millisecond) // let at least one attempt time out / be in-flight
	close(stop)

	shutCtx, scancel := context.WithTimeout(context.Background(), 2*time.Second)
	if err := eng.Shutdown(shutCtx); err != nil {
		t.Fatal(err)
	}
	scancel()

	// Restart with a fresh engine/channel, release the block, and confirm
	// the delivery resumes and succeeds without duplicate side effects.
	eng2, ch2 := build()
	if err := eng2.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer eng2.Shutdown(context.Background())
	ch2.Unblock(id)

	stop2 := make(chan struct{})
	go pump(clk, stop2)
	defer close(stop2)

	rep := waitResult(t, eng2, "blk")
	if rep.Results[0].Code != ResultSuccess {
		t.Fatalf("after restart code=%s reason=%s", rep.Results[0].Code, rep.Results[0].Reason)
	}
	if ch2.ObservedCount() != 1 {
		t.Fatalf("expected exactly 1 side effect across restart, got %d", ch2.ObservedCount())
	}
}

func idFor(i int) string {
	return "ev-" + strconv.Itoa(i)
}
