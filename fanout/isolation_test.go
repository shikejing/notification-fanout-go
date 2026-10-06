package fanout

import (
	"context"
	"testing"
	"time"
)

// TestBlockedChannelDoesNotAffectOthers: one target hangs forever (its
// attempts time out and it eventually dead-letters) while a sibling on the
// same event still delivers promptly. Per-channel schedulers isolate
// availability, latency and failure modes.
func TestBlockedChannelDoesNotAffectOthers(t *testing.T) {
	clk := NewFakeClock(time.Now())
	blockedID := deliveryID("iso-blk", "stuck")
	stuck := NewMemoryChannel(MemoryChannelConfig{
		Name: "stuck", Clock: clk,
		BlockOnIDs: map[string]bool{blockedID: true}, // never released
	})
	fast := NewMemoryChannel(MemoryChannelConfig{Name: "fast", Clock: clk})

	eng := newTestEngine(t, clk)
	if err := eng.AddChannel(stuck, ChannelConfig{
		Name: "stuck", Concurrency: 1, MaxAttempts: 2,
		AttemptTimeout: 30 * time.Millisecond, InitialBackoff: 10 * time.Millisecond,
	}); err != nil {
		t.Fatal(err)
	}
	if err := eng.AddChannel(fast, ChannelConfig{Name: "fast", MaxAttempts: 1}); err != nil {
		t.Fatal(err)
	}
	if err := eng.AddRule(RoutingRule{ID: "r1", EventType: "alert", Channel: "stuck"}); err != nil {
		t.Fatal(err)
	}
	if err := eng.AddRule(RoutingRule{ID: "r2", EventType: "alert", Channel: "fast"}); err != nil {
		t.Fatal(err)
	}
	if err := eng.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer eng.Shutdown(context.Background())

	stop := make(chan struct{})
	go pump(clk, stop)
	defer close(stop)

	eng.Submit(context.Background(), Event{ID: "iso-blk", Type: "alert"})

	// The fast channel must succeed promptly even though "stuck" is hung.
	deadline := time.Now().Add(2 * time.Second)
	for fast.ObservedCount() == 0 {
		if time.Now().After(deadline) {
			t.Fatal("fast channel was blocked by stuck sibling")
		}
		time.Sleep(2 * time.Millisecond)
	}

	// The stuck channel exhausts attempts and dead-letters with traceable
	// timeout reasons; the event as a whole is still explainable.
	rep := waitResult(t, eng, "iso-blk")
	var gotStuck, gotFast DeliveryResult
	for _, r := range rep.Results {
		switch r.Channel {
		case "stuck":
			gotStuck = r
		case "fast":
			gotFast = r
		}
	}
	if gotFast.Code != ResultSuccess {
		t.Fatalf("fast: %s (%s)", gotFast.Code, gotFast.Reason)
	}
	if gotStuck.Code != ResultFailed {
		t.Fatalf("stuck: %s (%s)", gotStuck.Code, gotStuck.Reason)
	}
	if gotStuck.Attempts != 2 {
		t.Fatalf("stuck attempts=%d want 2", gotStuck.Attempts)
	}
	d := eng.Delivery("iso-blk", "stuck")
	for _, a := range d.Attempts {
		if a.Error == "" {
			t.Fatal("timed-out attempt missing reason")
		}
	}
	if stuck.ObservedCount() != 0 || fast.ObservedCount() != 1 {
		t.Fatalf("side effects stuck=%d fast=%d", stuck.ObservedCount(), fast.ObservedCount())
	}
}
