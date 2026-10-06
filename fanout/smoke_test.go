package fanout

import (
	"context"
	"testing"
	"time"
)

func newTestEngine(t *testing.T, clk Clock) *Engine {
	t.Helper()
	eng, err := NewEngine(EngineConfig{Clock: clk})
	if err != nil {
		t.Fatalf("new engine: %v", err)
	}
	return eng
}

func TestSmokeFanout(t *testing.T) {
	clk := NewFakeClock(time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC))
	eng := newTestEngine(t, clk)
	c1 := NewMemoryChannel(MemoryChannelConfig{Name: "c1", Clock: clk})
	c2 := NewMemoryChannel(MemoryChannelConfig{Name: "c2", Clock: clk})
	if err := eng.AddChannel(c1, ChannelConfig{Name: "c1", Concurrency: 1, MaxAttempts: 1}); err != nil {
		t.Fatal(err)
	}
	if err := eng.AddChannel(c2, ChannelConfig{Name: "c2", Concurrency: 1, MaxAttempts: 1}); err != nil {
		t.Fatal(err)
	}
	if err := eng.AddRule(RoutingRule{ID: "r1", EventType: "alert", Channel: "c1"}); err != nil {
		t.Fatal(err)
	}
	if err := eng.AddRule(RoutingRule{ID: "r2", EventType: "alert", Channel: "c2"}); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := eng.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer eng.Shutdown(ctx)

	if _, err := eng.Submit(ctx, Event{ID: "e1", Type: "alert", Payload: []byte("hi")}); err != nil {
		t.Fatal(err)
	}

	// Progress fake clock in a helper goroutine until terminal.
	go func() {
		for i := 0; i < 1000; i++ {
			clk.Advance(time.Millisecond)
			time.Sleep(time.Microsecond)
		}
	}()
	rep, err := eng.WaitForEvent(ctx, "e1")
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range rep.Results {
		if r.Code != ResultSuccess {
			t.Fatalf("channel %s code=%s reason=%s", r.Channel, r.Code, r.Reason)
		}
	}
	if c1.ObservedCount() != 1 || c2.ObservedCount() != 1 {
		t.Fatalf("side effects c1=%d c2=%d", c1.ObservedCount(), c2.ObservedCount())
	}
}
