package fanout

import (
	"context"
	"sync"
	"testing"
	"time"
)

// pump advances a FakeClock in the background until stop.
func pump(clk *FakeClock, stop <-chan struct{}) {
	t := time.NewTicker(time.Microsecond * 200)
	defer t.Stop()
	for {
		select {
		case <-stop:
			return
		case <-t.C:
			clk.Advance(time.Millisecond)
		}
	}
}

// waitResult blocks until the event is terminal or the test deadline.
func waitResult(t *testing.T, eng *Engine, id string) *EventReport {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	rep, err := eng.WaitForEvent(ctx, id)
	if err != nil {
		t.Fatalf("wait %s: %v", id, err)
	}
	return rep
}

func setupFanout(t *testing.T, clk Clock, c1, c2 *MemoryChannel, c1cfg, c2cfg ChannelConfig) *Engine {
	t.Helper()
	eng := newTestEngine(t, clk)
	if err := eng.AddChannel(c1, c1cfg); err != nil {
		t.Fatal(err)
	}
	if err := eng.AddChannel(c2, c2cfg); err != nil {
		t.Fatal(err)
	}
	if err := eng.AddRule(RoutingRule{ID: "r1", EventType: "alert", Channel: c1.cfg.Name}); err != nil {
		t.Fatal(err)
	}
	if err := eng.AddRule(RoutingRule{ID: "r2", EventType: "alert", Channel: c2.cfg.Name}); err != nil {
		t.Fatal(err)
	}
	if err := eng.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = eng.Shutdown(context.Background()) })
	return eng
}

func TestDuplicateInputSingleSideEffect(t *testing.T) {
	clk := NewFakeClock(time.Now())
	c1 := NewMemoryChannel(MemoryChannelConfig{Name: "c1", Clock: clk})
	c2 := NewMemoryChannel(MemoryChannelConfig{Name: "c2", Clock: clk})
	base := ChannelConfig{Concurrency: 2, MaxAttempts: 1}
	eng := setupFanout(t, clk, c1, c2,
		withName(base, "c1"), withName(base, "c2"))

	stop := make(chan struct{})
	go pump(clk, stop)
	defer close(stop)

	ctx := context.Background()
	for i := 0; i < 5; i++ {
		rep, err := eng.Submit(ctx, Event{ID: "dup-1", Type: "alert", Payload: []byte("x")})
		if err != nil {
			t.Fatal(err)
		}
		_ = rep
	}
	rep := waitResult(t, eng, "dup-1")
	if len(rep.Results) != 2 {
		t.Fatalf("want 2 channel results, got %d", len(rep.Results))
	}
	for _, r := range rep.Results {
		if r.Code != ResultSuccess {
			t.Fatalf("%s: %s (%s)", r.Channel, r.Code, r.Reason)
		}
	}
	if c1.ObservedCount() != 1 || c2.ObservedCount() != 1 {
		t.Fatalf("side effects c1=%d c2=%d, want 1 each", c1.ObservedCount(), c2.ObservedCount())
	}
	// The duplicate submission returns the same logical report.
	r2, _ := eng.Submit(ctx, Event{ID: "dup-1", Type: "alert"})
	if len(r2.Results) != 2 {
		t.Fatalf("duplicate submit changed fan-out width: %d", len(r2.Results))
	}
}

func TestScheduledNotBeforeAndLateTrigger(t *testing.T) {
	start := time.Date(2025, 1, 1, 12, 0, 0, 0, time.UTC)
	clk := NewFakeClock(start)
	c1 := NewMemoryChannel(MemoryChannelConfig{Name: "c1", Clock: clk})
	c2 := NewMemoryChannel(MemoryChannelConfig{Name: "c2", Clock: clk})
	base := ChannelConfig{Concurrency: 1, MaxAttempts: 1}
	eng := setupFanout(t, clk, c1, c2,
		withName(base, "c1"), withName(base, "c2"))

	notBefore := start.Add(10 * time.Second)
	ctx := context.Background()
	if _, err := eng.Submit(ctx, Event{ID: "sched-1", Type: "alert", NotBefore: notBefore}); err != nil {
		t.Fatal(err)
	}

	// Before NotBefore the clock must not produce a side effect even when we
	// advance almost to the deadline.
	clk.Advance(9 * time.Second)
	time.Sleep(20 * time.Millisecond)
	if c1.ObservedCount() != 0 || c2.ObservedCount() != 0 {
		t.Fatalf("delivery happened before NotBefore: c1=%d c2=%d", c1.ObservedCount(), c2.ObservedCount())
	}
	if d := eng.Delivery("sched-1", "c1"); d.Status == StatusSuccess {
		t.Fatalf("delivery success before scheduled time")
	}

	stop := make(chan struct{})
	go pump(clk, stop)
	defer close(stop)

	rep := waitResult(t, eng, "sched-1")
	for _, r := range rep.Results {
		if r.Code != ResultSuccess {
			t.Fatalf("%s: %s (%s)", r.Channel, r.Code, r.Reason)
		}
	}

	// Simulate a *late trigger*: after terminal success, submit again. It
	// must be deduped and never re-deliver.
	if _, err := eng.Submit(ctx, Event{ID: "sched-1", Type: "alert", NotBefore: notBefore}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(30 * time.Millisecond)
	if c1.ObservedCount() != 1 || c2.ObservedCount() != 1 {
		t.Fatalf("late trigger caused duplicate: c1=%d c2=%d", c1.ObservedCount(), c2.ObservedCount())
	}
}

func TestSkippedChannelDisabled(t *testing.T) {
	clk := NewFakeClock(time.Now())
	c1 := NewMemoryChannel(MemoryChannelConfig{Name: "c1", Clock: clk})
	c2 := NewMemoryChannel(MemoryChannelConfig{Name: "off", Clock: clk})
	eng := setupFanout(t, clk, c1, c2,
		withName(ChannelConfig{MaxAttempts: 1}, "c1"),
		withName(ChannelConfig{MaxAttempts: 1, Disabled: true}, "off"))

	stop := make(chan struct{})
	go pump(clk, stop)
	defer close(stop)

	eng.Submit(context.Background(), Event{ID: "ev-off", Type: "alert"})
	rep := waitResult(t, eng, "ev-off")
	var sawSkip, sawOK bool
	for _, r := range rep.Results {
		if r.Channel == "off" {
			if r.Code != ResultSkipped || r.Reason != SkipChannelDisabled {
				t.Fatalf("off: code=%s reason=%q", r.Code, r.Reason)
			}
			sawSkip = true
		}
		if r.Channel == "c1" && r.Code == ResultSuccess {
			sawOK = true
		}
	}
	if !sawSkip || !sawOK {
		t.Fatalf("expected one skip one success, got %+v", rep.Results)
	}
	if c2.ObservedCount() != 0 {
		t.Fatalf("disabled channel produced side effect")
	}
}

func withName(c ChannelConfig, name string) ChannelConfig {
	c.Name = name
	return c
}

// TestConcurrentDuplicateSubmitsDedup: many goroutines submit the same event
// concurrently; the downstream observes exactly one side effect per channel.
func TestConcurrentDuplicateSubmitsDedup(t *testing.T) {
	clk := NewFakeClock(time.Now())
	c1 := NewMemoryChannel(MemoryChannelConfig{Name: "c1", Clock: clk})
	c2 := NewMemoryChannel(MemoryChannelConfig{Name: "c2", Clock: clk})
	eng := setupFanout(t, clk, c1, c2,
		withName(ChannelConfig{MaxAttempts: 1}, "c1"),
		withName(ChannelConfig{MaxAttempts: 1}, "c2"))

	stop := make(chan struct{})
	go pump(clk, stop)
	defer close(stop)

	const n = 32
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = eng.Submit(context.Background(), Event{ID: "concurrent-1", Type: "alert"})
		}()
	}
	wg.Wait()

	rep := waitResult(t, eng, "concurrent-1")
	if len(rep.Results) != 2 {
		t.Fatalf("fan-out width=%d want 2", len(rep.Results))
	}
	if c1.ObservedCount() != 1 || c2.ObservedCount() != 1 {
		t.Fatalf("side effects c1=%d c2=%d, want 1 each",
			c1.ObservedCount(), c2.ObservedCount())
	}
}
