package fanout

import (
	"context"
	"strings"
	"testing"
	"time"
)

func TestRetryBackoffThenSuccess(t *testing.T) {
	clk := NewFakeClock(time.Now())
	// Fail the first 2 fresh attempts, succeed on the 3rd.
	c1 := NewMemoryChannel(MemoryChannelConfig{Name: "retry", Clock: clk, FailFirst: 2})
	c2 := NewMemoryChannel(MemoryChannelConfig{Name: "other", Clock: clk})
	eng := setupFanout(t, clk, c1, c2,
		ChannelConfig{
			Name: "retry", Concurrency: 1, MaxAttempts: 5,
			InitialBackoff: 100 * time.Millisecond, MaxBackoff: time.Second,
		},
		ChannelConfig{Name: "other", Concurrency: 1, MaxAttempts: 1})

	stop := make(chan struct{})
	go pump(clk, stop)
	defer close(stop)

	eng.Submit(context.Background(), Event{ID: "r1", Type: "alert"})
	rep := waitResult(t, eng, "r1")

	var r DeliveryResult
	for _, x := range rep.Results {
		if x.Channel == "retry" {
			r = x
		}
	}
	if r.Code != ResultSuccess {
		t.Fatalf("retry channel code=%s reason=%s", r.Code, r.Reason)
	}
	if r.Attempts != 3 {
		t.Fatalf("want 3 attempts, got %d", r.Attempts)
	}
	// Exactly one downstream side effect despite 3 attempts.
	if c1.ObservedCount() != 1 {
		t.Fatalf("side effects=%d want 1", c1.ObservedCount())
	}
	// Attempt history carries the two failures with readable errors.
	d := eng.Delivery("r1", "retry")
	if len(d.Attempts) != 3 {
		t.Fatalf("attempt history len=%d", len(d.Attempts))
	}
	if d.Attempts[0].Error == "" || d.Attempts[1].Error == "" {
		t.Fatalf("first two attempts must record failure reasons: %+v", d.Attempts)
	}
	if d.Attempts[2].Error != "" {
		t.Fatalf("third attempt must succeed, got %q", d.Attempts[2].Error)
	}
	// Backoff must be visible in attempt start spacing.
	gap1 := d.Attempts[1].StartedAt.Sub(d.Attempts[0].StartedAt)
	gap2 := d.Attempts[2].StartedAt.Sub(d.Attempts[1].StartedAt)
	if gap1 < 100*time.Millisecond {
		t.Fatalf("first backoff too short: %v", gap1)
	}
	if gap2 < 200*time.Millisecond {
		t.Fatalf("second backoff must double: %v", gap2)
	}
}

func TestMaxAttemptsDeadLetter(t *testing.T) {
	clk := NewFakeClock(time.Now())
	c1 := NewMemoryChannel(MemoryChannelConfig{Name: "dead", Clock: clk, FailFirst: 1000})
	c2 := NewMemoryChannel(MemoryChannelConfig{Name: "fine", Clock: clk})
	eng := setupFanout(t, clk, c1, c2,
		ChannelConfig{
			Name: "dead", Concurrency: 1, MaxAttempts: 3,
			InitialBackoff: time.Millisecond, MaxBackoff: 5 * time.Millisecond,
		},
		ChannelConfig{Name: "fine", MaxAttempts: 1})

	stop := make(chan struct{})
	go pump(clk, stop)
	defer close(stop)

	eng.Submit(context.Background(), Event{ID: "d1", Type: "alert"})
	rep := waitResult(t, eng, "d1")

	var deadR, fineR DeliveryResult
	for _, x := range rep.Results {
		switch x.Channel {
		case "dead":
			deadR = x
		case "fine":
			fineR = x
		}
	}
	if deadR.Code != ResultFailed {
		t.Fatalf("dead channel code=%s reason=%s", deadR.Code, deadR.Reason)
	}
	if fineR.Code != ResultSuccess {
		t.Fatalf("fine channel must succeed independently: %s", fineR.Code)
	}
	if !strings.Contains(deadR.Reason, "max attempts exhausted") ||
		!strings.Contains(deadR.Reason, "injected failure") {
		t.Fatalf("dead reason not traceable: %q", deadR.Reason)
	}
	if deadR.Attempts != 3 {
		t.Fatalf("want exactly 3 attempts before dead-letter, got %d", deadR.Attempts)
	}
	if c1.ObservedCount() != 0 {
		t.Fatalf("failing channel must have no side effect")
	}

	dl := eng.DeadLetters()
	if len(dl) != 1 || dl[0].ID != deliveryID("d1", "dead") {
		t.Fatalf("dead letters=%+v", dl)
	}
	if len(dl[0].Attempts) != 3 {
		t.Fatalf("dead letter must carry full attempt history")
	}
}

// TestOneChannelFailureDoesNotBlockOthers verifies fan-out isolation: a
// permanently failing target never delays the healthy target, even though it
// eventually dead-letters.
func TestOneChannelFailureDoesNotBlockOthers(t *testing.T) {
	clk := NewFakeClock(time.Now())
	c1 := NewMemoryChannel(MemoryChannelConfig{Name: "hung", Clock: clk, FailFirst: 1000})
	c2 := NewMemoryChannel(MemoryChannelConfig{Name: "healthy", Clock: clk})
	eng := setupFanout(t, clk, c1, c2,
		ChannelConfig{Name: "hung", Concurrency: 1, MaxAttempts: 1, InitialBackoff: time.Millisecond},
		ChannelConfig{Name: "healthy", Concurrency: 1, MaxAttempts: 1})

	stop := make(chan struct{})
	go pump(clk, stop)
	defer close(stop)

	eng.Submit(context.Background(), Event{ID: "iso1", Type: "alert"})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	for {
		rep := eng.Report("iso1")
		healthyOK := false
		for _, r := range rep.Results {
			if r.Channel == "healthy" && r.Code == ResultSuccess {
				healthyOK = true
			}
		}
		if healthyOK {
			break
		}
		if ctx.Err() != nil {
			t.Fatalf("healthy channel blocked by failing sibling: %+v", rep.Results)
		}
		time.Sleep(5 * time.Millisecond)
	}
}
