package fanout

import (
	"context"
	"testing"
	"time"
)

// startEngine builds an engine on a persistent WAL with two channels.
func startEngine(t *testing.T, path string, clk Clock, failA, failB int) (*Engine, *MemoryChannel, *MemoryChannel) {
	t.Helper()
	eng, err := NewEngine(EngineConfig{StorePath: path, Clock: clk})
	if err != nil {
		t.Fatal(err)
	}
	a := NewMemoryChannel(MemoryChannelConfig{Name: "a", Clock: clk, FailFirst: failA})
	b := NewMemoryChannel(MemoryChannelConfig{Name: "b", Clock: clk, FailFirst: failB})
	if err := eng.AddChannel(a, ChannelConfig{
		Name: "a", MaxAttempts: 4, InitialBackoff: 5 * time.Millisecond, MaxBackoff: 50 * time.Millisecond,
	}); err != nil {
		t.Fatal(err)
	}
	if err := eng.AddChannel(b, ChannelConfig{
		Name: "b", MaxAttempts: 4, InitialBackoff: 5 * time.Millisecond, MaxBackoff: 50 * time.Millisecond,
	}); err != nil {
		t.Fatal(err)
	}
	if err := eng.AddRule(RoutingRule{ID: "ra", EventType: "alert", Channel: "a"}); err != nil {
		t.Fatal(err)
	}
	if err := eng.AddRule(RoutingRule{ID: "rb", EventType: "alert", Channel: "b"}); err != nil {
		t.Fatal(err)
	}
	if err := eng.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	return eng, a, b
}

// TestCrashRecoveryNoDuplicateAndResume: a channel that needs retries is
// restarted mid-flight; the first engine is killed before it succeeds. On
// restart the delivery resumes and produces exactly one side effect.
func TestCrashRecoveryNoDuplicateAndResume(t *testing.T) {
	path := t.TempDir() + "/wal.log"
	clk := NewFakeClock(time.Now())

	// First run: channel "a" always fails (will not reach success before
	// crash); channel "b" succeeds immediately.
	eng, a, b := startEngine(t, path, clk, 1000, 0)
	stop := make(chan struct{})
	go pump(clk, stop)

	eng.Submit(context.Background(), Event{ID: "ev1", Type: "alert", Payload: []byte("p")})
	// Let b succeed and a fail at least once.
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	for b.ObservedCount() == 0 {
		if ctx.Err() != nil {
			t.Fatal("b never succeeded pre-crash")
		}
		time.Sleep(2 * time.Millisecond)
	}
	cancel()
	time.Sleep(30 * time.Millisecond) // allow a to log failed attempts
	close(stop)

	// "Crash": shut down without waiting for retries to finish.
	sctx, sc := context.WithTimeout(context.Background(), 2*time.Second)
	eng.Shutdown(sctx)
	sc()

	if a.ObservedCount() != 0 {
		t.Fatalf("a should never have succeeded pre-crash")
	}
	aAttemptsBefore := 0
	if d := eng.Delivery("ev1", "a"); d != nil {
		aAttemptsBefore = d.AttemptCount
	}
	if aAttemptsBefore == 0 {
		t.Fatalf("expected at least one persisted attempt for a")
	}

	// Restart on the same WAL: channel a is now healthy.
	eng2, a2, b2 := startEngine(t, path, clk, 0, 0)
	stop2 := make(chan struct{})
	go pump(clk, stop2)
	defer close(stop2)
	defer eng2.Shutdown(context.Background())

	rep := waitResult(t, eng2, "ev1")
	for _, r := range rep.Results {
		if r.Code != ResultSuccess {
			t.Fatalf("channel %s after restart: %s (%s)", r.Channel, r.Code, r.Reason)
		}
	}
	if a2.ObservedCount() != 1 {
		t.Fatalf("a side effects after restart=%d want 1", a2.ObservedCount())
	}
	// b had already succeeded before the crash; replay must not re-deliver.
	if b2.ObservedCount() != 0 {
		t.Fatalf("b was re-delivered after restart: %d (new channel instance must see 0)", b2.ObservedCount())
	}
	// The post-restart delivery should continue the attempt count rather
	// than silently reset to 1: total attempts across restart >= before+1
	// and the WAL history is preserved.
	d := eng2.Delivery("ev1", "a")
	if d.AttemptCount < aAttemptsBefore+1 {
		t.Fatalf("attempt count not continued: before=%d after=%d", aAttemptsBefore, d.AttemptCount)
	}
	if len(d.Attempts) < aAttemptsBefore+1 {
		t.Fatalf("attempt history truncated by recovery")
	}
}

// TestCrashRecoveryRepeatedRestarts: multiple restart cycles converge without
// duplicates (idempotency across replays).
func TestCrashRecoveryRepeatedRestarts(t *testing.T) {
	path := t.TempDir() + "/wal.log"
	clk := NewFakeClock(time.Now())

	eng, a, b := startEngine(t, path, clk, 1000, 0)
	stop := make(chan struct{})
	go pump(clk, stop)
	eng.Submit(context.Background(), Event{ID: "ev2", Type: "alert"})
	time.Sleep(20 * time.Millisecond)
	close(stop)
	eng.Shutdown(context.Background())
	_ = a
	_ = b

	var healthyA *MemoryChannel
	for i := 0; i < 3; i++ {
		eng2, a2, _ := startEngine(t, path, clk, 1000, 0)
		st := make(chan struct{})
		go pump(clk, st)
		time.Sleep(15 * time.Millisecond)
		close(st)
		eng2.Shutdown(context.Background())
		healthyA = a2
	}
	_ = healthyA

	// Final restart with a healthy channel completes it.
	eng3, a3, _ := startEngine(t, path, clk, 0, 0)
	st := make(chan struct{})
	go pump(clk, st)
	defer close(st)
	defer eng3.Shutdown(context.Background())
	rep := waitResult(t, eng3, "ev2")
	for _, r := range rep.Results {
		if r.Channel == "a" && r.Code != ResultSuccess {
			t.Fatalf("a: %s (%s)", r.Code, r.Reason)
		}
	}
	if a3.ObservedCount() != 1 {
		t.Fatalf("a side effects=%d want 1 across 4 engine lifecycles", a3.ObservedCount())
	}
}

// TestDeadLetterRecoveredAfterRestart: a delivery already dead before crash
// is reported as dead after replay and never retried.
func TestDeadLetterRecoveredAfterRestart(t *testing.T) {
	path := t.TempDir() + "/wal.log"
	clk := NewFakeClock(time.Now())

	eng, a, _ := startEngine(t, path, clk, 1000, 0)
	stop := make(chan struct{})
	go pump(clk, stop)
	eng.Submit(context.Background(), Event{ID: "ev3", Type: "alert"})
	waitResult(t, eng, "ev3")
	close(stop)
	eng.Shutdown(context.Background())
	if len(eng.DeadLetters()) != 1 {
		t.Fatalf("precondition: 1 dead letter")
	}
	sideBefore := a.ObservedCount()

	// Restart still-failing; the dead delivery must not be retried.
	eng2, a2, _ := startEngine(t, path, clk, 1000, 0)
	defer eng2.Shutdown(context.Background())
	st := make(chan struct{})
	go pump(clk, st)
	defer close(st)
	time.Sleep(100 * time.Millisecond)

	if len(eng2.DeadLetters()) != 1 {
		t.Fatalf("dead letter lost on recovery: %d", len(eng2.DeadLetters()))
	}
	d := eng2.Delivery("ev3", "a")
	if d.Status != StatusFailed {
		t.Fatalf("recovered status=%s want failed", d.Status)
	}
	if a2.ObservedCount() != sideBefore {
		t.Fatalf("dead delivery was retried after restart")
	}
}

// TestScheduledDeliveryResumesAfterRestart: an event scheduled in the future
// that has not fired by shutdown fires after restart, exactly once.
func TestScheduledDeliveryResumesAfterRestart(t *testing.T) {
	path := t.TempDir() + "/wal.log"
	start := time.Date(2025, 3, 1, 0, 0, 0, 0, time.UTC)
	clk := NewFakeClock(start)

	eng, a, _ := startEngine(t, path, clk, 0, 0)
	nb := start.Add(time.Minute)
	eng.Submit(context.Background(), Event{ID: "ev4", Type: "alert", NotBefore: nb})
	// Advance only halfway, then crash: nothing delivered yet.
	clk.Advance(30 * time.Second)
	time.Sleep(30 * time.Millisecond)
	if a.ObservedCount() != 0 {
		t.Fatalf("scheduled delivery fired early")
	}
	eng.Shutdown(context.Background())

	eng2, a2, _ := startEngine(t, path, clk, 0, 0)
	defer eng2.Shutdown(context.Background())

	stop := make(chan struct{})
	go func() {
		tr := time.NewTicker(time.Microsecond * 200)
		defer tr.Stop()
		for {
			select {
			case <-stop:
				return
			case <-tr.C:
				clk.Advance(time.Second)
			}
		}
	}()
	defer close(stop)

	rep := waitResult(t, eng2, "ev4")
	if rep.Results[0].Code != ResultSuccess {
		t.Fatalf("scheduled resume: %s (%s)", rep.Results[0].Code, rep.Results[0].Reason)
	}
	if a2.ObservedCount() != 1 {
		t.Fatalf("scheduled delivery observed=%d want 1", a2.ObservedCount())
	}
}
