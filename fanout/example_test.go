package fanout_test

import (
	"context"
	"fmt"
	"time"

	"github.com/shikejing/notification-fanout-go/fanout"
)

// Example demonstrates the full lifecycle: configure two channels with
// different failure profiles, submit a scheduled event, wait for terminal
// state and inspect the explainable report and dead-letter queue.
func Example() {
	ctx := context.Background()
	clk := fanout.NewFakeClock(time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC))

	// One reliable channel, one channel whose first two attempts fail.
	okCh := fanout.NewMemoryChannel(fanout.MemoryChannelConfig{Name: "email", Clock: clk})
	badCh := fanout.NewMemoryChannel(fanout.MemoryChannelConfig{Name: "sms", Clock: clk, FailFirst: 2})

	eng, err := fanout.NewEngine(fanout.EngineConfig{Clock: clk})
	if err != nil {
		panic(err)
	}
	if err := eng.AddChannel(okCh, fanout.ChannelConfig{
		Name: "email", MaxAttempts: 1,
	}); err != nil {
		panic(err)
	}
	if err := eng.AddChannel(badCh, fanout.ChannelConfig{
		Name: "sms", MaxAttempts: 5,
		InitialBackoff: 100 * time.Millisecond,
		MaxBackoff:     time.Second,
	}); err != nil {
		panic(err)
	}
	_ = eng.AddRule(fanout.RoutingRule{ID: "r-email", EventType: "order", Channel: "email"})
	_ = eng.AddRule(fanout.RoutingRule{ID: "r-sms", EventType: "order", Channel: "sms"})

	if err := eng.Start(ctx); err != nil {
		panic(err)
	}
	defer eng.Shutdown(ctx)

	// Drive the fake clock in the background so timers/backoffs elapse.
	done := make(chan struct{})
	go func() {
		tk := time.NewTicker(time.Millisecond)
		defer tk.Stop()
		for {
			select {
			case <-done:
				return
			case <-tk.C:
				clk.Advance(50 * time.Millisecond)
			}
		}
	}()
	defer close(done)

	_, _ = eng.Submit(ctx, fanout.Event{
		ID: "order-1", Type: "order", Payload: []byte(`{"n":42}`),
	})

	rep, _ := eng.WaitForEvent(ctx, "order-1")
	for _, r := range rep.Results {
		fmt.Printf("%s -> %s (attempts=%d)\n", r.Channel, r.Code, r.Attempts)
	}
	fmt.Println("email side effects:", okCh.ObservedCount())
	fmt.Println("dead letters:", len(eng.DeadLetters()))

	// Submitting the same logical notification again is a no-op downstream.
	_, _ = eng.Submit(ctx, fanout.Event{ID: "order-1", Type: "order"})
	time.Sleep(30 * time.Millisecond)
	fmt.Println("email side effects after duplicate:", okCh.ObservedCount())

	// Output:
	// email -> success (attempts=1)
	// sms -> success (attempts=3)
	// email side effects: 1
	// dead letters: 0
	// email side effects after duplicate: 1
}
