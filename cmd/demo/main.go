// Command demo runs an end-to-end, self-contained demonstration of the
// fan-out subsystem against a persistent WAL in a temp directory: it fans one
// event to three channels (reliable, needs-retries, always-fails), prints the
// explainable report and dead-letter queue, then reopens the store on a new
// engine to demonstrate crash recovery with no duplicate side effects.
package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/shikejing/notification-fanout-go/fanout"
)

func main() {
	ctx := context.Background()
	dir, err := os.MkdirTemp("", "fanout-demo-")
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(dir)
	wal := filepath.Join(dir, "wal.log")

	clk := fanout.NewRealClock()

	build := func() (*fanout.Engine, *fanout.MemoryChannel, *fanout.MemoryChannel, *fanout.MemoryChannel) {
		eng, err := fanout.NewEngine(fanout.EngineConfig{StorePath: wal, Clock: clk})
		if err != nil {
			panic(err)
		}
		email := fanout.NewMemoryChannel(fanout.MemoryChannelConfig{Name: "email"})
		push := fanout.NewMemoryChannel(fanout.MemoryChannelConfig{Name: "push", FailFirst: 2})
		sms := fanout.NewMemoryChannel(fanout.MemoryChannelConfig{Name: "sms", FailFirst: 1_000_000})
		must(eng.AddChannel(email, fanout.ChannelConfig{Name: "email", MaxAttempts: 1}))
		must(eng.AddChannel(push, fanout.ChannelConfig{
			Name: "push", Concurrency: 2, RatePerSecond: 100, MaxAttempts: 5,
			InitialBackoff: 20 * time.Millisecond, MaxBackoff: time.Second,
		}))
		must(eng.AddChannel(sms, fanout.ChannelConfig{
			Name: "sms", MaxAttempts: 3,
			InitialBackoff: 10 * time.Millisecond, MaxBackoff: 100 * time.Millisecond,
		}))
		must(eng.AddRule(fanout.RoutingRule{ID: "r1", EventType: "order", Channel: "email"}))
		must(eng.AddRule(fanout.RoutingRule{ID: "r2", EventType: "order", Channel: "push"}))
		must(eng.AddRule(fanout.RoutingRule{ID: "r3", EventType: "order", Channel: "sms"}))
		return eng, email, push, sms
	}

	// ---- First run ------------------------------------------------------
	eng, email, push, sms := build()
	if err := eng.Start(ctx); err != nil {
		panic(err)
	}
	if _, err := eng.Submit(ctx, fanout.Event{
		ID: "order-1001", Type: "order", Payload: []byte(`{"amount":42}`),
	}); err != nil {
		panic(err)
	}
	rep, err := eng.WaitForEvent(ctx, "order-1001")
	if err != nil {
		panic(err)
	}
	fmt.Println("== delivery report ==")
	for _, r := range rep.Results {
		fmt.Printf("  channel=%-6s code=%-7s attempts=%d reason=%q\n",
			r.Channel, r.Code, r.Attempts, r.Reason)
	}
	fmt.Printf("side effects: email=%d push=%d sms=%d\n",
		email.ObservedCount(), push.ObservedCount(), sms.ObservedCount())
	fmt.Println("== dead letters ==")
	for _, d := range eng.DeadLetters() {
		fmt.Printf("  %s status=%s attempts=%d reason=%q\n",
			d.ID, d.Status, d.AttemptCount, d.Reason)
	}
	if err := eng.Shutdown(ctx); err != nil {
		panic(err)
	}

	// ---- Restart (crash recovery) --------------------------------------
	fmt.Println("== restarting on same WAL ==")
	eng2, email2, push2, sms2 := build()
	if err := eng2.Start(ctx); err != nil {
		panic(err)
	}
	defer eng2.Shutdown(ctx)
	rep2 := eng2.Report("order-1001")
	for _, r := range rep2.Results {
		fmt.Printf("  recovered channel=%-6s code=%-7s attempts=%d\n",
			r.Channel, r.Code, r.Attempts)
	}
	fmt.Printf("post-restart NEW side effects: email=%d push=%d sms=%d (must be 0)\n",
		email2.ObservedCount(), push2.ObservedCount(), sms2.ObservedCount())
	fmt.Printf("recovered dead letters: %d\n", len(eng2.DeadLetters()))
	fmt.Println("WAL at:", wal)
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}
