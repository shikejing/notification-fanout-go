package fanout

import (
	"context"
	"testing"
	"time"
)

// TestLoopbackChannelRealNetwork exercises a real HTTP path over loopback
// with server-side idempotency: retries reuse the same Idempotency-Key and
// the server records exactly one side effect.
func TestLoopbackChannelRealNetwork(t *testing.T) {
	srv := NewLoopbackServer(0, 1) // first attempt 500, second 200
	url := srv.Start()
	defer srv.Close()

	ch, err := NewLoopbackChannel(LoopbackConfig{Name: "http", URL: url})
	if err != nil {
		t.Fatal(err)
	}
	if ch.Kind() != KindLoopback || ch.Name() != "http" {
		t.Fatalf("kind/name mismatch: %s/%s", ch.Kind(), ch.Name())
	}

	key := "evt#http"
	ctx := context.Background()
	// First attempt: 500.
	if err := ch.Send(ctx, key, []byte("body")); err == nil {
		t.Fatal("expected injected 500 on first attempt")
	}
	// Second attempt: 200.
	if err := ch.Send(ctx, key, []byte("body")); err != nil {
		t.Fatalf("second attempt: %v", err)
	}
	// Third call is a replay: server dedups by key, no new side effect.
	if err := ch.Send(ctx, key, []byte("body")); err != nil {
		t.Fatalf("replay: %v", err)
	}
	if srv.ObservedCount() != 1 || !srv.HasObserved(key) {
		t.Fatalf("server side effects=%d", srv.ObservedCount())
	}
}

// TestLoopbackRejectsNonLoopback guards the loopback-only safety check.
func TestLoopbackRejectsNonLoopback(t *testing.T) {
	if _, err := NewLoopbackChannel(LoopbackConfig{Name: "x", URL: "http://10.255.255.1/hit"}); err == nil {
		t.Fatal("expected non-loopback URL to be rejected")
	}
}

// TestLoopbackThroughEngineEndToEnd wires the HTTP channel into the engine
// and verifies retry -> success -> single side effect end to end.
func TestLoopbackThroughEngineEndToEnd(t *testing.T) {
	srv := NewLoopbackServer(time.Millisecond, 2) // fail first 2
	url := srv.Start()
	defer srv.Close()

	ch, err := NewLoopbackChannel(LoopbackConfig{Name: "http", URL: url, AttemptTimeout: 2 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	clk := NewFakeClock(time.Now())
	eng := newTestEngine(t, clk)
	if err := eng.AddChannel(ch, ChannelConfig{
		Name: "http", Concurrency: 1, MaxAttempts: 5,
		InitialBackoff: 10 * time.Millisecond,
	}); err != nil {
		t.Fatal(err)
	}
	if err := eng.AddRule(RoutingRule{ID: "r", Channel: "http"}); err != nil {
		t.Fatal(err)
	}
	if err := eng.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer eng.Shutdown(context.Background())

	stop := make(chan struct{})
	go pump(clk, stop)
	defer close(stop)

	eng.Submit(context.Background(), Event{ID: "http1", Payload: []byte("ping")})
	rep := waitResult(t, eng, "http1")
	if rep.Results[0].Code != ResultSuccess {
		t.Fatalf("code=%s reason=%s", rep.Results[0].Code, rep.Results[0].Reason)
	}
	if rep.Results[0].Attempts != 3 {
		t.Fatalf("want 3 attempts, got %d", rep.Results[0].Attempts)
	}
	if srv.ObservedCount() != 1 {
		t.Fatalf("server side effects=%d want 1", srv.ObservedCount())
	}
}
