package notify

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// SimChannel is an in-memory downstream channel used to simulate real
// destinations: configurable latency, scripted transient/permanent
// failures, optional blocking, and an idempotent sink that records each
// delivery id at most once, so tests can verify exactly-once observable
// effects even across retries and restarts.
type SimChannel struct {
	name string

	mu       sync.Mutex
	latency  time.Duration
	failNext int           // transient failures remaining
	permNext int           // permanent failures remaining
	gate     chan struct{} // if non-nil, Deliver blocks until a value arrives or ctx ends
	received []string
	seen     map[string]bool
	calls    int
}

// NewSimChannel creates a simulated channel.
func NewSimChannel(name string) *SimChannel {
	return &SimChannel{name: name, seen: map[string]bool{}}
}

func (s *SimChannel) Name() string { return s.name }

// WithLatency makes each Deliver call take at least d (unless ctx ends first).
func (s *SimChannel) WithLatency(d time.Duration) *SimChannel {
	s.mu.Lock()
	s.latency = d
	s.mu.Unlock()
	return s
}

// WithFailures makes the next n Deliver calls fail with transient
// (retryable) errors.
func (s *SimChannel) WithFailures(n int) *SimChannel {
	s.mu.Lock()
	s.failNext = n
	s.mu.Unlock()
	return s
}

// WithPermanentFailures makes the next n Deliver calls fail with permanent
// (non-retryable) errors.
func (s *SimChannel) WithPermanentFailures(n int) *SimChannel {
	s.mu.Lock()
	s.permNext = n
	s.mu.Unlock()
	return s
}

// WithGate makes Deliver block until the gate channel yields (or ctx ends),
// simulating a stuck downstream.
func (s *SimChannel) WithGate(gate chan struct{}) *SimChannel {
	s.mu.Lock()
	s.gate = gate
	s.mu.Unlock()
	return s
}

// Deliver applies the (simulated) side effect, idempotent by delivery id.
func (s *SimChannel) Deliver(ctx context.Context, d Delivery) error {
	s.mu.Lock()
	s.calls++
	latency := s.latency
	gate := s.gate
	if s.failNext > 0 {
		s.failNext--
		s.mu.Unlock()
		return fmt.Errorf("channel %s: simulated transient failure", s.name)
	}
	if s.permNext > 0 {
		s.permNext--
		s.mu.Unlock()
		return Permanent(fmt.Errorf("channel %s: simulated permanent failure", s.name))
	}
	s.mu.Unlock()

	if latency > 0 {
		t := time.NewTimer(latency)
		select {
		case <-ctx.Done():
			t.Stop()
			return ctx.Err()
		case <-t.C:
		}
	}
	if gate != nil {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-gate:
		}
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.seen[d.ID] {
		return nil // idempotent: side effect already applied
	}
	s.seen[d.ID] = true
	s.received = append(s.received, d.ID)
	return nil
}

// Received returns the delivery ids whose side effect was applied, in order.
func (s *SimChannel) Received() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.received...)
}

// Calls returns the total number of Deliver invocations (including retries).
func (s *SimChannel) Calls() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls
}
