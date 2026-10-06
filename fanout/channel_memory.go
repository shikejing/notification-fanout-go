package fanout

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// MemoryChannel is an in-process downstream channel used to simulate a real
// downstream: it can add latency, fail a programmable number of attempts and
// block on specific deliveries. It is idempotent: each deliveryID produces at
// most one observable side effect, even if Send is replayed after a crash.
type MemoryChannel struct {
	cfg MemoryChannelConfig

	mu        sync.Mutex
	observed  map[string]bool   // deliveryIDs that produced a side effect
	payloads  map[string][]byte // payload of the first accepted delivery
	fresh     map[string]int    // fresh (non-replay) Send count per deliveryID
	blockers  map[string]chan struct{}
	unblocked map[string]bool // IDs permanently released via Unblock
	delivered chan string     // signalled (non-blocking) on each new side effect
}

// MemoryChannelConfig configures a MemoryChannel.
type MemoryChannelConfig struct {
	// Name is the channel identifier.
	Name string
	// Latency is applied to every fresh attempt before it succeeds/fails.
	Latency time.Duration
	// FailFirst makes the first FailFirst fresh attempts fail; subsequent
	// attempts succeed. A very large value simulates a permanently failing
	// target (which then goes to the dead-letter queue).
	FailFirst int
	// BlockOnIDs blocks Send for these deliveryIDs until Unblock is called,
	// to exercise timeout/cancellation behaviour.
	BlockOnIDs map[string]bool
	// Clock drives Latency; defaults to the real clock.
	Clock Clock
}

// NewMemoryChannel constructs a MemoryChannel.
func NewMemoryChannel(cfg MemoryChannelConfig) *MemoryChannel {
	if cfg.Clock == nil {
		cfg.Clock = NewRealClock()
	}
	return &MemoryChannel{
		cfg:       cfg,
		observed:  make(map[string]bool),
		payloads:  make(map[string][]byte),
		fresh:     make(map[string]int),
		blockers:  make(map[string]chan struct{}),
		unblocked: make(map[string]bool),
		delivered: make(chan string, 1024),
	}
}

// Name returns the channel name.
func (c *MemoryChannel) Name() string { return c.cfg.Name }

// Kind returns KindMemory.
func (c *MemoryChannel) Kind() ChannelKind { return KindMemory }

// Send attempts delivery. Repeated calls for a deliveryID whose side effect
// already happened return nil without doing anything (at-most-once side
// effect across retries and crash replay).
func (c *MemoryChannel) Send(ctx context.Context, deliveryID string, payload []byte) error {
	// Fast idempotency path.
	c.mu.Lock()
	if c.observed[deliveryID] {
		c.mu.Unlock()
		return nil
	}
	block := c.cfg.BlockOnIDs[deliveryID]
	var blocker chan struct{}
	if block && !c.unblocked[deliveryID] {
		blocker = c.blockers[deliveryID]
		if blocker == nil {
			blocker = make(chan struct{})
			c.blockers[deliveryID] = blocker
		}
	}
	c.fresh[deliveryID]++
	attempt := c.fresh[deliveryID]
	c.mu.Unlock()

	// Honour externally configured blocking (simulating a hung downstream).
	if blocker != nil {
		select {
		case <-ctx.Done():
			return fmt.Errorf("memory channel %q blocked call cancelled: %w", c.cfg.Name, ctx.Err())
		case <-blocker:
		}
	}

	if c.cfg.Latency > 0 {
		if err := Sleep(ctx, c.cfg.Clock, c.cfg.Latency); err != nil {
			return fmt.Errorf("memory channel %q latency interrupted: %w", c.cfg.Name, err)
		}
	}

	if attempt <= c.cfg.FailFirst {
		return fmt.Errorf("memory channel %q injected failure on attempt %d (<= fail-first %d)",
			c.cfg.Name, attempt, c.cfg.FailFirst)
	}

	// Commit the side effect under the lock; re-check to remain idempotent
	// even under concurrent calls with the same ID.
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.observed[deliveryID] {
		return nil
	}
	c.observed[deliveryID] = true
	cp := make([]byte, len(payload))
	copy(cp, payload)
	c.payloads[deliveryID] = cp
	select {
	case c.delivered <- deliveryID:
	default:
	}
	return nil
}

// ObservedIDs returns the set of deliveryIDs that produced a side effect.
func (c *MemoryChannel) ObservedIDs() map[string]bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make(map[string]bool, len(c.observed))
	for k := range c.observed {
		out[k] = true
	}
	return out
}

// ObservedCount returns the number of distinct side effects observed.
func (c *MemoryChannel) ObservedCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.observed)
}

// Payload returns the payload stored for deliveryID, or nil.
func (c *MemoryChannel) Payload(deliveryID string) []byte {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.payloads[deliveryID]
}

// Delivered returns a channel that receives each newly side-effected
// deliveryID exactly once.
func (c *MemoryChannel) Delivered() <-chan string { return c.delivered }

// Unblock releases any Send blocked on deliveryID.
func (c *MemoryChannel) Unblock(deliveryID string) {
	c.mu.Lock()
	ch := c.blockers[deliveryID]
	delete(c.blockers, deliveryID)
	c.unblocked[deliveryID] = true
	c.mu.Unlock()
	if ch != nil {
		close(ch)
	}
}
