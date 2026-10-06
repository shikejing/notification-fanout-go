package fanout

import (
	"context"
	"sync"
)

// MemoryStore keeps entries in memory only. It implements the same Store
// contract (monotonic sequence, replay order) as FileStore and is useful for
// tests that do not exercise crash recovery.
type MemoryStore struct {
	mu      sync.Mutex
	clk     Clock
	entries []Entry
	closed  bool
}

// NewMemoryStore returns an empty in-memory store.
func NewMemoryStore(clk Clock) *MemoryStore {
	if clk == nil {
		clk = NewRealClock()
	}
	return &MemoryStore{clk: clk}
}

// Append records e and returns its 1-based sequence number.
func (s *MemoryStore) Append(ctx context.Context, e Entry) (uint64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return 0, errStoreClosed
	}
	if e.Time.IsZero() {
		e.Time = s.clk.Now()
	}
	e.Seq = uint64(len(s.entries) + 1)
	s.entries = append(s.entries, e)
	return e.Seq, nil
}

// Replay invokes fn on a snapshot of all entries in append order.
func (s *MemoryStore) Replay(ctx context.Context, fn func(Entry) error) error {
	s.mu.Lock()
	snap := make([]Entry, len(s.entries))
	copy(snap, s.entries)
	s.mu.Unlock()
	for _, e := range snap {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := fn(e); err != nil {
			return err
		}
	}
	return nil
}

// Close marks the store closed.
func (s *MemoryStore) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
	return nil
}
