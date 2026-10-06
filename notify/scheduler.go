package notify

import (
	"container/heap"
	"context"
	"sync"
	"time"
)

// schedEntry is one scheduled firing of a delivery.
type schedEntry struct {
	id  string
	at  time.Time
	seq uint64 // generation; stale entries are skipped on pop
}

// schedHeap is a min-heap ordered by fire time.
type schedHeap []schedEntry

func (h schedHeap) Len() int           { return len(h) }
func (h schedHeap) Less(i, j int) bool { return h[i].at.Before(h[j].at) }
func (h schedHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *schedHeap) Push(x any)        { *h = append(*h, x.(schedEntry)) }
func (h *schedHeap) Pop() any {
	old := *h
	n := len(old)
	e := old[n-1]
	*h = old[:n-1]
	return e
}

// scheduler fires due deliveries exactly once: it pops (dueAt, deliveryID)
// entries from a min-heap and hands them to the fire callback. The
// dispatcher's guarded state transition (pending -> queued) makes late or
// duplicate triggers harmless no-ops, so a late trigger never causes a
// duplicate delivery.
type scheduler struct {
	mu      sync.Mutex
	heap    schedHeap
	latest  map[string]uint64 // deliveryID -> newest generation
	seq     uint64
	wake    chan struct{}
	stopped bool
}

func newScheduler() *scheduler {
	return &scheduler{
		latest: map[string]uint64{},
		wake:   make(chan struct{}, 1),
	}
}

// schedule registers a delivery to fire at the given time. Scheduling the
// same id again supersedes the previous entry.
func (s *scheduler) schedule(deliveryID string, at time.Time) {
	s.mu.Lock()
	s.seq++
	seq := s.seq
	s.latest[deliveryID] = seq
	heap.Push(&s.heap, schedEntry{id: deliveryID, at: at, seq: seq})
	s.mu.Unlock()
	select {
	case s.wake <- struct{}{}:
	default:
	}
}

// cancel forgets a pending entry (lazy deletion: the heap entry becomes
// stale and is skipped when it pops).
func (s *scheduler) cancel(deliveryID string) {
	s.mu.Lock()
	delete(s.latest, deliveryID)
	s.mu.Unlock()
}

// run fires due entries until ctx is done.
func (s *scheduler) run(ctx context.Context, fire func(deliveryID string)) {
	var timer *time.Timer
	var timerC <-chan time.Time
	for {
		s.mu.Lock()
		// Drop stale entries from the top.
		for len(s.heap) > 0 {
			top := s.heap[0]
			if cur, ok := s.latest[top.id]; !ok || cur != top.seq {
				heap.Pop(&s.heap)
				continue
			}
			break
		}
		var next *schedEntry
		if len(s.heap) > 0 {
			e := s.heap[0]
			next = &e
		}
		s.mu.Unlock()

		if next == nil {
			timerC = nil
		} else {
			d := time.Until(next.at)
			if d < 0 {
				d = 0
			}
			if timer == nil {
				timer = time.NewTimer(d)
			} else {
				timer.Reset(d)
			}
			timerC = timer.C
		}

		select {
		case <-ctx.Done():
			if timer != nil {
				timer.Stop()
			}
			return
		case <-s.wake:
			// heap changed; recompute.
		case <-timerC:
			s.mu.Lock()
			if len(s.heap) > 0 {
				top := s.heap[0]
				if cur, ok := s.latest[top.id]; ok && cur == top.seq {
					heap.Pop(&s.heap)
					delete(s.latest, top.id)
					s.mu.Unlock()
					fire(top.id)
					continue
				}
			}
			s.mu.Unlock()
		}
	}
}
