package notify

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
)

// record kinds in the append-only log.
const (
	recEvent    = "event"
	recDelivery = "delivery"
	recAttempt  = "attempt"
)

// logRecord is one line in the JSONL log. Exactly one payload field is set.
type logRecord struct {
	Kind     string    `json:"kind"`
	Event    *Event    `json:"event,omitempty"`
	Delivery *Delivery `json:"delivery,omitempty"`
	Attempt  *Attempt  `json:"attempt,omitempty"`
}

// FileStore is a Store backed by an append-only JSON-lines log with an
// in-memory index. On Open the log is replayed to rebuild state, which
// provides crash recovery: committed records survive restarts.
//
// Write policy: every mutation is appended and fsynced before the call
// returns, so a committed delivery is never lost and never repeated.
type FileStore struct {
	mu     sync.Mutex
	path   string
	f      *os.File
	enc    *json.Encoder
	closed bool

	events     map[string]Event
	deliveries map[string]Delivery
	attempts   map[string][]Attempt
}

// OpenFileStore opens (or creates) the log at path and replays it.
func OpenFileStore(path string) (*FileStore, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return nil, fmt.Errorf("notify: open store: %w", err)
	}
	s := &FileStore{
		path:       path,
		f:          f,
		events:     map[string]Event{},
		deliveries: map[string]Delivery{},
		attempts:   map[string][]Attempt{},
	}
	if err := s.replay(); err != nil {
		f.Close()
		return nil, err
	}
	s.enc = json.NewEncoder(f)
	return s, nil
}

// replay rebuilds the in-memory index from the log. A torn tail line
// (crash mid-write) is ignored: the record was never acknowledged anyway.
func (s *FileStore) replay() error {
	if _, err := s.f.Seek(0, io.SeekStart); err != nil {
		return err
	}
	sc := bufio.NewScanner(s.f)
	sc.Buffer(make([]byte, 0, 1<<20), 16<<20)
	for sc.Scan() {
		line := sc.Bytes()
		if len(line) == 0 {
			continue
		}
		var rec logRecord
		if err := json.Unmarshal(line, &rec); err != nil {
			continue // torn tail from a crash; safe to skip
		}
		s.apply(rec)
	}
	if _, err := s.f.Seek(0, io.SeekEnd); err != nil {
		return err
	}
	return nil
}

// apply folds one record into the in-memory index (must hold s.mu or be in replay).
func (s *FileStore) apply(rec logRecord) {
	switch rec.Kind {
	case recEvent:
		s.events[rec.Event.ID] = *rec.Event
	case recDelivery:
		s.deliveries[rec.Delivery.ID] = *rec.Delivery
	case recAttempt:
		s.attempts[rec.Attempt.DeliveryID] = append(s.attempts[rec.Attempt.DeliveryID], *rec.Attempt)
	}
}

// appendLocked writes and fsyncs one record (must hold s.mu).
func (s *FileStore) appendLocked(rec logRecord) error {
	if s.closed {
		return errors.New("notify: store closed")
	}
	if err := s.enc.Encode(rec); err != nil {
		return fmt.Errorf("notify: write store: %w", err)
	}
	if err := s.f.Sync(); err != nil {
		return fmt.Errorf("notify: sync store: %w", err)
	}
	s.apply(rec)
	return nil
}

func (s *FileStore) SaveEvent(ctx context.Context, e Event) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.events[e.ID]; ok {
		return false, nil // idempotent: duplicate logical notification
	}
	if err := s.appendLocked(logRecord{Kind: recEvent, Event: &e}); err != nil {
		return false, err
	}
	return true, nil
}

func (s *FileStore) GetEvent(ctx context.Context, id string) (Event, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.events[id]
	if !ok {
		return Event{}, ErrNotFound
	}
	return e, nil
}

func (s *FileStore) SaveDelivery(ctx context.Context, d Delivery) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.deliveries[d.ID]; ok {
		return false, nil
	}
	if err := s.appendLocked(logRecord{Kind: recDelivery, Delivery: &d}); err != nil {
		return false, err
	}
	return true, nil
}

func (s *FileStore) UpdateDelivery(ctx context.Context, d Delivery) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.deliveries[d.ID]; !ok {
		return ErrNotFound
	}
	return s.appendLocked(logRecord{Kind: recDelivery, Delivery: &d})
}

func (s *FileStore) GetDelivery(ctx context.Context, id string) (Delivery, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	d, ok := s.deliveries[id]
	if !ok {
		return Delivery{}, ErrNotFound
	}
	return d, nil
}

func (s *FileStore) ListDeliveries(ctx context.Context, channel string, status DeliveryStatus) ([]Delivery, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []Delivery
	for _, d := range s.deliveries {
		if channel != "" && d.Channel != channel {
			continue
		}
		if status != "" && d.Status != status {
			continue
		}
		out = append(out, d)
	}
	return out, nil
}

func (s *FileStore) AppendAttempt(ctx context.Context, a Attempt) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.appendLocked(logRecord{Kind: recAttempt, Attempt: &a})
}

func (s *FileStore) ListAttempts(ctx context.Context, deliveryID string) ([]Attempt, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]Attempt(nil), s.attempts[deliveryID]...), nil
}

func (s *FileStore) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	return s.f.Close()
}
