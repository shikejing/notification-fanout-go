package fanout

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"time"
)

// FileStore persists entries as newline-delimited JSON records appended to a
// single log file. Every Append is fsynced before it returns, so once Append
// succeeds the transition survives process crashes. Current state is
// reconstructed by Replay; the log is append-only/immutable, which keeps
// recovery explainable (every state change has a durable cause).
type FileStore struct {
	path string
	clk  Clock

	mu      sync.Mutex
	f       *os.File
	nextSeq uint64
}

type jsonEntry struct {
	Seq        uint64         `json:"seq"`
	Kind       EntryKind      `json:"kind"`
	Time       time.Time      `json:"time"`
	EventID    string         `json:"event_id"`
	DeliveryID string         `json:"delivery_id"`
	Channel    string         `json:"channel"`
	AttemptNo  int            `json:"attempt_no"`
	Starting   bool           `json:"starting"`
	Status     DeliveryStatus `json:"status"`
	Reason     string         `json:"reason"`
	NotBefore  time.Time      `json:"not_before"`
	Payload    []byte         `json:"payload,omitempty"`
}

// NewFileStore opens (creating if needed) the log file at path. Existing
// entries are scanned to determine the next sequence number; state itself is
// rebuilt by the caller through Replay.
func NewFileStore(path string, clk Clock) (*FileStore, error) {
	if clk == nil {
		clk = NewRealClock()
	}
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open store %q: %w", path, err)
	}
	s := &FileStore{path: path, clk: clk, f: f}
	if err := s.bootstrap(); err != nil {
		_ = f.Close()
		return nil, err
	}
	return s, nil
}

// bootstrap scans the log once to establish the next sequence number.
func (s *FileStore) bootstrap() error {
	var lastSeq uint64
	err := scanLines(s.f, func(line []byte) error {
		if len(bytes.TrimSpace(line)) == 0 {
			return nil
		}
		var je jsonEntry
		if err := json.Unmarshal(line, &je); err != nil {
			return fmt.Errorf("corrupt log entry near seq=%d: %w", lastSeq, err)
		}
		if je.Seq > lastSeq {
			lastSeq = je.Seq
		}
		return nil
	})
	if err != nil {
		return err
	}
	s.nextSeq = lastSeq
	return nil
}

// Append durably persists e and returns its 1-based sequence number.
func (s *FileStore) Append(ctx context.Context, e Entry) (uint64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.f == nil {
		return 0, errors.New("store is closed")
	}
	s.nextSeq++
	e.Seq = s.nextSeq
	if e.Time.IsZero() {
		e.Time = s.clk.Now()
	}
	line, err := json.Marshal(toJSON(e))
	if err != nil {
		s.nextSeq--
		return 0, fmt.Errorf("marshal entry: %w", err)
	}
	line = append(line, '\n')
	if _, err := s.f.Write(line); err != nil {
		return 0, fmt.Errorf("write store: %w", err)
	}
	if err := s.f.Sync(); err != nil {
		return 0, fmt.Errorf("sync store: %w", err)
	}
	return e.Seq, nil
}

// Replay invokes fn for every persisted entry in sequence order. It opens an
// independent read handle so it does not disturb the append offset.
func (s *FileStore) Replay(ctx context.Context, fn func(Entry) error) error {
	f, err := os.Open(s.path)
	if err != nil {
		return fmt.Errorf("open store for replay: %w", err)
	}
	defer f.Close()
	return scanLines(f, func(line []byte) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if len(bytes.TrimSpace(line)) == 0 {
			return nil
		}
		var je jsonEntry
		if err := json.Unmarshal(line, &je); err != nil {
			return fmt.Errorf("decode entry during replay: %w", err)
		}
		return fn(fromJSON(je))
	})
}

// Close flushes and closes the log.
func (s *FileStore) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.f == nil {
		return nil
	}
	err := s.f.Close()
	s.f = nil
	return err
}

func scanLines(r io.Reader, fn func([]byte) error) error {
	br := bufio.NewReader(r)
	for {
		line, err := br.ReadBytes('\n')
		if len(line) > 0 {
			trimmed := bytes.TrimRight(line, "\n")
			if e := fn(trimmed); e != nil {
				return e
			}
		}
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
	}
}
