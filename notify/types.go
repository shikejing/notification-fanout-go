// Package notify implements a notification fan-out and delivery scheduling
// subsystem: upstream events are fanned out to multiple downstream channels,
// each channel progresses independently, and the whole delivery lifecycle is
// explainable, recoverable and repeatedly verifiable.
package notify

import (
	"context"
	"time"
)

// Outcome is the result classification of a single delivery attempt.
type Outcome string

const (
	OutcomeSuccess Outcome = "success"
	OutcomeFailure Outcome = "failure"
	OutcomeSkipped Outcome = "skipped"
)

// DeliveryStatus is the lifecycle state of one (event, channel) delivery.
type DeliveryStatus string

const (
	StatusPending   DeliveryStatus = "pending"   // waiting for its scheduled time
	StatusQueued    DeliveryStatus = "queued"    // due, waiting for worker slot / rate token
	StatusInFlight  DeliveryStatus = "in_flight" // a worker is delivering right now
	StatusSucceeded DeliveryStatus = "succeeded" // delivered and acknowledged
	StatusDead      DeliveryStatus = "dead"      // retries exhausted, moved to dead letter
	StatusCanceled  DeliveryStatus = "canceled"  // canceled before completion
)

// Terminal reports whether the status is a final state.
func (s DeliveryStatus) Terminal() bool {
	switch s {
	case StatusSucceeded, StatusDead, StatusCanceled:
		return true
	}
	return false
}

// Event is one logical notification produced by upstream.
// ID is the idempotency key: re-submitting the same ID is a no-op.
type Event struct {
	ID        string            // logical notification id (idempotency key)
	Type      string            // routing/type tag, informational
	Payload   []byte            // opaque body handed to channels
	Targets   []string          // downstream channel names
	NotBefore time.Time         // scheduled delivery time (zero = ASAP)
	Meta      map[string]string // extra annotations
}

// Delivery is the per-(event, channel) unit of work.
type Delivery struct {
	ID        string         // EventID + "|" + Channel
	EventID   string         // logical notification id
	Channel   string         // target channel name
	Payload   []byte         // copied from the event
	Status    DeliveryStatus // lifecycle state
	Attempts  int            // number of finished attempts
	NotBefore time.Time      // scheduled time (updated for backoff)
	CreatedAt time.Time
	UpdatedAt time.Time
	LastError string // human readable reason of the last failure
}

// DeliveryID builds the deterministic delivery id.
func DeliveryID(eventID, channel string) string { return eventID + "|" + channel }

// Attempt is one recorded try (or skip) of a delivery, for explainability.
type Attempt struct {
	DeliveryID string
	Seq        int
	Outcome    Outcome
	Reason     string // human readable
	StartedAt  time.Time
	FinishedAt time.Time
}

// Channel is a downstream destination. Deliver must be idempotent with
// respect to Delivery.ID: the subsystem may re-drive a delivery after a
// crash, and the channel must not apply the side effect twice.
type Channel interface {
	Name() string
	Deliver(ctx context.Context, d Delivery) error
}

// PermanentError marks a failure as non-retryable: the delivery goes
// straight to the dead letter set instead of being retried.
type PermanentError struct{ Err error }

func (e PermanentError) Error() string { return e.Err.Error() }
func (e PermanentError) Unwrap() error { return e.Err }

// Permanent wraps err as a non-retryable error.
func Permanent(err error) error { return PermanentError{Err: err} }

// ChannelConfig tunes one channel's delivery pipeline.
type ChannelConfig struct {
	Concurrency    int           // max simultaneous in-flight deliveries
	RatePerSec     float64       // max deliveries started per second (0 = unlimited)
	MaxAttempts    int           // max attempts before dead-lettering (>=1)
	BaseBackoff    time.Duration // first retry delay
	MaxBackoff     time.Duration // retry delay cap
	DeliverTimeout time.Duration // per-attempt timeout (0 = none)
	QueueCapacity  int           // max queued deliveries (0 = unlimited); excess waits, never dropped
}

func (c ChannelConfig) withDefaults() ChannelConfig {
	if c.Concurrency <= 0 {
		c.Concurrency = 1
	}
	if c.MaxAttempts <= 0 {
		c.MaxAttempts = 3
	}
	if c.BaseBackoff <= 0 {
		c.BaseBackoff = 50 * time.Millisecond
	}
	if c.MaxBackoff <= 0 {
		c.MaxBackoff = 5 * time.Second
	}
	return c
}

// SubmitResult describes what Submit did with one event.
type SubmitResult struct {
	EventID    string
	Duplicated bool       // true when the event id was already known (no-op)
	Deliveries []Delivery // snapshot of created (or existing) deliveries
}
