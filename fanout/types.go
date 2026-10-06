// Package fanout implements a notification fan-out and delivery scheduling
// subsystem. A single upstream event is fanned out to one or more downstream
// channels that differ in availability, latency and failure modes. Delivery
// is explainable (per-delivery state machine + per-attempt history),
// recoverable (write-ahead log persisted to disk) and reproducibly testable
// (injectable clock and in-memory/loopback channels).
package fanout

import (
	"context"
	"time"
)

// DeliveryStatus is the state of a single (event, channel) delivery.
type DeliveryStatus string

const (
	// StatusPending means the delivery was created but processing has not
	// completed (queued, scheduled, waiting for a retry or in-flight).
	StatusPending DeliveryStatus = "pending"
	// StatusInFlight means an attempt is currently executing against the
	// downstream channel. This status is persisted so that a crash in the
	// middle of an attempt is observable after recovery.
	StatusInFlight DeliveryStatus = "inflight"
	// StatusSuccess means the downstream acknowledged the delivery exactly
	// once. Terminal.
	StatusSuccess DeliveryStatus = "success"
	// StatusFailed means all attempts were exhausted and the delivery was
	// moved to the dead-letter queue. Terminal.
	StatusFailed DeliveryStatus = "failed"
	// StatusSkipped means the delivery never produced a downstream side
	// effect because an explicit skip rule applied. Terminal.
	StatusSkipped DeliveryStatus = "skipped"
)

// ResultCode is the coarse-grained outcome of a delivery.
type ResultCode string

const (
	ResultSuccess ResultCode = "success"
	ResultFailed  ResultCode = "failed"
	ResultSkipped ResultCode = "skipped"
)

// Well-known human-readable skip reasons.
const (
	// SkipDedup is used when the same logical notification enters the
	// system more than once; only the first occurrence is delivered.
	SkipDedup = "dedup: logical notification already accepted"
	// SkipChannelDisabled is used when a routing rule points at a channel
	// that is disabled in configuration.
	SkipChannelDisabled = "channel disabled in configuration"
	// SkipBeforeReady is used when a scheduled trigger fires before the
	// event's not-before time and the delivery was already finalised.
	SkipBeforeReady = "trigger fired before not-before time"
)

// ChannelKind identifies a downstream channel implementation.
type ChannelKind string

const (
	KindMemory   ChannelKind = "memory"
	KindLoopback ChannelKind = "loopback"
)

// ChannelConfig is the per-channel runtime configuration.
type ChannelConfig struct {
	// Name is the unique channel identifier referenced by routing rules.
	Name string
	// Kind selects the downstream implementation (memory, loopback).
	Kind ChannelKind
	// Disabled turns the channel into a no-op target: deliveries routed to
	// it terminate as skipped without producing a side effect.
	Disabled bool
	// Concurrency is the maximum number of in-flight deliveries for this
	// channel at the same time. <= 0 means 1.
	Concurrency int
	// RatePerSecond is the maximum number of started attempts per second
	// for this channel. Over-limit deliveries queue instead of being
	// dropped. <= 0 disables rate limiting.
	RatePerSecond float64
	// MaxAttempts is the maximum number of attempts per delivery
	// (1 = no retry). <= 0 means 1.
	MaxAttempts int
	// InitialBackoff is the delay before the second attempt. Subsequent
	// attempts double it (exponential backoff).
	InitialBackoff time.Duration
	// MaxBackoff caps the computed backoff. Zero means no cap.
	MaxBackoff time.Duration
	// AttemptTimeout bounds a single attempt; a timed-out attempt is
	// retried like any other failure. Zero means no timeout.
	AttemptTimeout time.Duration
}

// RoutingRule maps a matching event to a downstream channel.
type RoutingRule struct {
	// ID is the rule identifier (used only for explanations).
	ID string
	// EventType limits the rule to events whose Type matches. Empty means
	// all event types.
	EventType string
	// Channel is the name of the target channel.
	Channel string
}

// Event is a single upstream notification.
type Event struct {
	// ID uniquely identifies the logical notification. It is the
	// idempotency key: submitting the same ID twice has no additional side
	// effect.
	ID string
	// Type selects routing rules.
	Type string
	// Payload is the opaque notification body handed to channels.
	Payload []byte
	// NotBefore schedules delivery: the event is not dispatched before the
	// given time. Zero means deliver as soon as possible. A trigger that
	// arrives late never causes a second delivery.
	NotBefore time.Time
}

// Attempt is one recorded try against a downstream channel.
type Attempt struct {
	// Number is 1-based.
	Number int
	// StartedAt / FinishedAt bracket the attempt.
	StartedAt  time.Time
	FinishedAt time.Time
	// Error is the failure reason ("" on success).
	Error string
}

// Delivery is the persisted state of delivering one event to one channel.
type Delivery struct {
	// ID is eventID + "#" + channelName; it is the idempotency key the
	// downstream uses to suppress duplicate side effects.
	ID       string
	EventID  string
	Channel  string
	Status   DeliveryStatus
	Attempts []Attempt
	// AttemptCount is the number of attempts that were started.
	AttemptCount int
	// Reason is a human-readable explanation of the terminal state (skip
	// reason or final failure cause).
	Reason    string
	CreatedAt time.Time
	UpdatedAt time.Time
	// NotBefore is copied from the event and drives scheduling.
	NotBefore time.Time
}

// DeliveryResult is the explanation returned (and queryable) for one delivery.
type DeliveryResult struct {
	DeliveryID string
	EventID    string
	Channel    string
	Code       ResultCode
	Status     DeliveryStatus
	Reason     string
	Attempts   int
}

// EventReport is the explainable summary of one event's whole fan-out.
type EventReport struct {
	EventID string
	Results []DeliveryResult
}

// Store is the durable state log. All state transitions are appended as
// immutable entries; current state is reconstructed by replay.
type Store interface {
	// Append durably persists one entry and returns its 1-based sequence
	// number.
	Append(ctx context.Context, e Entry) (seq uint64, err error)
	// Replay invokes fn for every persisted entry in sequence order.
	Replay(ctx context.Context, fn func(Entry) error) error
	// Close releases the underlying resources.
	Close() error
}

// EntryKind enumerates write-ahead log entry kinds.
type EntryKind string

const (
	EntryAccepted EntryKind = "accepted" // event accepted into the system
	EntryCreated  EntryKind = "created"  // one per-channel delivery created
	EntryAttempt  EntryKind = "attempt"  // attempt started or finished
	EntryTerminal EntryKind = "terminal" // delivery reached a terminal state
	EntryDead     EntryKind = "dead"     // delivery moved to dead-letter
)

// Entry is one immutable persisted record.
type Entry struct {
	Seq        uint64
	Kind       EntryKind
	Time       time.Time
	EventID    string
	DeliveryID string
	Channel    string
	// AttemptNo is 1-based for attempt entries, 0 otherwise.
	AttemptNo int
	// Starting distinguishes an attempt-start marker from its finish.
	Starting  bool
	Status    DeliveryStatus
	Reason    string
	NotBefore time.Time
	// Payload is persisted on the created entry so replay can re-deliver.
	Payload []byte
}

// Channel is a downstream delivery target.
type Channel interface {
	// Name returns the configured channel name.
	Name() string
	// Kind returns the implementation kind.
	Kind() ChannelKind
	// Send attempts delivery. The implementation MUST be idempotent for
	// repeated deliveryID values: a crash/replay may call Send twice for the
	// same deliveryID, but the observable side effect happens at most once.
	Send(ctx context.Context, deliveryID string, payload []byte) error
}
