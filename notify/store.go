package notify

import (
	"context"
	"errors"
)

// ErrNotFound is returned when a record does not exist.
var ErrNotFound = errors.New("notify: not found")

// Store persists events, deliveries and attempts so the system can recover
// after a crash. Implementations must be safe for concurrent use.
type Store interface {
	// SaveEvent inserts the event; returns false (no error) if the id
	// already exists, making submission idempotent.
	SaveEvent(ctx context.Context, e Event) (inserted bool, err error)
	GetEvent(ctx context.Context, id string) (Event, error)

	// SaveDelivery inserts a delivery; a duplicate id is a no-op returning false.
	SaveDelivery(ctx context.Context, d Delivery) (inserted bool, err error)
	// UpdateDelivery overwrites an existing delivery (state transitions).
	UpdateDelivery(ctx context.Context, d Delivery) error
	GetDelivery(ctx context.Context, id string) (Delivery, error)
	// ListDeliveries returns deliveries matching the filter; empty status
	// matches all, empty channel matches all.
	ListDeliveries(ctx context.Context, channel string, status DeliveryStatus) ([]Delivery, error)

	// AppendAttempt records one attempt (append-only audit log).
	AppendAttempt(ctx context.Context, a Attempt) error
	ListAttempts(ctx context.Context, deliveryID string) ([]Attempt, error)

	Close() error
}
