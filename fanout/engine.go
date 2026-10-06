package fanout

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"
)

// EngineConfig configures an Engine.
type EngineConfig struct {
	// StorePath is the durable WAL file path. Empty uses an in-memory store.
	StorePath string
	// Clock defaults to RealClock.
	Clock Clock
}

// channelReg bundles a channel, its config and its dispatcher.
type channelReg struct {
	ch   Channel
	cfg  ChannelConfig
	disp *Dispatcher
}

// eventState aggregates the deliveries of one event and their completion.
type eventState struct {
	report   *EventReport
	total    int
	finished int
	// done is closed when every delivery is terminal.
	done chan struct{}
}

// Engine is the entry point of the fan-out subsystem. It owns routing,
// durable state, per-channel schedulers and the delivery state machine.
type Engine struct {
	cfg   EngineConfig
	clk   Clock
	store Store

	mu sync.Mutex
	// acceptMu serialises the check-then-accept critical section of Submit
	// so two concurrent submissions of the same Event.ID cannot both pass
	// dedup before either is persisted.
	acceptMu sync.Mutex
	channels map[string]*channelReg
	rules    []RoutingRule
	events   map[string]*eventState // eventID -> state
	dels     map[string]*Delivery   // deliveryID -> delivery
	order    []string               // deliveryIDs in creation order
	dead     map[string]bool        // dead-lettered delivery IDs
	started  bool
	stopped  bool
	accepted map[string]bool     // accepted event IDs (from WAL)
	wake     chan struct{}       // signals terminal-state changes
	notifyCh chan DeliveryResult // optional external notifications

	schedCtx    context.Context
	schedCancel context.CancelFunc
	finishedM   map[string]bool   // deliveryIDs already counted as terminal
	payloads    map[string][]byte // eventID -> payload, persisted in WAL
}

// NewEngine constructs an engine; nothing runs until Start.
func NewEngine(cfg EngineConfig) (*Engine, error) {
	clk := cfg.Clock
	if clk == nil {
		clk = NewRealClock()
	}
	var st Store
	if cfg.StorePath == "" {
		st = NewMemoryStore(clk)
	} else {
		fs, err := NewFileStore(cfg.StorePath, clk)
		if err != nil {
			return nil, err
		}
		st = fs
	}
	return &Engine{
		cfg:      cfg,
		clk:      clk,
		store:    st,
		channels: make(map[string]*channelReg),
		events:   make(map[string]*eventState),
		dels:     make(map[string]*Delivery),
		dead:     make(map[string]bool),
		accepted: make(map[string]bool),
		wake:     make(chan struct{}, 16),
		payloads: make(map[string][]byte),
	}, nil
}

// Store exposes the underlying durable store (mainly for tests/inspection).
func (e *Engine) Store() Store { return e.store }

// AddChannel registers a downstream channel with its delivery config. Must
// be called before Start. The channel name must be unique.
func (e *Engine) AddChannel(ch Channel, cfg ChannelConfig) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.started {
		return errAlreadyStarted
	}
	if cfg.Name == "" {
		cfg.Name = ch.Name()
	}
	if _, ok := e.channels[cfg.Name]; ok {
		return fmt.Errorf("%w: %s", errDuplicateName, cfg.Name)
	}
	normalizeCfg(&cfg)
	e.channels[cfg.Name] = &channelReg{ch: ch, cfg: cfg}
	return nil
}

// AddRule registers a routing rule. Must be called before Start.
func (e *Engine) AddRule(rule RoutingRule) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.started {
		return errAlreadyStarted
	}
	if _, ok := e.channels[rule.Channel]; !ok {
		return fmt.Errorf("%w: %s", errUnknownChannel, rule.Channel)
	}
	e.rules = append(e.rules, rule)
	return nil
}

// Start replays the durable log, launches per-channel schedulers and
// re-enqueues unfinished deliveries so that processing resumes after a crash.
func (e *Engine) Start(ctx context.Context) error {
	e.mu.Lock()
	if e.started {
		e.mu.Unlock()
		return errAlreadyStarted
	}
	e.started = true
	// Schedulers run under an internal context cancelled on Shutdown.
	schedCtx, cancel := context.WithCancel(context.Background())
	_ = cancel
	e.schedCtx = schedCtx
	e.schedCancel = cancel
	for _, reg := range e.channels {
		reg.disp = NewDispatcher(reg.cfg, e.clk)
		reg.disp.Start(schedCtx)
	}
	e.mu.Unlock()

	if err := e.replay(ctx); err != nil {
		return err
	}

	// Resume unfinished deliveries from reconstructed state.
	e.mu.Lock()
	resume := make([]*Delivery, 0)
	for _, id := range e.order {
		d := e.dels[id]
		if d.Status != StatusSuccess && d.Status != StatusFailed && d.Status != StatusSkipped {
			resume = append(resume, d)
		}
	}
	e.mu.Unlock()
	for _, d := range resume {
		e.rescheduleAfterRecovery(d)
	}
	return nil
}

// replay reconstructs all in-memory state from the immutable WAL.
func (e *Engine) replay(ctx context.Context) error {
	return e.store.Replay(ctx, func(en Entry) error {
		e.applyEntry(en)
		return nil
	})
}

// applyEntry folds one WAL entry into in-memory state. It is the single
// source of truth for recovery and is written to tolerate out-of-order
// tolerant partial tails (a crash may leave an "attempt start" without its
// finish; that is repaired at reschedule time).
func (e *Engine) applyEntry(en Entry) {
	switch en.Kind {
	case EntryAccepted:
		e.accepted[en.EventID] = true
		if es, ok := e.events[en.EventID]; ok {
			_ = es
		} else {
			e.events[en.EventID] = &eventState{
				report: &EventReport{EventID: en.EventID},
				done:   make(chan struct{}),
			}
		}
	case EntryCreated:
		if len(en.Payload) > 0 {
			e.payloads[en.EventID] = append([]byte(nil), en.Payload...)
		}
		d := &Delivery{
			ID:        en.DeliveryID,
			EventID:   en.EventID,
			Channel:   en.Channel,
			Status:    en.Status,
			Reason:    en.Reason,
			CreatedAt: en.Time,
			UpdatedAt: en.Time,
			NotBefore: en.NotBefore,
		}
		if _, exists := e.dels[d.ID]; !exists {
			e.dels[d.ID] = d
			e.order = append(e.order, d.ID)
			es := e.ensureEvent(d.EventID)
			es.total++
			es.report.Results = append(es.report.Results, DeliveryResult{
				DeliveryID: d.ID, EventID: d.EventID, Channel: d.Channel,
			})
			if d.Status == StatusSkipped {
				e.markFinishedLocked(d)
			}
		}
	case EntryAttempt:
		d := e.dels[en.DeliveryID]
		if d == nil {
			return
		}
		if en.Starting {
			d.Status = StatusInFlight
			// Ensure an Attempt slot exists for this attempt number.
			for len(d.Attempts) < en.AttemptNo {
				d.Attempts = append(d.Attempts, Attempt{Number: len(d.Attempts) + 1})
			}
			a := &d.Attempts[en.AttemptNo-1]
			a.StartedAt = en.Time
			a.Number = en.AttemptNo
			d.AttemptCount = en.AttemptNo
		} else {
			for len(d.Attempts) < en.AttemptNo {
				d.Attempts = append(d.Attempts, Attempt{Number: len(d.Attempts) + 1})
			}
			a := &d.Attempts[en.AttemptNo-1]
			a.FinishedAt = en.Time
			a.Error = en.Reason
			if en.Reason == "" {
				d.Status = StatusSuccess
			} else {
				d.Status = StatusPending
			}
		}
		d.UpdatedAt = en.Time
	case EntryTerminal:
		d := e.dels[en.DeliveryID]
		if d == nil {
			return
		}
		d.Status = en.Status
		d.Reason = en.Reason
		d.UpdatedAt = en.Time
		e.markFinishedLocked(d)
	case EntryDead:
		e.dead[en.DeliveryID] = true
		// A dead entry always accompanies a failed terminal marker; fold
		// the state defensively in case the terminal marker is the tail
		// record being repaired.
		if d := e.dels[en.DeliveryID]; d != nil && d.Status != StatusFailed {
			d.Status = StatusFailed
			if en.Reason != "" {
				d.Reason = en.Reason
			}
			d.UpdatedAt = en.Time
			e.markFinishedLocked(d)
		}
	}
}

// ensureEvent returns the eventState, creating it (used during replay).
func (e *Engine) ensureEvent(eventID string) *eventState {
	es, ok := e.events[eventID]
	if !ok {
		es = &eventState{
			report: &EventReport{EventID: eventID},
			done:   make(chan struct{}),
		}
		e.events[eventID] = es
	}
	return es
}

// markFinishedLocked updates counters and closes the event's done channel
// once every delivery is terminal. Caller holds e.mu.
func (e *Engine) markFinishedLocked(d *Delivery) {
	es := e.ensureEvent(d.EventID)
	// Idempotent: only count the first transition into a terminal state.
	key := d.ID
	if e.finishedM == nil {
		e.finishedM = make(map[string]bool)
	}
	if e.finishedM[key] {
		return
	}
	e.finishedM[key] = true
	es.finished++
	e.refreshReportLocked(es)
	if es.finished >= es.total && es.total > 0 {
		select {
		case <-es.done:
		default:
			close(es.done)
		}
	}
}

// refreshReportLocked rebuilds the readable result list. Caller holds e.mu.
func (e *Engine) refreshReportLocked(es *eventState) {
	results := make([]DeliveryResult, 0, len(es.report.Results))
	for _, r := range es.report.Results {
		d := e.dels[r.DeliveryID]
		if d == nil {
			continue
		}
		results = append(results, toResult(d))
	}
	sort.SliceStable(results, func(i, j int) bool { return results[i].Channel < results[j].Channel })
	es.report.Results = results
}

// Submit ingests one upstream event and fans it out according to the rules.
// It is idempotent on Event.ID: a repeated logical notification produces no
// additional downstream side effects and the existing report is returned.
func (e *Engine) Submit(ctx context.Context, ev Event) (*EventReport, error) {
	// Serialise check-then-accept for the same (and all) submissions. The
	// window includes fsyncs but submission volume is low relative to
	// delivery throughput, and correctness (no double fan-out) comes first.
	e.acceptMu.Lock()
	defer e.acceptMu.Unlock()
	e.mu.Lock()
	if e.stopped {
		e.mu.Unlock()
		return nil, errEngineStopped
	}
	// Deduplicate at the entry boundary.
	if es, ok := e.events[ev.ID]; ok {
		rep := cloneReport(es.report)
		e.mu.Unlock()
		return rep, nil
	}
	es := &eventState{
		report: &EventReport{EventID: ev.ID},
		done:   make(chan struct{}),
	}
	e.events[ev.ID] = es

	// Resolve target channels deterministically (sorted by channel name).
	targets := e.resolveTargetsLocked(ev.Type)
	es.total = len(targets)

	// Durably record acceptance BEFORE creating any delivery: a crash after
	// this point still dedups a resubmission.
	e.mu.Unlock()
	if _, err := e.store.Append(ctx, Entry{
		Kind: EntryAccepted, EventID: ev.ID, NotBefore: ev.NotBefore,
	}); err != nil {
		return nil, fmt.Errorf("persist accepted: %w", err)
	}

	for _, reg := range targets {
		payload := append([]byte(nil), ev.Payload...)
		did := deliveryID(ev.ID, reg.cfg.Name)
		d := &Delivery{
			ID: did, EventID: ev.ID, Channel: reg.cfg.Name,
			Status: StatusPending, CreatedAt: e.clk.Now(),
			UpdatedAt: e.clk.Now(), NotBefore: ev.NotBefore,
		}

		e.mu.Lock()
		// Re-check under lock: a concurrent Submit of the same event may
		// have won (deduplication is concurrency-safe).
		if existing := e.dels[did]; existing != nil {
			e.mu.Unlock()
			continue
		}
		e.dels[did] = d
		e.order = append(e.order, did)
		e.payloads[ev.ID] = payload
		es.report.Results = append(es.report.Results, DeliveryResult{
			DeliveryID: did, EventID: ev.ID, Channel: reg.cfg.Name,
		})
		ch := reg.ch
		cfg := reg.cfg
		disp := reg.disp
		disabled := cfg.Disabled
		e.mu.Unlock()

		if disabled {
			e.skipDelivery(ctx, d, SkipChannelDisabled)
			continue
		}
		if _, err := e.store.Append(ctx, Entry{
			Kind: EntryCreated, EventID: ev.ID, DeliveryID: did,
			Channel: reg.cfg.Name, Status: StatusPending, NotBefore: ev.NotBefore,
			Payload: payload,
		}); err != nil {
			return nil, fmt.Errorf("persist created: %w", err)
		}
		e.scheduleDelivery(reg, d, ch, cfg, disp, payload, ev.NotBefore, time.Time{})
	}

	// Close the done channel immediately if there were no targets or all
	// were skipped synchronously.
	e.mu.Lock()
	if es.finished >= es.total {
		select {
		case <-es.done:
		default:
			close(es.done)
		}
	}
	rep := cloneReport(es.report)
	e.mu.Unlock()
	return rep, nil
}

// resolveTargetsLocked returns the unique channel registrations matching an
// event type, ordered by channel name for deterministic fan-out.
func (e *Engine) resolveTargetsLocked(eventType string) []*channelReg {
	seen := make(map[string]bool)
	var names []string
	for _, r := range e.rules {
		if r.EventType != "" && r.EventType != eventType {
			continue
		}
		if seen[r.Channel] {
			continue
		}
		seen[r.Channel] = true
		names = append(names, r.Channel)
	}
	sort.Strings(names)
	out := make([]*channelReg, 0, len(names))
	for _, n := range names {
		out = append(out, e.channels[n])
	}
	return out
}

// deliveryID builds the stable per-channel idempotency key.
func deliveryID(eventID, channel string) string {
	return eventID + "#" + channel
}

// nextBackoff computes exponential backoff before the given (1-based)
// upcoming attempt. attempt is the attempt about to run (2,3,...).
func nextBackoff(initial, max time.Duration, attempt int) time.Duration {
	if initial <= 0 {
		return 0
	}
	d := initial
	for i := 2; i < attempt; i++ {
		d *= 2
		if max > 0 && d > max {
			return max
		}
	}
	if max > 0 && d > max {
		d = max
	}
	return d
}

func normalizeCfg(c *ChannelConfig) {
	if c.Concurrency <= 0 {
		c.Concurrency = 1
	}
	if c.MaxAttempts <= 0 {
		c.MaxAttempts = 1
	}
}

func toResult(d *Delivery) DeliveryResult {
	r := DeliveryResult{
		DeliveryID: d.ID, EventID: d.EventID, Channel: d.Channel,
		Status: d.Status, Reason: d.Reason, Attempts: d.AttemptCount,
	}
	switch d.Status {
	case StatusSuccess:
		r.Code = ResultSuccess
	case StatusSkipped:
		r.Code = ResultSkipped
	case StatusFailed:
		r.Code = ResultFailed
	default:
		r.Code = "" // still in progress
	}
	return r
}

func cloneReport(in *EventReport) *EventReport {
	out := &EventReport{EventID: in.EventID}
	out.Results = append(out.Results, in.Results...)
	return out
}
