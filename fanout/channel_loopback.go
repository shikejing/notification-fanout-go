package fanout

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"time"
)

// LoopbackChannel delivers over HTTP to a server bound to the loopback
// interface. It exercises a real local network path (connect, write, read,
// status codes, deadlines) while staying fully local and reproducible. It
// sends the stable delivery id as the Idempotency-Key header so the server
// can suppress duplicate side effects after retries/replays.
type LoopbackChannel struct {
	cfg    LoopbackConfig
	client *http.Client
}

// LoopbackConfig configures a LoopbackChannel.
type LoopbackConfig struct {
	// Name is the channel identifier.
	Name string
	// URL must point at a loopback address (127.0.0.1, [::1] or localhost).
	URL string
	// AttemptTimeout bounds a single HTTP attempt. Zero uses no client
	// deadline (the per-attempt ctx from the engine still applies).
	AttemptTimeout time.Duration
}

// NewLoopbackChannel constructs a LoopbackChannel and validates that the URL
// host resolves to a loopback address.
func NewLoopbackChannel(cfg LoopbackConfig) (*LoopbackChannel, error) {
	u, err := url.Parse(cfg.URL)
	if err != nil {
		return nil, fmt.Errorf("loopback channel %q: parse url: %w", cfg.Name, err)
	}
	if err := assertLoopback(u.Hostname()); err != nil {
		return nil, fmt.Errorf("loopback channel %q: %w", cfg.Name, err)
	}
	tr := &http.Transport{
		Proxy:                 nil,
		MaxIdleConns:          16,
		IdleConnTimeout:       30 * time.Second,
		ResponseHeaderTimeout: 10 * time.Second,
	}
	c := &LoopbackChannel{
		cfg:    cfg,
		client: &http.Client{Transport: tr},
	}
	return c, nil
}

// Name returns the channel name.
func (c *LoopbackChannel) Name() string { return c.cfg.Name }

// Kind returns KindLoopback.
func (c *LoopbackChannel) Kind() ChannelKind { return KindLoopback }

// Send POSTs the payload with the Idempotency-Key header. Any transport error
// or non-2xx status is returned as a retriable error; the channel itself
// holds no state and relies on the server honouring the idempotency key.
func (c *LoopbackChannel) Send(ctx context.Context, deliveryID string, payload []byte) error {
	if c.cfg.AttemptTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, c.cfg.AttemptTimeout)
		defer cancel()
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.cfg.URL, bytes.NewReader(payload))
	if err != nil {
		return fmt.Errorf("loopback channel %q: build request: %w", c.cfg.Name, err)
	}
	req.Header.Set("Content-Type", "application/octet-stream")
	req.Header.Set("Idempotency-Key", deliveryID)
	resp, err := c.client.Do(req)
	if err != nil {
		return fmt.Errorf("loopback channel %q: transport: %w", c.cfg.Name, err)
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	if resp.StatusCode/100 != 2 {
		return fmt.Errorf("loopback channel %q: non-2xx status %d", c.cfg.Name, resp.StatusCode)
	}
	return nil
}

func assertLoopback(host string) error {
	switch strings.ToLower(host) {
	case "localhost", "127.0.0.1", "::1", "[::1]":
		return nil
	}
	ips, err := net.LookupIP(host)
	if err != nil {
		return fmt.Errorf("resolve %q: %w", host, err)
	}
	for _, ip := range ips {
		if ip.IsLoopback() {
			return nil
		}
	}
	return fmt.Errorf("host %q is not a loopback address", host)
}

// ---------------------------------------------------------------------------
// Test server (loopback, in-process) with programmable latency/failures and
// server-side idempotency enforcement.
// ---------------------------------------------------------------------------

// LoopbackServer is an in-process HTTP server that accepts deliveries on a
// loopback address, deduplicates by Idempotency-Key and records one side
// effect per key.
type LoopbackServer struct {
	mu        sync.Mutex
	latency   time.Duration
	failFirst int
	fresh     map[string]int
	observed  map[string][]byte
	delivered chan string
	blocked   map[string]chan struct{}
	server    *httptest.Server
}

// NewLoopbackServer constructs the programmable test server. Call Start to
// bind it; it listens on 127.0.0.1 with a random port.
func NewLoopbackServer(latency time.Duration, failFirst int) *LoopbackServer {
	return &LoopbackServer{
		latency:   latency,
		failFirst: failFirst,
		fresh:     make(map[string]int),
		observed:  make(map[string][]byte),
		delivered: make(chan string, 1024),
		blocked:   make(map[string]chan struct{}),
	}
}

// Start binds the server on loopback and returns its base URL.
func (s *LoopbackServer) Start() string {
	mux := http.NewServeMux()
	mux.HandleFunc("/deliver", s.handle)
	ts := httptest.NewUnstartedServer(mux)
	ts.Listener = newLoopbackListener(ts.Listener)
	ts.Start()
	s.mu.Lock()
	s.server = ts
	s.mu.Unlock()
	return ts.URL + "/deliver"
}

// Close stops the server.
func (s *LoopbackServer) Close() {
	s.mu.Lock()
	ts := s.server
	s.mu.Unlock()
	if ts != nil {
		ts.Close()
	}
}

// ObservedCount returns the number of distinct keys with a side effect.
func (s *LoopbackServer) ObservedCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.observed)
}

// HasObserved reports whether key produced a side effect exactly once.
func (s *LoopbackServer) HasObserved(key string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.observed[key]
	return ok
}

// Delivered returns a channel signalled per new side effect.
func (s *LoopbackServer) Delivered() <-chan string { return s.delivered }

// Block pauses processing of key until Release; used to simulate hung server.
func (s *LoopbackServer) Block(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.blocked[key]; !ok {
		s.blocked[key] = make(chan struct{})
	}
}

// Release unblocks key.
func (s *LoopbackServer) Release(key string) {
	s.mu.Lock()
	ch := s.blocked[key]
	delete(s.blocked, key)
	s.mu.Unlock()
	if ch != nil {
		close(ch)
	}
}

func (s *LoopbackServer) handle(w http.ResponseWriter, r *http.Request) {
	key := r.Header.Get("Idempotency-Key")
	if key == "" {
		http.Error(w, "missing Idempotency-Key", http.StatusBadRequest)
		return
	}

	s.mu.Lock()
	if _, done := s.observed[key]; done {
		s.mu.Unlock()
		// Idempotent replay: report success without repeating the effect.
		w.WriteHeader(http.StatusOK)
		return
	}
	s.fresh[key]++
	attempt := s.fresh[key]
	ch := s.blocked[key]
	latency := s.latency
	fail := attempt <= s.failFirst
	s.mu.Unlock()

	if ch != nil {
		select {
		case <-r.Context().Done():
			return
		case <-ch:
		}
	}
	if latency > 0 {
		select {
		case <-r.Context().Done():
			return
		case <-time.After(latency):
		}
	}
	if fail {
		http.Error(w, fmt.Sprintf("injected failure attempt %d", attempt), http.StatusInternalServerError)
		return
	}
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "read body", http.StatusBadRequest)
		return
	}

	s.mu.Lock()
	if _, done := s.observed[key]; done {
		s.mu.Unlock()
		w.WriteHeader(http.StatusOK)
		return
	}
	cp := make([]byte, len(body))
	copy(cp, body)
	s.observed[key] = cp
	select {
	case s.delivered <- key:
	default:
	}
	s.mu.Unlock()
	w.WriteHeader(http.StatusOK)
}

// loopbackListener guarantees the test server only accepts loopback peers.
type loopbackListener struct{ net.Listener }

func newLoopbackListener(l net.Listener) net.Listener { return &loopbackListener{l} }

func (l *loopbackListener) Accept() (net.Conn, error) {
	for {
		c, err := l.Listener.Accept()
		if err != nil {
			return nil, err
		}
		host, _, _ := net.SplitHostPort(c.RemoteAddr().String())
		if ip := net.ParseIP(host); ip != nil && ip.IsLoopback() {
			return c, nil
		}
		_ = c.Close()
	}
}
