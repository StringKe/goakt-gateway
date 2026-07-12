// MIT License
//
// Copyright (c) 2026 StringKe
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

package gateway

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"sync"
	"time"

	"github.com/google/uuid"
	"go.uber.org/atomic"

	"github.com/tochemey/goakt/v4/log"
)

// SSEHandlerOption configures an http.Handler created with NewSSEHandler.
type SSEHandlerOption func(*SSEHandler)

// WithSSEIDFunc sets the function used to derive a connection's id from the request.
// Without one, a random UUID is generated per connection.
func WithSSEIDFunc(f func(*http.Request) string) SSEHandlerOption {
	return func(h *SSEHandler) { h.idFunc = f }
}

// WithSSEAuthFunc sets the auth hook run before a connection is accepted and
// registered. A non-nil error rejects the request with HTTP 403 Forbidden.
func WithSSEAuthFunc(f func(*http.Request) error) SSEHandlerOption {
	return func(h *SSEHandler) { h.authFunc = f }
}

// WithSSETopics sets the function used to derive the topics a connection should be
// joined to at registration time.
func WithSSETopics(f func(*http.Request) []string) SSEHandlerOption {
	return func(h *SSEHandler) { h.topicsFunc = f }
}

// WithSSEOnConnect sets the callback invoked once a connection has been registered and
// the response stream has been opened.
func WithSSEOnConnect(f func(ctx context.Context, id string, r *http.Request)) SSEHandlerOption {
	return func(h *SSEHandler) { h.onConnect = f }
}

// WithSSEOnDisconnect sets the callback invoked once a connection has been unregistered.
func WithSSEOnDisconnect(f func(id string)) SSEHandlerOption {
	return func(h *SSEHandler) { h.onDisconnect = f }
}

// WithSSESendBuffer sets the size of each connection's outbound buffer. When full,
// Registry.SendToConnection/Broadcast deliveries to that connection return
// ErrBackpressure instead of blocking. Defaults to 256.
func WithSSESendBuffer(size int) SSEHandlerOption {
	return func(h *SSEHandler) { h.bufferSize = size }
}

// WithSSEKeepAlive sets the interval at which a comment-only keepalive event is sent to
// detect dead connections and prevent idle-timing-out intermediate proxies. Defaults to
// 15 seconds.
func WithSSEKeepAlive(d time.Duration) SSEHandlerOption {
	return func(h *SSEHandler) { h.keepAlive = d }
}

// WithSSELogger sets the logger used to report connection-handling errors. Defaults to
// log.DiscardLogger.
func WithSSELogger(logger log.Logger) SSEHandlerOption {
	return func(h *SSEHandler) { h.logger = logger }
}

// SSEHandler opens a Server-Sent Events stream for every incoming request and registers
// each one in its Registry for the lifetime of the connection. SSE is one-way (server to
// client); inbound application data, if any, belongs in an ordinary HTTP endpoint the
// client posts to separately.
type SSEHandler struct {
	registry     *Registry
	idFunc       func(*http.Request) string
	authFunc     func(*http.Request) error
	topicsFunc   func(*http.Request) []string
	onConnect    func(ctx context.Context, id string, r *http.Request)
	onDisconnect func(id string)
	bufferSize   int
	keepAlive    time.Duration
	logger       log.Logger

	// shutdown unblocks every streaming loop on Drain. SSE handlers are ordinary
	// (non-hijacked) requests, so without this http.Server.Shutdown would wait on them
	// until its context expired.
	shutdown  chan struct{}
	drainOnce sync.Once
}

// enforce compilation error
var _ http.Handler = (*SSEHandler)(nil)

// NewSSEHandler returns an SSEHandler bound to registry. The returned handler is an
// http.Handler; wire its Drain method into Server via WithDrainOnShutdown (or call it
// from your own shutdown path) so open streams terminate promptly on shutdown.
func NewSSEHandler(registry *Registry, opts ...SSEHandlerOption) *SSEHandler {
	h := &SSEHandler{
		registry:   registry,
		bufferSize: 256,
		keepAlive:  15 * time.Second,
		logger:     log.DiscardLogger,
		shutdown:   make(chan struct{}),
	}
	for _, opt := range opts {
		opt(h)
	}
	return h
}

// Drain terminates every open SSE stream and makes new requests fail fast with 503, so
// a graceful server shutdown is not held hostage by long-lived streams. Safe to call
// more than once.
func (h *SSEHandler) Drain() {
	h.drainOnce.Do(func() { close(h.shutdown) })
}

// ServeHTTP implements http.Handler.
func (h *SSEHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	select {
	case <-h.shutdown:
		http.Error(w, "shutting down", http.StatusServiceUnavailable)
		return
	default:
	}

	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}

	if h.authFunc != nil {
		if err := h.authFunc(r); err != nil {
			http.Error(w, ErrUnauthorized.Error(), http.StatusForbidden)
			return
		}
	}

	ctx := r.Context()

	id := ""
	if h.idFunc != nil {
		id = h.idFunc(r)
	}
	if id == "" {
		id = uuid.NewString()
	}

	var topics []string
	if h.topicsFunc != nil {
		topics = h.topicsFunc(r)
	}

	var closed atomic.Bool
	outbound := make(chan []byte, h.bufferSize)
	send := func(payload []byte) error {
		if closed.Load() {
			return ErrConnectionClosed
		}
		select {
		case outbound <- payload:
			return nil
		default:
			return ErrBackpressure
		}
	}

	if err := h.registry.Register(ctx, id, send, topics...); err != nil {
		h.logger.Warnf("gateway: failed to register SSE connection %q: %v", id, err)
		http.Error(w, "registration failed", http.StatusInternalServerError)
		return
	}
	defer func() {
		closed.Store(true)
		if err := h.registry.Unregister(context.WithoutCancel(ctx), id); err != nil {
			h.logger.Warnf("gateway: failed to unregister SSE connection %q: %v", id, err)
		}
		if h.onDisconnect != nil {
			h.onDisconnect(id)
		}
	}()

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.WriteHeader(http.StatusOK)
	flusher.Flush()

	if h.onConnect != nil {
		h.onConnect(ctx, id, r)
	}

	ticker := time.NewTicker(h.keepAlive)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-h.shutdown:
			return
		case <-ticker.C:
			if _, err := io.WriteString(w, ": keepalive\n\n"); err != nil {
				return
			}
			flusher.Flush()
		case payload := <-outbound:
			if err := writeSSEEvent(w, payload); err != nil {
				return
			}
			flusher.Flush()
		}
	}
}

// writeSSEEvent writes payload to w as one SSE "data:" event, splitting on newlines per
// the SSE framing rules (https://html.spec.whatwg.org/multipage/server-sent-events.html).
func writeSSEEvent(w io.Writer, payload []byte) error {
	for _, line := range bytes.Split(payload, []byte("\n")) {
		if _, err := w.Write([]byte("data: ")); err != nil {
			return err
		}
		if _, err := w.Write(line); err != nil {
			return err
		}
		if _, err := w.Write([]byte("\n")); err != nil {
			return err
		}
	}
	_, err := w.Write([]byte("\n"))
	return err
}
