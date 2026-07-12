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
	"context"
	"net/http"
	"sync"

	"github.com/google/uuid"
	"go.uber.org/atomic"
	"golang.org/x/net/websocket"

	"github.com/tochemey/goakt/v4/log"
)

// WSHandlerOption configures a WebSocket http.Handler created with NewWSHandler.
type WSHandlerOption func(*WSHandler)

// WithWSIDFunc sets the function used to derive a connection's id from the upgrade
// request. Without one, a random UUID is generated per connection.
func WithWSIDFunc(f func(*http.Request) string) WSHandlerOption {
	return func(h *WSHandler) { h.idFunc = f }
}

// WithWSAuthFunc sets the auth hook run during the WebSocket handshake, before the
// connection is accepted or registered. A non-nil error rejects the upgrade with
// HTTP 403 Forbidden.
func WithWSAuthFunc(f func(*http.Request) error) WSHandlerOption {
	return func(h *WSHandler) { h.authFunc = f }
}

// WithWSTopics sets the function used to derive the topics a connection should be
// joined to at registration time, from the upgrade request (e.g. a room/channel query
// parameter or claim in an auth token).
func WithWSTopics(f func(*http.Request) []string) WSHandlerOption {
	return func(h *WSHandler) { h.topicsFunc = f }
}

// WithWSOnMessage sets the callback invoked for every text/binary message received from
// the client.
func WithWSOnMessage(f func(ctx context.Context, id string, payload []byte)) WSHandlerOption {
	return func(h *WSHandler) { h.onMessage = f }
}

// WithWSOnConnect sets the callback invoked once a connection has been registered and
// is ready to receive deliveries.
func WithWSOnConnect(f func(ctx context.Context, id string, r *http.Request)) WSHandlerOption {
	return func(h *WSHandler) { h.onConnect = f }
}

// WithWSOnDisconnect sets the callback invoked once a connection has been unregistered.
func WithWSOnDisconnect(f func(id string)) WSHandlerOption {
	return func(h *WSHandler) { h.onDisconnect = f }
}

// WithWSSendBuffer sets the size of each connection's outbound buffer. When full,
// Registry.SendToConnection/Broadcast deliveries to that connection return
// ErrBackpressure instead of blocking. Defaults to 256.
func WithWSSendBuffer(size int) WSHandlerOption {
	return func(h *WSHandler) { h.bufferSize = size }
}

// WithWSLogger sets the logger used to report connection-handling errors. Defaults to
// log.DiscardLogger.
func WithWSLogger(logger log.Logger) WSHandlerOption {
	return func(h *WSHandler) { h.logger = logger }
}

// WSHandler upgrades incoming requests to WebSocket connections and registers each one
// in its Registry for the lifetime of the socket. It implements http.Handler; mount it
// only on the path(s) meant to accept WebSocket upgrades - plain (non-upgrade) HTTP
// handling is entirely out of scope here by design.
type WSHandler struct {
	registry     *Registry
	idFunc       func(*http.Request) string
	authFunc     func(*http.Request) error
	topicsFunc   func(*http.Request) []string
	onMessage    func(ctx context.Context, id string, payload []byte)
	onConnect    func(ctx context.Context, id string, r *http.Request)
	onDisconnect func(id string)
	bufferSize   int
	logger       log.Logger

	server *websocket.Server

	// connMu guards conns and draining. http.Server.Shutdown deliberately ignores
	// hijacked connections, so graceful drain has to be driven from here: Drain closes
	// every tracked socket, which unblocks each handler's read loop and lets the normal
	// unregister path run.
	connMu   sync.Mutex
	conns    map[*websocket.Conn]struct{}
	draining bool
}

// NewWSHandler returns a WSHandler bound to registry. The returned handler is an
// http.Handler; wire its Drain method into Server via WithDrainOnShutdown (or call it
// from your own shutdown path) so established sockets are evicted on shutdown.
func NewWSHandler(registry *Registry, opts ...WSHandlerOption) *WSHandler {
	h := &WSHandler{
		registry:   registry,
		bufferSize: 256,
		logger:     log.DiscardLogger,
		conns:      make(map[*websocket.Conn]struct{}),
	}
	for _, opt := range opts {
		opt(h)
	}

	h.server = &websocket.Server{
		Handshake: h.handshake,
		Handler:   h.handle,
	}
	return h
}

// ServeHTTP implements http.Handler.
func (h *WSHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.server.ServeHTTP(w, r)
}

// Drain closes every established WebSocket connection and rejects connections that are
// mid-upgrade, so a graceful server shutdown evicts long-lived sockets instead of
// leaving them until the peer disconnects. Safe to call more than once.
func (h *WSHandler) Drain() {
	h.connMu.Lock()
	h.draining = true
	conns := make([]*websocket.Conn, 0, len(h.conns))
	for c := range h.conns {
		conns = append(conns, c)
	}
	h.connMu.Unlock()

	for _, c := range conns {
		_ = c.Close()
	}
}

// track records an accepted connection for Drain, refusing it when draining already
// started so shutdown cannot race with late upgrades.
func (h *WSHandler) track(ws *websocket.Conn) bool {
	h.connMu.Lock()
	defer h.connMu.Unlock()
	if h.draining {
		return false
	}
	h.conns[ws] = struct{}{}
	return true
}

func (h *WSHandler) untrack(ws *websocket.Conn) {
	h.connMu.Lock()
	delete(h.conns, ws)
	h.connMu.Unlock()
}

// handshake runs the configured auth hook, if any, before the connection is accepted.
func (h *WSHandler) handshake(_ *websocket.Config, r *http.Request) error {
	if h.authFunc == nil {
		return nil
	}
	if err := h.authFunc(r); err != nil {
		return ErrUnauthorized
	}
	return nil
}

// handle is the websocket.Handler run for every accepted connection. It registers the
// connection, pumps outbound deliveries to the socket, and reads inbound messages until
// the socket closes, at which point it unregisters cleanly.
func (h *WSHandler) handle(ws *websocket.Conn) {
	if !h.track(ws) {
		_ = ws.Close()
		return
	}
	defer h.untrack(ws)

	req := ws.Request()
	ctx := req.Context()

	id := ""
	if h.idFunc != nil {
		id = h.idFunc(req)
	}
	if id == "" {
		id = uuid.NewString()
	}

	var topics []string
	if h.topicsFunc != nil {
		topics = h.topicsFunc(req)
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
		h.logger.Warnf("gateway: failed to register websocket connection %q: %v", id, err)
		return
	}

	stopWriter := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case payload := <-outbound:
				if err := websocket.Message.Send(ws, payload); err != nil {
					return
				}
			case <-stopWriter:
				return
			}
		}
	}()

	if h.onConnect != nil {
		h.onConnect(ctx, id, req)
	}

	for {
		var payload []byte
		if err := websocket.Message.Receive(ws, &payload); err != nil {
			break
		}
		if h.onMessage != nil {
			h.onMessage(ctx, id, payload)
		}
	}

	closed.Store(true)
	close(stopWriter)
	wg.Wait()

	if err := h.registry.Unregister(context.WithoutCancel(ctx), id); err != nil {
		h.logger.Warnf("gateway: failed to unregister websocket connection %q: %v", id, err)
	}
	if h.onDisconnect != nil {
		h.onDisconnect(id)
	}
}
