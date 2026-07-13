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
	"errors"
	"net/http"
	"net/url"
	"path"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/coder/websocket"
	"github.com/google/uuid"
	"golang.org/x/time/rate"

	"github.com/tochemey/goakt/v4/log"
)

const (
	// defaultWSPingInterval is how often an idle connection is probed. A gateway sits behind
	// load balancers and mobile networks that silently drop idle flows; without an
	// application-level probe a half-open socket looks alive until TCP keepalive notices,
	// which can take over an hour with the default kernel settings.
	defaultWSPingInterval = 30 * time.Second

	// defaultWSPongTimeout is how long a ping waits for its pong before the peer is declared
	// gone.
	defaultWSPongTimeout = 10 * time.Second

	// defaultWSWriteTimeout bounds a single outbound frame write, so one stalled TCP receiver
	// cannot pin a writer goroutine forever.
	defaultWSWriteTimeout = 10 * time.Second

	// defaultWSReadLimit caps a single inbound message at 1 MiB. Anything larger is a client
	// bug or an attack: gateways carry control messages, not uploads.
	defaultWSReadLimit = 1 << 20

	// defaultWSSendBuffer is the per-connection outbound queue depth.
	defaultWSSendBuffer = 256

	// wsSubprotocolMetaKey is the ConnInfo.Meta key under which the negotiated subprotocol is
	// published to the application callbacks.
	wsSubprotocolMetaKey = "subprotocol"
)

// WSAuthFunc authenticates an upgrade request and resolves the connection's identity in one
// step. Returning a non-nil error rejects the upgrade with HTTP 403.
//
// The returned ConnInfo is carried through registration and into every subsequent callback,
// so the auth token is parsed exactly once instead of once per attribute the handler needs.
// Any field may be left zero: an empty ID falls back to WithWSIDFunc and then to a generated
// UUID, and empty Topics fall back to WithWSTopics.
type WSAuthFunc func(*http.Request) (*ConnInfo, error)

// WSHandlerOption configures a WebSocket http.Handler created with NewWSHandler.
type WSHandlerOption func(*WSHandler)

// WithWSAuth sets the auth hook run before the upgrade is accepted. A non-nil error rejects
// the request with HTTP 403 Forbidden.
func WithWSAuth(f WSAuthFunc) WSHandlerOption {
	return func(h *WSHandler) { h.auth = f }
}

// WithWSIDFunc sets the fallback used to derive a connection's id from the upgrade request
// when WSAuthFunc did not supply one. Without either, a random UUID is generated per
// connection.
//
// Security: the connection id is a takeover key, not just a label. Because takeover is on by
// default (see WithWSReplaceExisting), a client that controls the id it registers under can
// evict any live connection that already holds that id and rebind it to itself: every later
// SendToConnection and SendToGroup delivery for that id then lands on the attacker's socket.
// Derive the id from an authenticated principal (do that in WSAuthFunc, which runs first),
// never from an unauthenticated request field such as a query parameter or client-supplied
// header.
func WithWSIDFunc(f func(*http.Request) string) WSHandlerOption {
	return func(h *WSHandler) { h.idFunc = f }
}

// WithWSTopics sets the fallback used to derive the topics a connection joins at registration
// time when WSAuthFunc did not supply any (e.g. a room query parameter).
func WithWSTopics(f func(*http.Request) []string) WSHandlerOption {
	return func(h *WSHandler) { h.topicsFunc = f }
}

// WithWSOriginPatterns authorizes cross-origin upgrade requests whose Origin host matches one
// of the patterns. Patterns are matched case-insensitively with path.Match against the origin
// host, or against "scheme://host" when the pattern itself contains "://".
//
// By default only same-Host origins are accepted. WebSockets are exempt from the same-origin
// policy while the browser still attaches cookies to the upgrade request, so an unchecked
// origin is a cross-site WebSocket hijacking hole.
func WithWSOriginPatterns(patterns ...string) WSHandlerOption {
	return func(h *WSHandler) { h.originPatterns = append(h.originPatterns, patterns...) }
}

// WithWSInsecureSkipOriginCheck disables origin verification entirely.
//
// Any web page on any site can then open an authenticated WebSocket to this handler using the
// visitor's cookies, read everything pushed to that user and send messages as them. Only use
// it when the handler is not cookie-authenticated at all (e.g. a bearer token supplied by the
// client) and you accept that consequence.
func WithWSInsecureSkipOriginCheck() WSHandlerOption {
	return func(h *WSHandler) { h.insecureSkipOriginCheck = true }
}

// WithWSSubprotocols lists the subprotocols the handler is willing to negotiate, in order of
// preference. The negotiated result is published to the application as
// ConnInfo.Meta["subprotocol"].
func WithWSSubprotocols(protocols ...string) WSHandlerOption {
	return func(h *WSHandler) { h.subprotocols = append(h.subprotocols, protocols...) }
}

// WithWSMessageType sets the frame type used for outbound payloads. Defaults to
// websocket.MessageText, because a browser handed a binary frame receives a Blob rather than a
// string and every JSON-over-WebSocket client breaks on it.
func WithWSMessageType(t websocket.MessageType) WSHandlerOption {
	return func(h *WSHandler) { h.messageType = t }
}

// WithWSPingInterval sets how often an otherwise idle connection is probed with a ping.
// Defaults to 30 seconds; 0 disables keepalive probing.
func WithWSPingInterval(d time.Duration) WSHandlerOption {
	return func(h *WSHandler) { h.pingInterval = d }
}

// WithWSPongTimeout sets how long a ping waits for its pong before the connection is torn
// down. Defaults to 10 seconds.
func WithWSPongTimeout(d time.Duration) WSHandlerOption {
	return func(h *WSHandler) { h.pongTimeout = d }
}

// WithWSWriteTimeout bounds a single outbound frame write. Defaults to 10 seconds.
func WithWSWriteTimeout(d time.Duration) WSHandlerOption {
	return func(h *WSHandler) { h.writeTimeout = d }
}

// WithWSReadLimit caps the size of a single inbound message. Exceeding it closes the
// connection with status 1009 (message too big). Defaults to 1 MiB; a negative value disables
// the limit.
func WithWSReadLimit(n int64) WSHandlerOption {
	return func(h *WSHandler) { h.readLimit = n }
}

// WithWSInboundRate limits how many inbound messages a single connection may send per second,
// with burst as the bucket depth. Disabled by default. A perSecond <= 0 leaves it disabled;
// a burst < 1 is treated as 1 (a burst of 0 would otherwise reject every message).
//
// What happens on excess follows the backpressure policy: BackpressureDrop discards the
// offending message, BackpressureClose tears the connection down with status 1008. Either way
// the rejection is logged as ErrRateLimited.
func WithWSInboundRate(perSecond float64, burst int) WSHandlerOption {
	return func(h *WSHandler) {
		h.inboundRate = perSecond
		h.inboundBurst = burst
	}
}

// WithWSBackpressurePolicy decides what happens to a connection whose outbound buffer is full.
// Defaults to BackpressureDrop.
func WithWSBackpressurePolicy(p BackpressurePolicy) WSHandlerOption {
	return func(h *WSHandler) { h.backpressure = p }
}

// WithWSOnMessage sets the callback invoked for every message received from the client, with
// the identity resolved during the handshake.
func WithWSOnMessage(f func(ctx context.Context, info *ConnInfo, payload []byte)) WSHandlerOption {
	return func(h *WSHandler) { h.onMessage = f }
}

// WithWSOnConnect sets the callback invoked once a connection has been registered and is ready
// to receive deliveries.
func WithWSOnConnect(f func(ctx context.Context, info *ConnInfo, r *http.Request)) WSHandlerOption {
	return func(h *WSHandler) { h.onConnect = f }
}

// WithWSOnDisconnect sets the callback invoked once a connection's socket is gone. It also runs
// for a connection evicted by a takeover, which is the only way an application can tell that
// this particular socket - rather than the identity behind it - went away.
func WithWSOnDisconnect(f func(info *ConnInfo)) WSHandlerOption {
	return func(h *WSHandler) { h.onDisconnect = f }
}

// WithWSSendBuffer sets the size of each connection's outbound buffer. When full, deliveries to
// that connection follow the backpressure policy instead of blocking. Defaults to 256.
func WithWSSendBuffer(size int) WSHandlerOption {
	return func(h *WSHandler) { h.bufferSize = size }
}

// WithWSLogger sets the logger used to report connection-handling errors. Defaults to
// log.DiscardLogger.
func WithWSLogger(logger log.Logger) WSHandlerOption {
	return func(h *WSHandler) { h.logger = logger }
}

// WithWSReplaceExisting makes a new connection evict any existing one with the same id. It is
// already the default and the option exists to state that intent explicitly.
//
// Takeover is the default because the alternative strands the user: behind a half-open TCP
// connection (a phone switching from wifi to cellular) the previous socket stays readable for
// as long as TCP keepalive takes to notice, and rejecting the reconnect with
// ErrConnectionExists would keep the client offline for exactly that long.
//
// Security: takeover trusts that the reconnecting client is the same principal as the one it
// evicts. That is only true when the connection id is bound to an authenticated identity. If
// the id comes from an unauthenticated request field (see the warning on WithWSIDFunc), an
// attacker who supplies a victim's id both kicks the victim offline and hijacks every future
// delivery addressed to it.
func WithWSReplaceExisting() WSHandlerOption {
	return func(h *WSHandler) { h.replaceExisting = true }
}

// WithWSCompression enables the permessage-deflate extension with the given mode. Defaults to
// websocket.CompressionDisabled, so an unconfigured handler negotiates no compression and pays
// nothing for it. Compression is only negotiated when the client offers it too; a client that
// does not falls back to uncompressed frames transparently.
//
// Modes are the ones coder/websocket defines: CompressionContextTakeover keeps a sliding
// window across messages for a better ratio at the cost of memory, CompressionNoContextTakeover
// resets per message for less memory and a worse ratio.
func WithWSCompression(mode websocket.CompressionMode) WSHandlerOption {
	return func(h *WSHandler) { h.compression = mode }
}

// WithWSReauth periodically re-runs f against the original upgrade request to re-check a
// connection whose authorization can be revoked while the socket is open (a role change, a
// logout, an expiring token). The first failure closes the connection with status 1008 (policy
// violation) and unregisters it. Disabled by default; a non-positive interval or a nil f leaves
// it disabled.
//
// f is the same shape as the handshake WSAuthFunc but is consulted only for its error: the
// re-resolved identity is not applied to the live connection, because changing a socket's id or
// group mid-flight would strand deliveries already addressed to it. Only header and cookie based
// auth can be re-checked this way, since that is all the retained request still carries.
func WithWSReauth(interval time.Duration, f WSAuthFunc) WSHandlerOption {
	return func(h *WSHandler) {
		h.reauthInterval = interval
		h.reauth = f
	}
}

// WithWSGroupRate limits the aggregate inbound message rate of every connection in the same
// group on this node, sharing one token bucket per group (perSecond sustained, burst depth).
// Disabled by default; a perSecond <= 0 leaves it disabled, and a burst < 1 is treated as 1 so
// a misconfigured burst limits hard rather than rejecting every message.
//
// It composes with WithWSInboundRate: a message must satisfy both the per-connection and the
// per-group limiter to be accepted. On excess the backpressure policy decides the outcome, the
// same as the per-connection limiter: BackpressureDrop discards the offending message,
// BackpressureClose closes the connection with status 1008. Connections with an empty group are
// not group-limited, since they share no group bucket.
func WithWSGroupRate(perSecond float64, burst int) WSHandlerOption {
	return func(h *WSHandler) {
		h.groupRatePerSec = perSecond
		h.groupRateBurst = burst
	}
}

// WSHandler upgrades incoming requests to WebSocket connections and registers each one in its
// Registry for the lifetime of the socket. It implements http.Handler; mount it only on the
// path(s) meant to accept WebSocket upgrades - plain (non-upgrade) HTTP handling is entirely
// out of scope here by design.
type WSHandler struct {
	registry *Registry

	auth       WSAuthFunc
	idFunc     func(*http.Request) string
	topicsFunc func(*http.Request) []string

	originPatterns          []string
	insecureSkipOriginCheck bool
	subprotocols            []string
	compression             websocket.CompressionMode

	messageType  websocket.MessageType
	pingInterval time.Duration
	pongTimeout  time.Duration
	writeTimeout time.Duration
	readLimit    int64
	inboundRate  float64
	inboundBurst int
	backpressure BackpressurePolicy
	bufferSize   int

	reauth         WSAuthFunc
	reauthInterval time.Duration

	// groupRatePerSec and groupRateBurst are the raw WithWSGroupRate settings; NewWSHandler
	// turns them into groupRate once, so a disabled limiter costs nothing per connection.
	groupRatePerSec float64
	groupRateBurst  int
	groupRate       *groupRateLimiters

	onMessage    func(ctx context.Context, info *ConnInfo, payload []byte)
	onConnect    func(ctx context.Context, info *ConnInfo, r *http.Request)
	onDisconnect func(info *ConnInfo)

	replaceExisting bool
	logger          log.Logger

	// ids serializes the registration transitions of one connection id. A takeover and the
	// evicted socket's own teardown both act on that id in the Registry, and without per-id
	// ordering the loser's Unregister can delete the winner's registration.
	ids wsConnLocks

	// connMu guards conns and draining. http.Server.Shutdown deliberately ignores hijacked
	// connections, so graceful drain has to be driven from here. The table is also what lets a
	// takeover close the socket it evicted: the Registry only drops the registration, it has no
	// handle on the connection itself.
	connMu   sync.Mutex
	conns    map[string]*wsConn
	draining bool

	// teardownWG counts every connection from the moment register() admits it (inside the same
	// connMu critical section that checks draining) until its Registry teardown - not merely its
	// socket close - has finished. Drain waits on it so a rolling deploy that calls Drain and
	// then exits cannot leave a connection's cluster-wide actor name (and, with WithOwnerLease,
	// its owner lease) behind for however long its TTL takes to expire. Counting inside that same
	// critical section, rather than after RegisterHandle returns, is what stops a registration
	// that is still in flight when Drain runs from being missed by teardownWG.Wait(): either it
	// is admitted (and counted) before Drain observes draining, or Drain observes draining first
	// and the registration never proceeds to spawn anything for Drain to have to wait for.
	teardownWG sync.WaitGroup
}

// NewWSHandler returns a WSHandler bound to registry. The returned handler is an http.Handler;
// wire its Drain method into Server via WithDrainOnShutdown (or call it from your own shutdown
// path) so established sockets are evicted on shutdown.
func NewWSHandler(registry *Registry, opts ...WSHandlerOption) *WSHandler {
	h := &WSHandler{
		registry:        registry,
		messageType:     websocket.MessageText,
		pingInterval:    defaultWSPingInterval,
		pongTimeout:     defaultWSPongTimeout,
		writeTimeout:    defaultWSWriteTimeout,
		readLimit:       defaultWSReadLimit,
		bufferSize:      defaultWSSendBuffer,
		replaceExisting: true,
		logger:          log.DiscardLogger,
		conns:           make(map[string]*wsConn),
	}
	for _, opt := range opts {
		opt(h)
	}
	if h.bufferSize <= 0 {
		h.bufferSize = defaultWSSendBuffer
	}
	if h.pongTimeout <= 0 {
		h.pongTimeout = defaultWSPongTimeout
	}
	if h.writeTimeout <= 0 {
		h.writeTimeout = defaultWSWriteTimeout
	}
	if h.groupRatePerSec > 0 {
		h.groupRate = newGroupRateLimiters(h.groupRatePerSec, h.groupRateBurst)
	}
	h.ids.locks = make(map[string]*wsConnLock)
	return h
}

// ServeHTTP implements http.Handler.
func (h *WSHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if h.isDraining() {
		http.Error(w, "server is shutting down", http.StatusServiceUnavailable)
		return
	}

	if err := h.authorizeOrigin(r); err != nil {
		h.logger.Warnf("gateway: rejected websocket upgrade from origin %q: %v", r.Header.Get("Origin"), err)
		http.Error(w, ErrOriginNotAllowed.Error(), http.StatusForbidden)
		return
	}

	info, err := h.resolveConnInfo(r)
	if err != nil {
		h.logger.Warnf("gateway: rejected websocket upgrade: %v", err)
		http.Error(w, ErrUnauthorized.Error(), http.StatusForbidden)
		return
	}

	conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{
		Subprotocols:       h.subprotocols,
		OriginPatterns:     h.originPatterns,
		InsecureSkipVerify: h.insecureSkipOriginCheck,
		CompressionMode:    h.compression,
	})
	if err != nil {
		// Accept has already written the failure response.
		h.logger.Warnf("gateway: failed to accept websocket upgrade: %v", err)
		return
	}
	if subprotocol := conn.Subprotocol(); subprotocol != "" {
		info.Meta[wsSubprotocolMetaKey] = subprotocol
	}
	conn.SetReadLimit(h.readLimit)

	h.serve(r, conn, info)
}

// serve owns an accepted connection end to end: registration, the writer and keepalive
// goroutines, the inbound read loop, and teardown.
func (h *WSHandler) serve(r *http.Request, conn *websocket.Conn, info *ConnInfo) {
	// The request context is bound to the HTTP handler and is unreliable once the connection
	// has been hijacked, so the socket gets a lifetime of its own while keeping the request's
	// values (trace ids, and so on).
	ctx, cancel := context.WithCancel(context.WithoutCancel(r.Context()))
	defer cancel()

	c := &wsConn{
		conn:       conn,
		info:       info,
		outbound:   make(chan []byte, h.bufferSize),
		disconnect: make(chan string, 1),
	}

	if err := h.register(ctx, c); err != nil {
		h.logger.Warnf("gateway: failed to register websocket connection %q: %v", info.ID, err)
		c.close(websocket.StatusInternalError, "registration failed")
		return
	}

	// The group token bucket is shared by every connection of this group on this node, so it is
	// acquired for the connection's lifetime and released on teardown rather than rebuilt per
	// message. An empty group shares no bucket and is left unlimited.
	if h.groupRate != nil && info.Group != "" {
		c.groupLimiter = h.groupRate.acquire(info.Group)
		defer h.groupRate.release(info.Group)
	}

	var wg sync.WaitGroup
	wg.Go(func() {
		h.writeLoop(ctx, c)
	})

	if h.pingInterval > 0 {
		wg.Go(func() {
			h.pingLoop(ctx, c)
		})
	}

	if h.reauth != nil && h.reauthInterval > 0 {
		wg.Go(func() {
			h.reauthLoop(ctx, c, r)
		})
	}

	if h.onConnect != nil {
		h.onConnect(ctx, info, r)
	}

	h.readLoop(ctx, c)

	cancel()
	wg.Wait()
	c.abort()

	h.unregister(context.WithoutCancel(ctx), c)
	if h.onDisconnect != nil {
		h.onDisconnect(info)
	}
}

// register publishes c in the handler's connection table and in the Registry, evicting the
// socket it takes over from. Both steps run under the id's lock so that the evicted socket's
// teardown cannot race ahead and unregister the connection that just replaced it.
func (h *WSHandler) register(ctx context.Context, c *wsConn) error {
	id := c.info.ID

	lock := h.ids.lock(id)
	defer h.ids.unlock(id, lock)

	h.connMu.Lock()
	if h.draining {
		h.connMu.Unlock()
		return ErrConnectionClosed
	}
	evicted := h.conns[id]
	h.conns[id] = c
	// Counted in the same critical section as the draining check and the table insert above; see
	// teardownWG's doc comment for why that ordering is what makes Drain's Wait race-free.
	h.teardownWG.Add(1)
	h.connMu.Unlock()

	opts := []RegisterOption{
		WithConnGroup(c.info.Group),
		WithConnTopics(c.info.Topics...),
		WithConnMeta(c.info.Meta),
		// The close hook is how Registry.Disconnect and DisconnectGroup reach this socket: the
		// Registry holds only the send function, not the connection, so it cannot frame a close
		// on its own. requestClose routes the instruction to the write loop, the sole goroutine
		// allowed to write to the socket.
		WithConnCloseHook(c.requestClose),
	}
	if h.replaceExisting {
		opts = append(opts, WithReplaceExisting())
	}

	handle, err := h.registry.RegisterHandle(ctx, id, c.send(h), opts...)
	if err != nil {
		h.connMu.Lock()
		if h.conns[id] == c {
			if evicted != nil {
				h.conns[id] = evicted
			} else {
				delete(h.conns, id)
			}
		}
		h.connMu.Unlock()
		// unregister() is never called for a registration that did not succeed - serve() closes
		// the socket and returns instead - so this is the only Done that will ever pay off the
		// Add above.
		h.teardownWG.Done()
		return err
	}
	c.handle = handle

	if evicted != nil {
		// The evicted socket's own teardown will block on this id's lock until we are done, and
		// will then see that it no longer owns the id and leave the new registration alone.
		go evicted.close(websocket.StatusPolicyViolation, "connection replaced")
	}
	return nil
}

// unregister removes c from the Registry, but only while c still owns its id: a connection
// that was evicted by a takeover must not tear down the registration of the socket that
// replaced it. It tears down through c's entry-guarded ConnHandle rather than an id-scoped
// Unregister: an id-scoped call resolves whatever is currently registered under id at the
// moment it runs, so a stale c whose id was already reused by a same-id takeover elsewhere
// (this handler's own h.conns is process-local and cannot observe a takeover that landed on
// another node) could otherwise delete the newer owner's registration out from under it. The
// handle's UnregisterHandle instead only ever tears down the exact entry c registered.
func (h *WSHandler) unregister(ctx context.Context, c *wsConn) {
	defer h.teardownWG.Done()

	id := c.info.ID

	lock := h.ids.lock(id)
	defer h.ids.unlock(id, lock)

	h.connMu.Lock()
	owner := h.conns[id] == c
	if owner {
		delete(h.conns, id)
	}
	h.connMu.Unlock()

	if !owner {
		return
	}
	if err := c.handle.UnregisterHandle(ctx); err != nil {
		h.logger.Warnf("gateway: failed to unregister websocket connection %q: %v", id, err)
	}
}

// readLoop pumps inbound messages until the socket dies, enforcing the inbound rate limit.
func (h *WSHandler) readLoop(ctx context.Context, c *wsConn) {
	var limiter *rate.Limiter
	if h.inboundRate > 0 {
		// A burst below 1 would make x/time/rate reject every message; clamp to 1 so a
		// misconfigured burst limits hard rather than silently failing open to no limit.
		burst := max(h.inboundBurst, 1)
		limiter = rate.NewLimiter(rate.Limit(h.inboundRate), burst)
	}

	for {
		_, payload, err := c.conn.Read(ctx)
		if err != nil {
			if errors.Is(err, websocket.ErrMessageTooBig) {
				h.logger.Warnf("gateway: websocket connection %q: %v", c.info.ID, ErrPayloadTooLarge)
			}
			return
		}

		// Both limiters must admit the message. The per-connection check is short-circuited first,
		// so a message the connection's own limit already rejects does not also spend a token from
		// the shared group bucket.
		if (limiter != nil && !limiter.Allow()) || (c.groupLimiter != nil && !c.groupLimiter.Allow()) {
			h.logger.Warnf("gateway: websocket connection %q: %v", c.info.ID, ErrRateLimited)
			if h.backpressure == BackpressureClose {
				c.close(websocket.StatusPolicyViolation, ErrRateLimited.Error())
				return
			}
			continue
		}

		if h.onMessage != nil {
			h.onMessage(ctx, c.info, payload)
		}
	}
}

// writeLoop drains the outbound buffer to the socket. A write that cannot complete within the
// write timeout kills the connection: the peer's receive window has been closed long enough
// that the session is worthless anyway, and holding the goroutine would leak it.
func (h *WSHandler) writeLoop(ctx context.Context, c *wsConn) {
	for {
		select {
		case <-ctx.Done():
			return
		case reason := <-c.disconnect:
			// A forced close (Registry.Disconnect, or a failed reauthentication) uses 1008 policy
			// violation: it tells the client the server deliberately ended the session, so it does
			// not treat the close as a transient fault and reconnect aggressively the way a 1011
			// would invite.
			c.close(websocket.StatusPolicyViolation, reason)
			return
		case payload := <-c.outbound:
			writeCtx, cancel := context.WithTimeout(ctx, h.writeTimeout)
			err := c.conn.Write(writeCtx, h.messageType, payload)
			cancel()
			if err != nil {
				h.logger.Warnf("gateway: failed to write to websocket connection %q: %v", c.info.ID, err)
				c.abort()
				return
			}
		}
	}
}

// reauthLoop periodically re-checks a connection's authorization against the original upgrade
// request and forces it closed on the first failure. It shares the connection's context, so it
// stops as soon as the socket is torn down for any other reason.
func (h *WSHandler) reauthLoop(ctx context.Context, c *wsConn, r *http.Request) {
	ticker := time.NewTicker(h.reauthInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if _, err := h.reauth(r); err != nil {
				h.logger.Warnf("gateway: websocket connection %q failed reauthentication: %v", c.info.ID, err)
				c.requestClose("reauthentication failed")
				return
			}
		}
	}
}

// pingLoop probes an idle connection and tears it down when no pong comes back in time. This is
// what makes a half-open socket (a phone that changed networks) die promptly instead of holding
// its id hostage until TCP keepalive expires.
func (h *WSHandler) pingLoop(ctx context.Context, c *wsConn) {
	ticker := time.NewTicker(h.pingInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			pingCtx, cancel := context.WithTimeout(ctx, h.pongTimeout)
			err := c.conn.Ping(pingCtx)
			cancel()
			if err != nil {
				if ctx.Err() == nil {
					h.logger.Debugf("gateway: websocket connection %q failed to answer a ping: %v", c.info.ID, err)
				}
				c.abort()
				return
			}
		}
	}
}

// Drain closes every established WebSocket connection with status 1001 (going away) and rejects
// further upgrades, so a graceful server shutdown evicts long-lived sockets instead of leaving
// them until the peer disconnects.
//
// The close code matters: 1001 tells the client this is a planned server-side departure (a
// rolling deploy), which is what lets it reconnect promptly instead of applying the long backoff
// it would use for an abnormal close. Safe to call more than once.
func (h *WSHandler) Drain() {
	h.connMu.Lock()
	h.draining = true
	conns := make([]*wsConn, 0, len(h.conns))
	for _, c := range h.conns {
		conns = append(conns, c)
	}
	h.connMu.Unlock()

	var wg sync.WaitGroup
	for _, c := range conns {
		wg.Go(func() {
			c.close(websocket.StatusGoingAway, "server is shutting down")
		})
	}
	wg.Wait()

	// Closing the socket only starts a connection's teardown: readLoop notices the close and
	// unwinds serve() into unregister(), which is what actually frees the connection's
	// cluster-wide actor name and (with WithOwnerLease) its owner lease. Returning as soon as the
	// close handshakes above complete - without waiting here - would let a caller proceed to kill
	// the process before that teardown ran, leaving a stale actor/lease behind for however long
	// its TTL takes to expire.
	h.teardownWG.Wait()
}

// isDraining reports whether Drain has been called.
func (h *WSHandler) isDraining() bool {
	h.connMu.Lock()
	defer h.connMu.Unlock()
	return h.draining
}

// resolveConnInfo runs the auth hook and fills in whatever identity it left unset.
func (h *WSHandler) resolveConnInfo(r *http.Request) (*ConnInfo, error) {
	var info *ConnInfo
	if h.auth != nil {
		resolved, err := h.auth(r)
		if err != nil {
			return nil, err
		}
		info = resolved
	}
	if info == nil {
		info = &ConnInfo{}
	}
	if info.ID == "" && h.idFunc != nil {
		info.ID = h.idFunc(r)
	}
	if info.ID == "" {
		info.ID = uuid.NewString()
	}
	if len(info.Topics) == 0 && h.topicsFunc != nil {
		info.Topics = h.topicsFunc(r)
	}
	if info.Meta == nil {
		info.Meta = make(map[string]string)
	}
	return info, nil
}

// authorizeOrigin rejects a cross-origin upgrade whose Origin matches none of the configured
// patterns. It mirrors the check websocket.Accept performs so that the rejection surfaces as
// ErrOriginNotAllowed with a logged reason; Accept then runs the very same check with the very
// same patterns, so an error here can only ever make the handler stricter, never more
// permissive.
func (h *WSHandler) authorizeOrigin(r *http.Request) error {
	if h.insecureSkipOriginCheck {
		return nil
	}

	origin := r.Header.Get("Origin")
	if origin == "" {
		// A browser always sends Origin. A request without one is a non-browser client, which
		// carries no ambient cookies and therefore none of the risk origin checking exists for.
		return nil
	}

	u, err := url.Parse(origin)
	if err != nil {
		return ErrOriginNotAllowed
	}
	if strings.EqualFold(u.Host, r.Host) {
		return nil
	}

	for _, pattern := range h.originPatterns {
		target := strings.ToLower(u.Host)
		if strings.Contains(pattern, "://") {
			target = strings.ToLower(u.Scheme + "://" + u.Host)
		}
		matched, err := path.Match(strings.ToLower(pattern), target)
		if err != nil {
			return ErrOriginNotAllowed
		}
		if matched {
			return nil
		}
	}
	return ErrOriginNotAllowed
}

// wsConn is one accepted socket and the queue feeding it.
type wsConn struct {
	conn     *websocket.Conn
	info     *ConnInfo
	outbound chan []byte

	// handle is the entry-guarded ConnHandle register() obtained for this connection. It is set
	// once, before the socket becomes reachable by unregister(), and read only from there.
	handle *ConnHandle

	// disconnect carries an out-of-band close instruction (a Registry.Disconnect kick or a
	// failed reauthentication) to the write loop, the only goroutine allowed to frame a close.
	// It is buffered by one and gated by disconnectOnce so the instruction never blocks its
	// sender and a second instruction cannot double-send on a full channel.
	disconnect     chan string
	disconnectOnce sync.Once

	// groupLimiter is the token bucket shared by this connection's group on this node, or nil
	// when group rate limiting is disabled or the connection has no group.
	groupLimiter *rate.Limiter

	closed    atomic.Bool
	closeOnce sync.Once
}

// requestClose asks the write loop to close this socket with the given reason. It is the close
// hook handed to the Registry, so it must be safe to call from any goroutine and idempotent:
// the buffered channel plus disconnectOnce guarantee both, and a late call after the write loop
// has already exited simply fills the buffer and is harmless.
func (c *wsConn) requestClose(reason string) {
	c.disconnectOnce.Do(func() {
		c.disconnect <- reason
	})
}

// send builds the delivery function the Registry writes through. It never blocks: a delivery
// that would have to wait for a slow reader is either dropped or costs the reader its
// connection, per the handler's backpressure policy.
func (c *wsConn) send(h *WSHandler) func([]byte) error {
	return func(payload []byte) error {
		if c.closed.Load() {
			return ErrConnectionClosed
		}
		select {
		case c.outbound <- payload:
			return nil
		default:
			if h.backpressure == BackpressureClose {
				// 1008 (policy violation) rather than 1011 (internal error): the connection is
				// being closed because the client did not keep up, not because the server broke,
				// and a client told 1011 would rightly treat it as a transient server fault and
				// hammer its way back in.
				go c.close(websocket.StatusPolicyViolation, "outbound buffer overflow")
			}
			return ErrBackpressure
		}
	}
}

// maxCloseReason is the largest close reason a WebSocket close frame can carry: a control
// frame's payload is capped at 125 bytes and the close code consumes the first two, per RFC
// 6455 section 5.5. coder/websocket rejects a longer reason before it writes any frame, so
// the reason must be clamped rather than passed through.
const maxCloseReason = 123

// clampCloseReason trims reason to fit a close frame's 123-byte limit, cutting on a UTF-8 rune
// boundary so the truncated reason stays valid UTF-8 (a close frame reason must be). A reason
// within the limit is returned unchanged.
func clampCloseReason(reason string) string {
	if len(reason) <= maxCloseReason {
		return reason
	}
	end := maxCloseReason
	// Back up off a byte that is a UTF-8 continuation byte (10xxxxxx) so the cut never splits
	// a multi-byte rune.
	for end > 0 && reason[end]&0xC0 == 0x80 {
		end--
	}
	return reason[:end]
}

// close shuts the socket down with a close handshake, so the peer learns why. The reason is
// clamped to what a close frame can carry, because an over-long reason makes coder/websocket
// refuse to write the frame at all, silently downgrading a clean 1008 close to an abrupt 1006.
// A handshake that still cannot be completed (the peer is gone, or the write side is jammed)
// falls back to dropping the TCP connection.
func (c *wsConn) close(code websocket.StatusCode, reason string) {
	c.closeOnce.Do(func() {
		c.closed.Store(true)
		if err := c.conn.Close(code, clampCloseReason(reason)); err != nil {
			_ = c.conn.CloseNow()
		}
	})
}

// abort drops the socket without a close handshake. It is the teardown for a connection that is
// already dead or unreachable, where waiting on a handshake would only stall.
func (c *wsConn) abort() {
	c.closeOnce.Do(func() {
		c.closed.Store(true)
		_ = c.conn.CloseNow()
	})
}

// wsConnLocks hands out one mutex per connection id, so registration transitions for the same id
// are serialized while unrelated upgrades stay concurrent.
type wsConnLocks struct {
	mu    sync.Mutex
	locks map[string]*wsConnLock
}

// wsConnLock is a reference-counted mutex for a single connection id.
type wsConnLock struct {
	mu   sync.Mutex
	refs int
}

// lock acquires the mutex for id, creating it on first use.
func (l *wsConnLocks) lock(id string) *wsConnLock {
	l.mu.Lock()
	entry, exists := l.locks[id]
	if !exists {
		entry = &wsConnLock{}
		l.locks[id] = entry
	}
	entry.refs++
	l.mu.Unlock()

	entry.mu.Lock()
	return entry
}

// unlock releases the mutex for id and forgets it once nobody is waiting on it, so the table
// does not grow with every connection that ever existed.
func (l *wsConnLocks) unlock(id string, entry *wsConnLock) {
	entry.mu.Unlock()

	l.mu.Lock()
	entry.refs--
	if entry.refs == 0 {
		delete(l.locks, id)
	}
	l.mu.Unlock()
}

// groupRateLimiters hands out one shared token bucket per group, reference counted so a group's
// bucket is created when its first connection registers and reclaimed when its last one leaves.
// Without the reclaim the table would retain a bucket for every group that ever had a connection.
type groupRateLimiters struct {
	limit rate.Limit
	burst int

	mu   sync.Mutex
	lims map[string]*groupRateLimiter
}

// groupRateLimiter is one group's token bucket and the count of connections currently sharing it.
type groupRateLimiter struct {
	limiter *rate.Limiter
	refs    int
}

// newGroupRateLimiters builds the per-group limiter table. A burst below 1 is clamped to 1 so a
// misconfigured burst limits hard rather than making the limiter reject every message.
func newGroupRateLimiters(perSecond float64, burst int) *groupRateLimiters {
	return &groupRateLimiters{
		limit: rate.Limit(perSecond),
		burst: max(burst, 1),
		lims:  make(map[string]*groupRateLimiter),
	}
}

// acquire returns the token bucket for group, creating it on first use and incrementing its
// share count. Every acquire must be paired with a release.
func (g *groupRateLimiters) acquire(group string) *rate.Limiter {
	g.mu.Lock()
	defer g.mu.Unlock()

	entry, ok := g.lims[group]
	if !ok {
		entry = &groupRateLimiter{limiter: rate.NewLimiter(g.limit, g.burst)}
		g.lims[group] = entry
	}
	entry.refs++
	return entry.limiter
}

// release drops one share of group's bucket and forgets the bucket once nobody holds it.
func (g *groupRateLimiters) release(group string) {
	g.mu.Lock()
	defer g.mu.Unlock()

	entry, ok := g.lims[group]
	if !ok {
		return
	}
	entry.refs--
	if entry.refs == 0 {
		delete(g.lims, group)
	}
}
