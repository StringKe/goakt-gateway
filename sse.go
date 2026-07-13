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
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"go.uber.org/atomic"

	"github.com/tochemey/goakt/v4/log"
)

// SSEGapEventName is the SSE "event:" name used to tell a reconnecting client that the
// events between its Last-Event-ID and the oldest retained event are gone for good. Its data
// is the Last-Event-ID that could not be resolved. It is a named event, so a client that does
// not listen for it is unaffected (an EventSource's onmessage only sees unnamed events),
// while a client that cares can resynchronize through its regular API instead of silently
// running on an incomplete stream.
const SSEGapEventName = "gateway-gap"

const (
	// defaultSSESendBuffer is the per-connection outbound queue depth, matching the WebSocket
	// path's default.
	defaultSSESendBuffer = 256

	// defaultSSEWriteTimeout bounds a single write+flush to the client, so one stalled reader
	// cannot pin the streaming loop and block Drain indefinitely. It mirrors the WebSocket
	// path's default per-write timeout.
	defaultSSEWriteTimeout = 10 * time.Second
)

// SSEAuthFunc authenticates an SSE request and resolves the identity of the connection it
// will open. A non-nil error rejects the request with HTTP 403 Forbidden.
//
// The returned ConnInfo is the single source of truth for the connection: its ID, Group,
// Topics and Meta are used for registration and handed to every subsequent callback, so the
// auth token is parsed once instead of once per concern.
type SSEAuthFunc func(*http.Request) (*ConnInfo, error)

// SSEHandlerOption configures an http.Handler created with NewSSEHandler.
type SSEHandlerOption func(*SSEHandler)

// WithSSEAuth sets the auth hook run before a connection is accepted and registered. A
// non-nil error rejects the request with HTTP 403 Forbidden.
func WithSSEAuth(f SSEAuthFunc) SSEHandlerOption {
	return func(h *SSEHandler) { h.auth = f }
}

// WithSSEIDFunc sets the function used to derive a connection's id from the request. It is a
// fallback, consulted only when SSEAuthFunc did not return an ID. Without either, a random
// UUID is generated per connection, which also means Last-Event-ID replay cannot work:
// history is keyed by connection id, so a client that wants to be caught up after a
// reconnect must come back under the same id.
//
// Security: SSEHandler always registers with takeover enabled, so the connection id is a
// takeover key. A client that controls the id it registers under can evict any live stream
// holding that id and rebind it, redirecting every later SendToConnection and SendToGroup
// delivery for that id to itself. Derive the id from an authenticated principal (do that in
// SSEAuthFunc, which runs first), never from an unauthenticated request field.
func WithSSEIDFunc(f func(*http.Request) string) SSEHandlerOption {
	return func(h *SSEHandler) { h.idFunc = f }
}

// WithSSETopics sets the function used to derive the topics a connection is joined to at
// registration time. It is a fallback, consulted only when SSEAuthFunc returned no Topics.
func WithSSETopics(f func(*http.Request) []string) SSEHandlerOption {
	return func(h *SSEHandler) { h.topicsFunc = f }
}

// WithSSERetry sets the reconnection delay advertised to the client as an SSE "retry:" field
// when the stream opens. Defaults to 3 seconds; 0 omits the field and leaves the browser's
// own default (typically 3 seconds) in place.
func WithSSERetry(d time.Duration) SSEHandlerOption {
	return func(h *SSEHandler) { h.retry = d }
}

// WithSSEEventName sets the function that names each outgoing event, written as the SSE
// "event:" field. Returning an empty name emits an anonymous event, which is what an
// EventSource's onmessage handler receives; a named event only reaches listeners registered
// for that name. Defaults to anonymous events.
func WithSSEEventName(f func(payload []byte) string) SSEHandlerOption {
	return func(h *SSEHandler) { h.eventName = f }
}

// WithSSEHistory enables Last-Event-ID replay through h. Every event written to a stream is
// appended to the history under its connection id; when a client reconnects with the same id
// and a Last-Event-ID header, the events after it are replayed before the live stream
// resumes. If the requested id is no longer retained, whatever is left is replayed after an
// SSEGapEventName event, so the loss is visible rather than silent.
//
// Without a history, event ids are still emitted but restart at 1 on every connection and no
// replay is possible. A backend implementing SharedSSEHistory is accepted only with a Registry
// configured by WithOwnerLease and a backend implementing GenerationalHistory. This prevents a
// stale replica from writing into a new owner's shared replay stream after takeover.
func WithSSEHistory(h SSEHistory) SSEHandlerOption {
	return func(handler *SSEHandler) { handler.history = h }
}

// WithSSEBackpressurePolicy decides what happens when a connection's outbound buffer is full.
// Defaults to BackpressureDrop.
func WithSSEBackpressurePolicy(p BackpressurePolicy) SSEHandlerOption {
	return func(h *SSEHandler) { h.backpressure = p }
}

// WithSSEOnConnect sets the callback invoked once a connection has been registered and the
// response stream has been opened.
func WithSSEOnConnect(f func(ctx context.Context, info *ConnInfo, r *http.Request)) SSEHandlerOption {
	return func(h *SSEHandler) { h.onConnect = f }
}

// WithSSEOnDisconnect sets the callback invoked once a connection's stream has ended.
func WithSSEOnDisconnect(f func(info *ConnInfo)) SSEHandlerOption {
	return func(h *SSEHandler) { h.onDisconnect = f }
}

// WithSSESendBuffer sets the size of each connection's outbound buffer. When full, the
// configured BackpressurePolicy decides between dropping the message (the sender sees
// ErrBackpressure) and closing the stream. A value at or below zero is raised to the default.
// Defaults to 256.
func WithSSESendBuffer(size int) SSEHandlerOption {
	return func(h *SSEHandler) { h.bufferSize = size }
}

// WithSSEWriteTimeout bounds a single write+flush to the client. A slow or dead reader that
// stalls a write is treated as a connection failure and torn down, rather than blocking the
// streaming loop and Drain forever. A value at or below zero is raised to the default.
// Defaults to 10 seconds.
func WithSSEWriteTimeout(d time.Duration) SSEHandlerOption {
	return func(h *SSEHandler) { h.writeTimeout = d }
}

// WithSSEKeepAlive sets the interval at which a comment-only keepalive event is sent to
// detect dead connections and prevent idle-timing-out intermediate proxies. Defaults to
// 15 seconds; 0 disables it.
func WithSSEKeepAlive(d time.Duration) SSEHandlerOption {
	return func(h *SSEHandler) { h.keepAlive = d }
}

// WithSSELogger sets the logger used to report connection-handling errors. Defaults to
// log.DiscardLogger.
func WithSSELogger(logger log.Logger) SSEHandlerOption {
	return func(h *SSEHandler) { h.logger = logger }
}

// WithSSEReauth periodically re-runs f against the original request to re-check a stream whose
// authorization can be revoked while it is open (a role change, a logout, an expiring token).
// The first failure ends the stream: a terminating comment naming the reason is written and the
// connection is unregistered. Disabled by default; a non-positive interval or a nil f leaves it
// disabled.
//
// f is the same shape as the handshake SSEAuthFunc but is consulted only for its error: the
// re-resolved identity is not applied to the live stream, because changing a stream's id or
// group mid-flight would strand deliveries already addressed to it. Only header and cookie based
// auth can be re-checked this way, since that is all the request carries.
func WithSSEReauth(interval time.Duration, f SSEAuthFunc) SSEHandlerOption {
	return func(h *SSEHandler) {
		h.reauthInterval = interval
		h.reauth = f
	}
}

// SSEHandler opens a Server-Sent Events stream for every incoming request and registers each
// one in its Registry for the lifetime of the connection. SSE is one-way (server to client);
// inbound application data, if any, belongs in an ordinary HTTP endpoint the client posts to
// separately.
//
// Every event carries an id of the form "<connID>-<seq>" with seq counting from 1 within the
// connection. The id is what a browser echoes back in Last-Event-ID after an automatic
// reconnect, and it is self-describing on purpose: an id from another connection is
// recognizable as such rather than being mistaken for a position in this connection's stream.
// With a SSEHistory configured, seq resumes past the replayed events instead of restarting,
// so ids stay unique for as long as the history retains them.
//
// A reconnecting client that reuses its connection id takes over from the previous stream:
// the old registration is replaced and the old stream is terminated. A dead SSE stream is
// invisible to the server until a write to it fails, which behind a half-open TCP connection
// can take minutes, and refusing the client for that long is worse than dropping a stream
// nobody is reading.
type SSEHandler struct {
	registry       *Registry
	auth           SSEAuthFunc
	idFunc         func(*http.Request) string
	topicsFunc     func(*http.Request) []string
	eventName      func(payload []byte) string
	history        SSEHistory
	configErr      error
	retry          time.Duration
	backpressure   BackpressurePolicy
	onConnect      func(ctx context.Context, info *ConnInfo, r *http.Request)
	onDisconnect   func(info *ConnInfo)
	bufferSize     int
	writeTimeout   time.Duration
	keepAlive      time.Duration
	reauth         SSEAuthFunc
	reauthInterval time.Duration
	logger         log.Logger

	// shutdown unblocks every streaming loop on Drain. SSE handlers are ordinary
	// (non-hijacked) requests, so without this http.Server.Shutdown would wait on them
	// until its context expired.
	shutdown  chan struct{}
	drainOnce sync.Once

	// mu guards sessions, draining and the teardownWG bookkeeping below. The Registry evicts a
	// replaced connection from its table but cannot close the socket behind it, so the handler
	// has to keep the close handles itself.
	mu       sync.Mutex
	sessions map[string]*sseSession

	// draining is set under mu, in the same critical section that admits a session into
	// sessions (see open), so Drain can never race a registration that has not yet decided
	// whether to proceed: either open's critical section runs first and the session is counted
	// in teardownWG before Drain can observe it missing, or Drain's runs first and the
	// registration is rejected outright. Mirrors WSHandler's connMu-guarded draining field.
	draining bool

	// teardownWG counts every session from the moment open() admits it until its close() -
	// including the Registry unregister - has finished, so Drain can block until every
	// terminated stream has actually freed its registration rather than just ended its HTTP
	// response. See WSHandler.teardownWG, which exists for the identical reason.
	teardownWG sync.WaitGroup

	// idLocks serializes setup and teardown of the same connection id. Without it a stream
	// ending concurrently with its own replacement could Unregister the replacement it had
	// already lost the table entry to.
	idLocks keyedMutex
}

// sseSession is one live stream. done is closed to end it, either because a newer connection
// with the same id took over or because the outbound buffer overflowed under
// BackpressureClose. finished is closed once its writer goroutine has left the loop, which is
// what tells a replacement that no further events will be appended to the history behind its
// back.
type sseSession struct {
	done      chan struct{}
	finished  chan struct{}
	closeOnce sync.Once
	finishOne sync.Once

	// reason, set before done is closed, is the disconnect reason a Registry.Disconnect or a
	// failed reauthentication attaches so the writer loop can emit a terminating comment naming
	// it. A takeover or backpressure close leaves it unset and simply ends the stream.
	reasonMu sync.Mutex
	reason   string
	reasonOK bool

	// handle is the entry-guarded ConnHandle open() obtained for this session, used to tear the
	// registration down in close() instead of an id-scoped Unregister (see close's doc comment).
	// Set once, before the session becomes reachable outside open(), and read only afterward.
	handle *ConnHandle

	// generation is this session's owner lease generation (see WithOwnerLease), copied off the
	// ConnHandle's entry at open() time. It is 0 whenever no lease is configured, which
	// appendHistory treats as "no fencing to do" - the same zero-cost default every other
	// owner-lease fencing check in this package shares.
	generation uint64
}

func (s *sseSession) stop() {
	s.closeOnce.Do(func() { close(s.done) })
}

func (s *sseSession) finish() {
	s.finishOne.Do(func() { close(s.finished) })
}

// disconnect records reason and ends the stream. It is the close hook handed to the Registry,
// so it must be safe to call from any goroutine and idempotent; the first reason wins and stop
// is guarded by its own Once.
func (s *sseSession) disconnect(reason string) {
	s.reasonMu.Lock()
	if !s.reasonOK {
		s.reason = reason
		s.reasonOK = true
	}
	s.reasonMu.Unlock()
	s.stop()
}

// disconnectReason reports the reason attached by disconnect, if any.
func (s *sseSession) disconnectReason() (string, bool) {
	s.reasonMu.Lock()
	defer s.reasonMu.Unlock()
	return s.reason, s.reasonOK
}

// enforce compilation error
var _ http.Handler = (*SSEHandler)(nil)

// NewSSEHandler returns an SSEHandler bound to registry. The returned handler is an
// http.Handler; wire its Drain method into Server via WithDrainOnShutdown (or call it from
// your own shutdown path) so open streams terminate promptly on shutdown.
func NewSSEHandler(registry *Registry, opts ...SSEHandlerOption) *SSEHandler {
	h := &SSEHandler{
		registry:     registry,
		retry:        3 * time.Second,
		bufferSize:   defaultSSESendBuffer,
		writeTimeout: defaultSSEWriteTimeout,
		keepAlive:    15 * time.Second,
		logger:       log.DiscardLogger,
		shutdown:     make(chan struct{}),
		sessions:     make(map[string]*sseSession),
	}
	for _, opt := range opts {
		opt(h)
	}
	if shared, ok := h.history.(SharedSSEHistory); ok && shared != nil {
		if h.registry == nil || h.registry.lease == nil {
			h.configErr = ErrSSESharedHistoryRequiresOwnerLease
		} else if _, ok := h.history.(GenerationalHistory); !ok {
			h.configErr = ErrSSESharedHistoryRequiresGenerationalHistory
		}
	}
	// Clamp after options so a make(chan []byte, size) below cannot panic on a negative buffer
	// and a non-positive write timeout cannot disable the deadline that bounds a stalled write.
	if h.bufferSize <= 0 {
		h.bufferSize = defaultSSESendBuffer
	}
	if h.writeTimeout <= 0 {
		h.writeTimeout = defaultSSEWriteTimeout
	}
	return h
}

// Drain terminates every open SSE stream and makes new requests fail fast with 503, so a
// graceful server shutdown is not held hostage by long-lived streams. It blocks until every
// stream that was open when Drain was called - and every registration still being admitted at
// that instant - has actually unregistered from the Registry: ending the HTTP response alone
// would free the socket but not the connection's cluster-wide actor name or (with
// WithOwnerLease) its owner lease, leaving them to a caller that kills the process right after
// Drain returns. Safe to call more than once.
func (h *SSEHandler) Drain() {
	h.drainOnce.Do(func() {
		h.mu.Lock()
		h.draining = true
		h.mu.Unlock()
		close(h.shutdown)
	})
	h.teardownWG.Wait()
}

// ServeHTTP implements http.Handler.
func (h *SSEHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if h.configErr != nil {
		http.Error(w, h.configErr.Error(), http.StatusInternalServerError)
		return
	}
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
	rc := http.NewResponseController(w)

	info, err := h.resolveConnInfo(r)
	if err != nil {
		http.Error(w, ErrUnauthorized.Error(), http.StatusForbidden)
		return
	}

	ctx := r.Context()
	session := &sseSession{done: make(chan struct{}), finished: make(chan struct{})}

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
			if h.backpressure == BackpressureClose {
				session.stop()
				return ErrConnectionClosed
			}
			return ErrBackpressure
		}
	}

	if err := h.open(ctx, info, session, send); err != nil {
		h.logger.Warnf("gateway: failed to register SSE connection %q: %v", info.ID, err)
		http.Error(w, "registration failed", http.StatusInternalServerError)
		return
	}
	defer func() {
		closed.Store(true)
		h.close(ctx, info, session)
	}()
	// Runs before the deferred close above (LIFO), and therefore before the id's lock is
	// contended: a replacement waiting on this stream must be released as soon as its writer
	// loop is out, not after the whole teardown.
	defer session.finish()

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.WriteHeader(http.StatusOK)
	if h.retry > 0 {
		h.armWriteDeadline(rc)
		if _, err := io.WriteString(w, "retry: "+strconv.FormatInt(h.retry.Milliseconds(), 10)+"\n\n"); err != nil {
			return
		}
	}
	flusher.Flush()

	if h.onConnect != nil {
		h.onConnect(ctx, info, r)
	}

	if h.reauth != nil && h.reauthInterval > 0 {
		go h.reauthLoop(ctx, r, info, session)
	}

	// The replayed events are written before the loop starts, so anything delivered to this
	// connection since it registered is queued behind them in outbound and stays in order.
	seq, err := h.replay(ctx, w, rc, info.ID, r.Header.Get("Last-Event-ID"))
	if err != nil {
		return
	}
	flusher.Flush()

	var keepAlive <-chan time.Time
	if h.keepAlive > 0 {
		ticker := time.NewTicker(h.keepAlive)
		defer ticker.Stop()
		keepAlive = ticker.C
	}

	for {
		select {
		case <-ctx.Done():
			return
		case <-h.shutdown:
			return
		case <-session.done:
			if reason, ok := session.disconnectReason(); ok {
				// Best-effort terminating comment: the stream is ending regardless, but a comment
				// naming the reason is more useful to the client than a silent EOF. The reason is
				// sanitized so it cannot inject additional SSE lines.
				h.armWriteDeadline(rc)
				_, _ = io.WriteString(w, ": disconnect "+sanitizeSSEField(reason)+"\n\n")
				flusher.Flush()
			}
			return
		case <-keepAlive:
			// A live but idle stream emits keepalives and no real events, so a history whose
			// retention is time-based (ssehistory/redis) would otherwise reclaim its buffer
			// mid-connection and answer the next reconnect with a false gap. Re-arm it here so
			// the buffer lives exactly as long as the connection does, matching MemorySSEHistory.
			if refresher, ok := h.history.(sseHistoryTTLRefresher); ok {
				if err := refresher.RefreshTTL(ctx, info.ID); err != nil {
					h.logger.Warnf("gateway: failed to refresh SSE history retention for connection %q: %v", info.ID, err)
				}
			}
			h.armWriteDeadline(rc)
			if _, err := io.WriteString(w, ": keepalive\n\n"); err != nil {
				return
			}
			flusher.Flush()
		case payload := <-outbound:
			seq++
			eventID := formatSSEEventID(info.ID, seq)
			if h.history != nil {
				if err := h.appendHistory(ctx, info.ID, eventID, payload, session.generation); err != nil {
					if errors.Is(err, ErrStaleGeneration) {
						// The owner lease this stream was writing under (see WithOwnerLease) has
						// been superseded by a cross-node takeover. Writing this event anyway
						// could interleave it into shared history a new owner's stream is about
						// to replay from, out of order or after events it never wrote, so the
						// stream ends here instead of emitting it.
						h.logger.Warnf("gateway: sse connection %q stopped: %v", info.ID, err)
						return
					}
					h.logger.Warnf("gateway: failed to record SSE event %q for replay: %v", eventID, err)
				}
			}
			h.armWriteDeadline(rc)
			if err := writeSSEEvent(w, eventID, h.nameOf(payload), payload); err != nil {
				return
			}
			flusher.Flush()
		}
	}
}

// armWriteDeadline sets the deadline for the next write+flush so a stalled reader cannot pin
// the streaming loop and block Drain. A ResponseWriter that does not support deadlines reports
// ErrNotSupported, which is harmless: the write then simply has no deadline, as before.
func (h *SSEHandler) armWriteDeadline(rc *http.ResponseController) {
	_ = rc.SetWriteDeadline(time.Now().Add(h.writeTimeout))
}

// resolveConnInfo authenticates the request and fills in the identity of the connection it
// opens, falling back to the id/topics hooks for whatever the auth hook left empty.
func (h *SSEHandler) resolveConnInfo(r *http.Request) (*ConnInfo, error) {
	info := &ConnInfo{}
	if h.auth != nil {
		authenticated, err := h.auth(r)
		if err != nil {
			return nil, err
		}
		if authenticated != nil {
			info = authenticated
		}
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
	return info, nil
}

// open publishes the session in the handler's table, terminates any stream still holding the
// same id, and registers the connection. Both halves run under the id's lock so a concurrent
// teardown of the replaced stream cannot unregister this one.
func (h *SSEHandler) open(ctx context.Context, info *ConnInfo, session *sseSession, send func([]byte) error) error {
	h.idLocks.Lock(info.ID)
	defer h.idLocks.Unlock(info.ID)

	h.mu.Lock()
	if h.draining {
		h.mu.Unlock()
		return ErrConnectionClosed
	}
	previous := h.sessions[info.ID]
	h.sessions[info.ID] = session
	// Counted in the same critical section as the draining check and the table insert above;
	// see teardownWG's doc comment for why that ordering is what makes Drain's Wait race-free.
	h.teardownWG.Add(1)
	h.mu.Unlock()

	if previous != nil {
		previous.stop()
		h.awaitFinished(ctx, info.ID, previous)
	}

	handle, err := h.registry.RegisterHandle(ctx, info.ID, send,
		WithConnGroup(info.Group),
		WithConnTopics(info.Topics...),
		WithConnMeta(info.Meta),
		WithReplaceExisting(),
		// The close hook is how Registry.Disconnect and DisconnectGroup reach this stream: the
		// Registry holds only the send function, so it drives teardown through session.disconnect,
		// which the streaming loop turns into a terminating comment and a return.
		WithConnCloseHook(session.disconnect),
	)
	if err != nil {
		h.mu.Lock()
		if h.sessions[info.ID] == session {
			delete(h.sessions, info.ID)
		}
		h.mu.Unlock()
		// close() (and its Done) is only ever reached via the defer ServeHTTP registers after a
		// successful open; this failure path is the only Done that pays off the Add above.
		h.teardownWG.Done()
		return err
	}
	session.handle = handle
	session.generation = handle.entry.generation.Load()
	// Raise h.history's generation-fencing floor for this connection before any event of this
	// session's own is appended (see GenerationalHistory.AdvanceGeneration): without this, the
	// floor is only ever raised by this session's own first AppendGenerational call, leaving an
	// unbounded window in which a still-draining previous owner's queued writes - already
	// in-flight before its takeover eviction landed, so connActor.Receive's own staleOwner check
	// never sees them - are appended at the old, lower generation because nothing has told the
	// shared history a takeover happened yet. A generation of 0 (no lease configured) and a
	// history that does not implement the optional capability both make this a no-op.
	if session.generation != 0 && h.history != nil {
		if generational, ok := h.history.(GenerationalHistory); ok {
			if err := generational.AdvanceGeneration(ctx, info.ID, session.generation); err != nil {
				// The generation floor is the fence that makes the registration safe to expose.
				// Removing this exact entry also releases its actor and lease before open returns.
				if unregisterErr := handle.UnregisterHandle(context.WithoutCancel(ctx)); unregisterErr != nil {
					h.logger.Warnf("gateway: failed to roll back SSE connection %q after history generation advance failed: %v", info.ID, unregisterErr)
				}
				h.mu.Lock()
				if h.sessions[info.ID] == session {
					delete(h.sessions, info.ID)
				}
				h.mu.Unlock()
				// open failed before ServeHTTP installed its deferred close, so this is the one
				// matching Done for the Add performed when the session was admitted above.
				h.teardownWG.Done()
				return err
			}
		}
	}
	return nil
}

// takeoverGrace bounds how long a new stream waits for the one it is replacing to leave its
// writer loop. The old stream is sitting in a select and normally exits at once, but it can
// also be stuck in a write to a socket whose peer is gone and whose window never opens again,
// and making the client wait on that indefinitely would be worse than the narrow risk of
// missing an event the old stream appends to the history on its way out.
const takeoverGrace = 2 * time.Second

// awaitFinished blocks until the replaced stream has stopped writing (and therefore stopped
// appending to the history), so the replay this connection is about to read is complete.
func (h *SSEHandler) awaitFinished(ctx context.Context, id string, previous *sseSession) {
	timer := time.NewTimer(takeoverGrace)
	defer timer.Stop()

	select {
	case <-previous.finished:
	case <-ctx.Done():
	case <-timer.C:
		h.logger.Warnf("gateway: replaced SSE stream for connection %q did not stop within %s", id, takeoverGrace)
	}
}

// close unregisters the connection, unless a newer stream already took the id over: in that
// case the newer stream owns the registration and this one must leave it alone. onDisconnect
// fires either way, because this stream did end.
//
// Teardown goes through session's entry-guarded ConnHandle rather than an id-scoped Unregister:
// an id-scoped call resolves whatever is currently registered under the id at the moment it
// runs, so a session this handler still (locally) believes it owns, but whose Registry entry a
// same-id takeover already replaced from elsewhere (this handler's own h.sessions table is
// process-local and cannot observe a takeover that landed on another node), could otherwise
// delete the newer owner's registration out from under it. UnregisterHandle only ever tears
// down the exact entry open() registered.
func (h *SSEHandler) close(ctx context.Context, info *ConnInfo, session *sseSession) {
	defer h.teardownWG.Done()

	h.idLocks.Lock(info.ID)

	h.mu.Lock()
	current := h.sessions[info.ID] == session
	if current {
		delete(h.sessions, info.ID)
	}
	h.mu.Unlock()

	if current {
		if err := session.handle.UnregisterHandle(context.WithoutCancel(ctx)); err != nil {
			h.logger.Warnf("gateway: failed to unregister SSE connection %q: %v", info.ID, err)
		}
	}
	h.idLocks.Unlock(info.ID)

	if h.onDisconnect != nil {
		h.onDisconnect(info)
	}
}

// reauthLoop periodically re-checks a stream's authorization against the original request and
// ends the stream on the first failure. It stops as soon as the stream ends for any other
// reason: ctx is the request context (cancelled when the client goes away), session.done fires
// on takeover or a completed teardown, and shutdown fires on Drain.
func (h *SSEHandler) reauthLoop(ctx context.Context, r *http.Request, info *ConnInfo, session *sseSession) {
	ticker := time.NewTicker(h.reauthInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-h.shutdown:
			return
		case <-session.done:
			return
		case <-ticker.C:
			if _, err := h.reauth(r); err != nil {
				h.logger.Warnf("gateway: SSE connection %q failed reauthentication: %v", info.ID, err)
				session.disconnect("reauthentication failed")
				return
			}
		}
	}
}

// replay writes the events the client missed and returns the sequence number the live stream
// must continue from. A client that never saw an event (no Last-Event-ID) gets no replay, but
// the sequence still resumes past whatever the history holds so that a reused connection id
// cannot mint an id twice.
func (h *SSEHandler) replay(ctx context.Context, w io.Writer, rc *http.ResponseController, connID, lastEventID string) (uint64, error) {
	if h.history == nil {
		return 0, nil
	}

	events, err := h.history.Since(ctx, connID, lastEventID)
	gap := errors.Is(err, ErrHistoryGap)
	if err != nil && !gap {
		// A broken history must not cost the client its stream: it goes live without replay,
		// which is exactly what it would get with no history configured at all.
		h.logger.Warnf("gateway: failed to read SSE history for connection %q: %v", connID, err)
		if seq, ok := parseSSEEventSeq(connID, lastEventID); ok {
			return seq, nil
		}
		return 0, nil
	}

	seq := uint64(0)
	if len(events) > 0 {
		if last, ok := parseSSEEventSeq(connID, events[len(events)-1].ID); ok {
			seq = last
		}
	}
	if seq == 0 {
		if parsed, ok := parseSSEEventSeq(connID, lastEventID); ok {
			seq = parsed
		}
	}

	if lastEventID == "" {
		// The client is starting fresh and asked for nothing; only the resume point matters.
		return seq, nil
	}

	if gap {
		h.logger.Warnf("gateway: SSE connection %q asked to resume from %q, which is no longer retained", connID, lastEventID)
		h.armWriteDeadline(rc)
		if err := writeSSEEvent(w, "", SSEGapEventName, []byte(lastEventID)); err != nil {
			return seq, err
		}
	}

	for _, event := range events {
		h.armWriteDeadline(rc)
		if err := writeSSEEvent(w, event.ID, h.nameOf(event.Payload), event.Payload); err != nil {
			return seq, err
		}
	}
	return seq, nil
}

// appendHistory records eventID/payload for connID in h.history, fencing the write against
// generation when both a lease is configured (generation != 0) and h.history implements
// GenerationalHistory (see sse_history.go): a write from a connection whose generation has
// since been superseded by a cross-node takeover is then rejected with ErrStaleGeneration
// rather than recorded, closing the gap where a dispossessed old owner's event could still
// land in shared history a new owner's stream is about to replay from, or interleave into it
// out of order. Called only when h.history is non-nil.
//
// A history backend that does not implement GenerationalHistory does not participate in
// generation fencing: appendHistory falls back to plain Append, unchanged from before
// WithOwnerLease existed - the same zero-cost-when-unconfigured default every owner-lease
// fencing check in this package shares. This mirrors sseHistoryTTLRefresher's
// optional-capability pattern (see sse_history.go).
func (h *SSEHandler) appendHistory(ctx context.Context, connID, eventID string, payload []byte, generation uint64) error {
	if generation == 0 {
		return h.history.Append(ctx, connID, eventID, payload)
	}
	generational, ok := h.history.(GenerationalHistory)
	if !ok {
		return h.history.Append(ctx, connID, eventID, payload)
	}
	_, err := generational.AppendGenerational(ctx, connID, eventID, payload, generation)
	return err
}

// nameOf returns the SSE event name for payload, or "" for an anonymous event.
func (h *SSEHandler) nameOf(payload []byte) string {
	if h.eventName == nil {
		return ""
	}
	return h.eventName(payload)
}

// formatSSEEventID builds the wire id of the seq-th event of connID.
func formatSSEEventID(connID string, seq uint64) string {
	return connID + "-" + strconv.FormatUint(seq, 10)
}

// parseSSEEventSeq extracts the sequence number an event id encodes, reporting false when the
// id does not belong to connID (a stale id from another connection, or a client-invented one).
func parseSSEEventSeq(connID, eventID string) (uint64, bool) {
	suffix, ok := strings.CutPrefix(eventID, connID+"-")
	if !ok {
		return 0, false
	}
	seq, err := strconv.ParseUint(suffix, 10, 64)
	if err != nil {
		return 0, false
	}
	return seq, true
}

// writeSSEEvent writes one SSE event to w: an optional "id:" field, an optional "event:"
// name, and the payload split into one "data:" line per newline, per the SSE framing rules
// (https://html.spec.whatwg.org/multipage/server-sent-events.html). The frame is assembled in
// full before it is written, so a failing write cannot leave a half-event on the wire.
func writeSSEEvent(w io.Writer, id, event string, payload []byte) error {
	var frame bytes.Buffer
	if id != "" {
		frame.WriteString("id: ")
		frame.WriteString(sanitizeSSEField(id))
		frame.WriteByte('\n')
	}
	if event != "" {
		frame.WriteString("event: ")
		frame.WriteString(sanitizeSSEField(event))
		frame.WriteByte('\n')
	}
	for line := range bytes.SplitSeq(payload, []byte("\n")) {
		frame.WriteString("data: ")
		frame.Write(line)
		frame.WriteByte('\n')
	}
	frame.WriteByte('\n')

	_, err := w.Write(frame.Bytes())
	return err
}

// sanitizeSSEField strips the line terminators an id or event name must not contain: they
// would end the field early and let application data forge SSE frames.
func sanitizeSSEField(value string) string {
	return strings.NewReplacer("\r", "", "\n", "").Replace(value)
}

// keyedMutex is a mutex per key, allocated on demand and released once nobody holds or waits
// on it.
type keyedMutex struct {
	mu    sync.Mutex
	locks map[string]*keyedLock
}

type keyedLock struct {
	mu   sync.Mutex
	refs int
}

// Lock acquires the mutex for key.
func (k *keyedMutex) Lock(key string) {
	k.mu.Lock()
	if k.locks == nil {
		k.locks = make(map[string]*keyedLock)
	}
	lock, ok := k.locks[key]
	if !ok {
		lock = &keyedLock{}
		k.locks[key] = lock
	}
	lock.refs++
	k.mu.Unlock()

	lock.mu.Lock()
}

// Unlock releases the mutex for key. It panics if key is not locked.
func (k *keyedMutex) Unlock(key string) {
	k.mu.Lock()
	lock, ok := k.locks[key]
	if !ok {
		k.mu.Unlock()
		panic("gateway: keyedMutex.Unlock of an unlocked key " + key)
	}
	lock.refs--
	if lock.refs == 0 {
		delete(k.locks, key)
	}
	k.mu.Unlock()

	lock.mu.Unlock()
}
