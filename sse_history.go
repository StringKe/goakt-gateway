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
	"container/list"
	"context"
	"errors"
	"sync"
)

// SSEEvent is one replayable event: the id that was written on the wire as the SSE "id:"
// field, and the payload that followed it.
type SSEEvent struct {
	// ID is the event id as written to the client, e.g. "conn-7-42".
	ID string

	// Payload is the raw event body, before SSE "data:" framing.
	Payload []byte
}

// SSEHistory retains recently delivered events per connection so that a client reconnecting
// with a Last-Event-ID header can be caught up on what it missed. Browsers reconnect an
// EventSource on their own and always send Last-Event-ID, so without a history every event
// written between the socket dying and the server noticing is lost with no trace.
//
// Implementations must be safe for concurrent use.
type SSEHistory interface {
	// Append records that eventID/payload was written to connID's stream. It is called from
	// the connection's writer goroutine, in wire order.
	Append(ctx context.Context, connID, eventID string, payload []byte) error

	// Since returns the events written to connID strictly after lastEventID, oldest first.
	//
	// An empty lastEventID returns everything still retained for the connection and no
	// error. An unknown lastEventID returns everything still retained together with
	// ErrHistoryGap, so the caller can replay what survives and tell the client that
	// earlier events are unrecoverable.
	Since(ctx context.Context, connID, lastEventID string) (events []SSEEvent, err error)
}

// ErrStaleGeneration is returned by GenerationalHistory.AppendGenerational when generation is
// lower than the highest generation already recorded for the connection: a write from a node
// whose connection owner lease (see WithOwnerLease in the registry) was superseded by a
// takeover elsewhere. The call is rejected outright rather than recorded, so a reconnect's
// replay can never interleave events written by two nodes that both believed they owned the
// connection at once.
var ErrStaleGeneration = errors.New("gateway: sse history append rejected: generation has been superseded by a newer owner")

// GenerationalHistory is an optional capability of an SSEHistory that fences writes by
// generation once WithOwnerLease is configured for a connection's owner lease. It exists
// alongside the plain SSEHistory contract, not in place of it: a caller that never acquired a
// lease keeps calling Append and Since exactly as before, at zero additional cost, and only a
// caller that holds a fenced generation type-asserts up to this interface to use it. This
// mirrors the WithDeliveryConfirmation opt-in precedent elsewhere in this package.
//
// MemorySSEHistory and ssehistory/redis.History both implement it. A third-party SSEHistory
// that does not is simply used unfenced, exactly as it is today.
type GenerationalHistory interface {
	SSEHistory

	// AppendGenerational is Append fenced by generation. It records the event and returns the
	// sequence number assigned to it - a per-connection counter that increases by exactly one
	// on every accepted call, with no gaps and no repeats, so a caller can detect reordering or
	// loss independently of whatever event IDs it chose - unless generation is lower than the
	// highest generation already observed for connID (by a prior AppendGenerational or
	// AdvanceGeneration call), in which case it returns ErrStaleGeneration and neither records
	// the event nor advances any state.
	AppendGenerational(ctx context.Context, connID, eventID string, payload []byte, generation uint64) (seq uint64, err error)

	// AdvanceGeneration records that connID's owner lease has moved to generation without
	// appending any event, so a stale writer using the previous generation is rejected by the
	// very first AppendGenerational call it attempts after a takeover instead of racing to
	// append one more event before the new owner does. Call it once a takeover's lease
	// acquisition succeeds, before traffic resumes on the new owner.
	//
	// It is a no-op, not an error, when generation is not strictly greater than what is already
	// recorded for connID - that means a takeover (this one or a still newer one) already
	// advanced the record past it, so there is nothing to do.
	AdvanceGeneration(ctx context.Context, connID string, generation uint64) error
}

// sseHistoryTTLRefresher is an optional capability of an SSEHistory whose retention is
// reclaimed after an idle interval rather than by count (ssehistory/redis). The SSEHandler
// calls RefreshTTL on every keepalive so a still-connected but low-traffic stream - one that
// emits no real event for longer than that interval - does not have its buffer expire under
// it and then answer a reconnect with a false gap. A backend with no time-based reclamation
// (MemorySSEHistory, whose buffers only ever leave via the per-connection cap or the
// connection LRU, never a timer) does not implement it, and the keepalive path skips it. It is
// deliberately not part of SSEHistory so the contract every implementation must satisfy stays
// the minimal Append/Since pair.
type sseHistoryTTLRefresher interface {
	// RefreshTTL re-arms the idle reclamation window for connID's buffer without recording an
	// event, signalling that the connection is still live. It is a no-op when connID has no
	// buffer (nothing has been appended yet, or it was already reclaimed).
	RefreshTTL(ctx context.Context, connID string) error
}

// defaultMemorySSEHistoryConnections bounds how many connections a MemorySSEHistory tracks
// before it starts evicting.
const defaultMemorySSEHistoryConnections = 4096

// MemorySSEHistoryOption configures a MemorySSEHistory.
type MemorySSEHistoryOption func(*MemorySSEHistory)

// WithSSEHistoryMaxConnections caps how many connections a MemorySSEHistory retains buffers
// for. When the cap is reached, the least recently touched connection is evicted whole.
// Values below 1 are ignored. Defaults to 4096.
func WithSSEHistoryMaxConnections(n int) MemorySSEHistoryOption {
	return func(h *MemorySSEHistory) {
		if n > 0 {
			h.maxConns = n
		}
	}
}

// MemorySSEHistory is an in-process SSEHistory: a ring buffer of the last perConn events per
// connection, bounded to maxConns connections.
//
// Reclamation is deliberately not tied to disconnects: replay happens after a disconnect, so
// dropping a connection's buffer when its stream ends would defeat the entire feature. What
// bounds the memory instead is (1) the per-connection cap, and (2) an LRU over connections,
// so a connection that never comes back is evicted once maxConns newer ones have gone
// through. Applications that know a session is over for good (logout, account deletion) can
// reclaim it immediately with Drop.
//
// It is a single-process buffer, so it only replays for a client that reconnects to the same
// node. Route reconnects by connection id (sticky sessions), or plug in a shared backend
// implementing SSEHistory.
type MemorySSEHistory struct {
	mu       sync.Mutex
	perConn  int
	maxConns int

	// lru is ordered most-recently-touched first; entries holds the same *list.Element by
	// connection id so a touch is O(1).
	lru     *list.List
	entries map[string]*list.Element
}

// enforce compilation error
var (
	_ SSEHistory          = (*MemorySSEHistory)(nil)
	_ GenerationalHistory = (*MemorySSEHistory)(nil)
)

// historyEntry is one connection's ring buffer. events is kept in wire order, oldest first,
// and never exceeds perConn. generation and seq back GenerationalHistory: generation is the
// highest generation ever accepted for the connection (by AppendGenerational or
// AdvanceGeneration), and seq is the last sequence number assigned. Both reset to zero when
// the entry is evicted and later recreated - matching ssehistory/redis, whose equivalent state
// does not outlive the connection key's idle TTL either - so they need no separate reclamation
// path of their own.
type historyEntry struct {
	connID     string
	events     []SSEEvent
	generation uint64
	seq        uint64
}

// NewMemorySSEHistory returns a MemorySSEHistory retaining the last perConn events of each
// connection. A perConn below 1 is raised to 1.
func NewMemorySSEHistory(perConn int, opts ...MemorySSEHistoryOption) *MemorySSEHistory {
	if perConn < 1 {
		perConn = 1
	}
	h := &MemorySSEHistory{
		perConn:  perConn,
		maxConns: defaultMemorySSEHistoryConnections,
		lru:      list.New(),
		entries:  make(map[string]*list.Element),
	}
	for _, opt := range opts {
		opt(h)
	}
	return h
}

// Append implements SSEHistory. The payload is copied: the caller owns its buffer and the
// history outlives the write.
func (h *MemorySSEHistory) Append(_ context.Context, connID, eventID string, payload []byte) error {
	stored := make([]byte, len(payload))
	copy(stored, payload)

	h.mu.Lock()
	defer h.mu.Unlock()

	entry := h.touchLocked(connID, true)
	entry.events = append(entry.events, SSEEvent{ID: eventID, Payload: stored})
	if overflow := len(entry.events) - h.perConn; overflow > 0 {
		entry.events = append(entry.events[:0], entry.events[overflow:]...)
	}
	return nil
}

// AppendGenerational implements GenerationalHistory.
func (h *MemorySSEHistory) AppendGenerational(_ context.Context, connID, eventID string, payload []byte, generation uint64) (uint64, error) {
	stored := make([]byte, len(payload))
	copy(stored, payload)

	h.mu.Lock()
	defer h.mu.Unlock()

	entry := h.touchLocked(connID, true)
	if generation < entry.generation {
		return 0, ErrStaleGeneration
	}
	entry.generation = generation
	entry.seq++
	entry.events = append(entry.events, SSEEvent{ID: eventID, Payload: stored})
	if overflow := len(entry.events) - h.perConn; overflow > 0 {
		entry.events = append(entry.events[:0], entry.events[overflow:]...)
	}
	return entry.seq, nil
}

// AdvanceGeneration implements GenerationalHistory.
func (h *MemorySSEHistory) AdvanceGeneration(_ context.Context, connID string, generation uint64) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	entry := h.touchLocked(connID, true)
	if generation > entry.generation {
		entry.generation = generation
	}
	return nil
}

// Since implements SSEHistory.
func (h *MemorySSEHistory) Since(_ context.Context, connID, lastEventID string) ([]SSEEvent, error) {
	h.mu.Lock()
	defer h.mu.Unlock()

	entry := h.touchLocked(connID, false)
	if entry == nil {
		if lastEventID == "" {
			return nil, nil
		}
		return nil, ErrHistoryGap
	}

	if lastEventID == "" {
		return cloneEvents(entry.events), nil
	}
	for i, event := range entry.events {
		if event.ID == lastEventID {
			return cloneEvents(entry.events[i+1:]), nil
		}
	}
	return cloneEvents(entry.events), ErrHistoryGap
}

// Drop discards everything retained for connID. Use it when a session is over for good and
// waiting for LRU eviction is not good enough.
func (h *MemorySSEHistory) Drop(connID string) {
	h.mu.Lock()
	defer h.mu.Unlock()

	element, ok := h.entries[connID]
	if !ok {
		return
	}
	h.lru.Remove(element)
	delete(h.entries, connID)
}

// Len reports how many connections currently have retained events.
func (h *MemorySSEHistory) Len() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.entries)
}

// touchLocked moves connID to the front of the LRU and returns its entry, creating it (and
// evicting the least recently touched connection if the cap is reached) when create is set.
// It returns nil for an unknown connection when create is not set.
func (h *MemorySSEHistory) touchLocked(connID string, create bool) *historyEntry {
	if element, ok := h.entries[connID]; ok {
		h.lru.MoveToFront(element)
		return element.Value.(*historyEntry)
	}
	if !create {
		return nil
	}
	if len(h.entries) >= h.maxConns {
		if oldest := h.lru.Back(); oldest != nil {
			h.lru.Remove(oldest)
			delete(h.entries, oldest.Value.(*historyEntry).connID)
		}
	}
	entry := &historyEntry{connID: connID}
	h.entries[connID] = h.lru.PushFront(entry)
	return entry
}

// cloneEvents returns a fully independent snapshot of events: a fresh slice plus a copy of
// every payload. The interface contract hands ownership of the returned events to the caller,
// so a caller that mutates a replayed payload must not corrupt the retained buffer or race a
// concurrent Since reading the same connection. Deep-copying here is what makes that safe;
// replay is a cold reconnect path, so the extra copy is off the delivery hot path.
func cloneEvents(events []SSEEvent) []SSEEvent {
	if len(events) == 0 {
		return nil
	}
	out := make([]SSEEvent, len(events))
	for i, event := range events {
		payload := make([]byte, len(event.Payload))
		copy(payload, event.Payload)
		out[i] = SSEEvent{ID: event.ID, Payload: payload}
	}
	return out
}
