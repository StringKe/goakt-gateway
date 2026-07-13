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

// Package redis provides a Redis- or Valkey-backed gateway.SSEHistory, so a client that
// reconnects its EventSource to any node in a deployment can be replayed from Last-Event-ID,
// not only to the same node it originally landed on. gateway.MemorySSEHistory is a
// single-process buffer and can only replay a reconnect that sticky-routes back to the same
// process; this shared backend removes that constraint. It is a separate package specifically
// so that importing the root gateway package never pulls in github.com/redis/go-redis/v9 for
// applications that do not want it.
//
// go-redis speaks the same RESP protocol to Redis and to Valkey (a BSD-licensed fork of
// Redis 7.2.4), so the constructor takes a goredis.UniversalClient pointed at either one and
// the code below carries no Redis-versus-Valkey branch. Only commands present and identical on
// Redis 7.2 and Valkey 8 are used: RPUSH, LTRIM, LINDEX, PEXPIRE, LRANGE, and EVAL (Lua 5.1,
// including its bundled cjson library, present in both Redis 7.2's and Valkey 8's script
// environment).
//
// # Data model
//
// One Redis LIST per connection id holds that connection's most recent events, oldest at
// the head. Each element is the JSON encoding of {id, payload}; the payload is a Go []byte,
// which encoding/json emits as base64, so an event body of arbitrary bytes (including
// newlines and NULs) round-trips without any delimiter framing that a raw payload could
// collide with.
//
// A LIST (not a sorted set or stream) is the right shape because the access pattern is
// exactly append-to-tail and read-in-order: RPUSH is O(1), LTRIM keeps only the newest
// perConn elements, and LRANGE returns them already in wire order.
//
// # Atomicity
//
// Append runs RPUSH + LTRIM + PEXPIRE as one Lua script (a single round trip executed
// atomically by Redis). Doing the three as separate commands would leave observable
// intermediate states - an appended element the trim had not yet bounded, or a grown list
// with no refreshed TTL - and a crash between them would leak an un-expiring key. As one
// script the connection's buffer is only ever seen bounded and armed with a TTL.
//
// # Generation fencing
//
// History also implements gateway.GenerationalHistory. AppendGenerational and
// AdvanceGeneration need a per-connection "highest generation observed" fact available to the
// very next write, atomically with that write, but adding a second Redis key to hold it would
// break the single-key-per-connection design this package advertises (a Redis Cluster would
// then need a hash tag to keep the two keys co-located, and there would be two round trips to
// keep consistent instead of one). Instead the fact rides along inside the connection's
// existing LIST: every element AppendGenerational or AdvanceGeneration writes is stamped with
// the generation and sequence it was accepted at, and appendGenerationalScript/
// advanceGenerationScript both derive "highest observed" by LINDEX-ing the list's own tail
// (the newest, and therefore highest-generation, element - the scripts never accept a write
// that would make the list stop being non-decreasing in generation) rather than consulting any
// separate piece of state.
//
// AdvanceGeneration has no event of its own to attach the generation to, so it stores it in a
// reserved marker element carrying generationMarkerEventID - the same reserved-control-byte
// convention the root gateway package uses for evictReasonPrefix and groupTopicPrefix, chosen
// so it can never collide with a real id (every real id this package is handed comes from
// gateway's formatSSEEventID, which never emits a leading NUL). It is stamped with the tail's
// sequence unchanged, not the next one, since it carries no real event and must not create a
// gap in the counter AppendGenerational hands out. Since filters markers out of everything it
// returns, so they are invisible to every caller of the plain SSEHistory contract and only ever
// observed, indirectly, by a later AppendGenerational/AdvanceGeneration call reading the tail.
//
// Plain (non-generational) Append never reads or writes generation/sequence at all; an element
// it stores simply has no "g"/"s" fields, and a generational call that later encounters one as
// the tail treats the missing fields as generation 0, sequence 0 - the correct behavior for a
// connection that has never had a lease, and consistent with generation 0 being what a caller
// without a configured owner lease always passes.
//
// # Reclamation
//
// Reclamation is deliberately not tied to disconnects, exactly as in gateway.MemorySSEHistory:
// replay happens after a disconnect, so dropping a connection's buffer when its stream ends
// would defeat the feature. Instead each connection key carries an idle TTL (see WithTTL) that
// both Append and RefreshTTL re-arm, so a connection that never comes back is reclaimed by
// Redis once the TTL elapses with no further activity. The gateway SSEHandler calls RefreshTTL
// on every keepalive, so a still-connected stream that simply produces no events for longer
// than the TTL keeps its buffer for as long as it stays up - matching MemorySSEHistory, whose
// buffers are never reclaimed on a timer - rather than expiring mid-connection and answering
// the next reconnect with a false gap. A stream that has truly gone away sends no more
// keepalives and no more Appends, so its buffer still ages out on schedule. Since is a pure
// read and does not re-arm the TTL.
package redis

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	goredis "github.com/redis/go-redis/v9"

	gateway "github.com/StringKe/goakt-gateway"
)

// DefaultKeyPrefix namespaces SSE history keys away from every other kind of key a gateway
// deployment may keep in the same Redis database (notably the coordinator's and presence's).
const DefaultKeyPrefix = "gateway:ssehistory:"

// DefaultPerConn is the number of most-recent events retained per connection when WithPerConn
// is not supplied. It mirrors gateway.NewMemorySSEHistory's typical sizing: enough to cover a
// browser's reconnect window without unbounded growth.
const DefaultPerConn = 64

// DefaultTTL is the idle reclaim window applied to each connection key when WithTTL is not
// supplied. A connection whose node dies is reclaimed by Redis one hour after its last write,
// which is far longer than any realistic EventSource reconnect gap yet still bounds leakage.
const DefaultTTL = time.Hour

// generationMarkerEventID is the reserved event id AdvanceGeneration stamps its markers with.
// It leads with a NUL byte, the same reserved-control-byte trick evictReasonPrefix and
// groupTopicPrefix use in the root gateway package: every real id this package is ever handed
// comes from gateway's formatSSEEventID (connID + "-" + a decimal sequence), which can never
// produce one starting with a NUL, so a marker can never be mistaken for - or collide with - a
// real replayable event. Since strips every element carrying this id out of what it returns.
const generationMarkerEventID = "\x00goaktGatewaySSEGenerationMarker\x00"

// appendScript records one event at the tail of the connection's list, trims the list back to
// the most recent perConn elements, and re-arms the key's idle TTL, all atomically.
//
// KEYS[1] connection key. ARGV[1] JSON-encoded event, ARGV[2] perConn, ARGV[3] TTL in
// milliseconds.
var appendScript = goredis.NewScript(`
redis.call("RPUSH", KEYS[1], ARGV[1])
redis.call("LTRIM", KEYS[1], -tonumber(ARGV[2]), -1)
redis.call("PEXPIRE", KEYS[1], ARGV[3])
return 1
`)

// lastGenerationAndSeq is shared Lua, inlined into both scripts below rather than factored into
// a callable, since Redis EVAL scripts do not share state or functions across separate EVAL
// invocations - each script is its own self-contained source. It reads the connection list's
// tail element (the newest, and by construction the highest-generation, one - see the package
// doc's "Generation fencing" section) and decodes the generation/sequence it was stamped with,
// defaulting to 0/0 for a connection with no prior generational write (an empty list, or a tail
// written by the plain, non-generational Append).
const lastGenerationAndSeq = `
local lastGeneration = 0
local lastSeq = 0
local tail = redis.call("LINDEX", KEYS[1], -1)
if tail then
	local ok, decoded = pcall(cjson.decode, tail)
	if ok and type(decoded) == "table" and decoded.g and decoded.s then
		lastGeneration = decoded.g
		lastSeq = decoded.s
	end
end
`

// appendGenerationalScript is AppendGenerational's atomic accept/reject-and-assign-sequence
// counterpart to appendScript. See the package doc's "Generation fencing" section for how it
// derives the connection's last known generation/sequence from the list's own tail instead of a
// second key.
//
// KEYS[1] connection key. ARGV[1] event id, ARGV[2] base64-encoded payload, ARGV[3] caller's
// generation, ARGV[4] perConn, ARGV[5] TTL in milliseconds.
//
// Returns a two-element array: {1, newSeq} once accepted, or {0, lastSeq} when generation is
// stale and nothing was recorded.
var appendGenerationalScript = goredis.NewScript(lastGenerationAndSeq + `
local generation = tonumber(ARGV[3])
if generation < lastGeneration then
	return {0, lastSeq}
end

local newSeq = lastSeq + 1
local event = cjson.encode({id = ARGV[1], p = ARGV[2], g = generation, s = newSeq})
redis.call("RPUSH", KEYS[1], event)
redis.call("LTRIM", KEYS[1], -tonumber(ARGV[4]), -1)
redis.call("PEXPIRE", KEYS[1], ARGV[5])
return {1, newSeq}
`)

// advanceGenerationScript is AdvanceGeneration's atomic implementation: a no-op when the
// caller's generation does not exceed what the list's tail already carries, otherwise a marker
// element recording the new generation. The marker is stamped with lastSeq, not lastSeq + 1: it
// carries no real event, so it must not consume a sequence number, and the next
// AppendGenerational call (which computes its own sequence as the tail's s plus one) then
// resumes at exactly the value it would have had with no takeover in between - the counter
// GenerationalHistory documents as gapless across accepted AppendGenerational calls stays
// gapless with an AdvanceGeneration between two of them.
//
// KEYS[1] connection key. ARGV[1] generationMarkerEventID, ARGV[2] caller's generation,
// ARGV[3] perConn, ARGV[4] TTL in milliseconds.
//
// Returns 1 when a marker was written, 0 for the no-op case.
var advanceGenerationScript = goredis.NewScript(lastGenerationAndSeq + `
local generation = tonumber(ARGV[2])
if generation <= lastGeneration then
	return 0
end

local marker = cjson.encode({id = ARGV[1], p = "", g = generation, s = lastSeq})
redis.call("RPUSH", KEYS[1], marker)
redis.call("LTRIM", KEYS[1], -tonumber(ARGV[3]), -1)
redis.call("PEXPIRE", KEYS[1], ARGV[4])
return 1
`)

// History is a gateway.SSEHistory backed by a Redis or Valkey client. It is safe for
// concurrent use.
type History struct {
	client  goredis.UniversalClient
	prefix  string
	perConn int
	ttl     time.Duration
}

// Option configures a History created with New.
type Option func(*History)

// WithKeyPrefix namespaces every key this History reads or writes, so multiple gateway
// deployments (or unrelated applications) can share one Redis instance/database without
// colliding. Defaults to DefaultKeyPrefix.
func WithKeyPrefix(prefix string) Option {
	return func(h *History) { h.prefix = prefix }
}

// WithPerConn sets how many of the most recent events are retained per connection. Values
// below 1 are raised to 1. Defaults to DefaultPerConn.
func WithPerConn(n int) Option {
	return func(h *History) {
		h.perConn = max(n, 1)
	}
}

// WithTTL sets the idle reclaim window refreshed on every Append. A connection key is dropped
// by Redis once this duration elapses with no further Append, which is how a disconnected
// connection's buffer is reclaimed - buffers are never dropped on disconnect itself, because
// replay happens after the disconnect. Non-positive values are ignored and leave DefaultTTL.
func WithTTL(d time.Duration) Option {
	return func(h *History) {
		if d > 0 {
			h.ttl = d
		}
	}
}

// New creates a History backed by client. client may be a *redis.Client, a
// *redis.ClusterClient, *redis.Ring, or any other goredis.UniversalClient implementation,
// pointed at either a Redis or a Valkey server; every operation touches exactly one key, so
// the cluster case needs no hash tags.
func New(client goredis.UniversalClient, opts ...Option) *History {
	h := &History{
		client:  client,
		prefix:  DefaultKeyPrefix,
		perConn: DefaultPerConn,
		ttl:     DefaultTTL,
	}
	for _, opt := range opts {
		opt(h)
	}
	return h
}

// enforce compilation error
var (
	_ gateway.SSEHistory          = (*History)(nil)
	_ gateway.GenerationalHistory = (*History)(nil)
)

// storedEvent is the on-wire shape of one retained event. Payload is a []byte, so
// encoding/json base64-encodes it, keeping an arbitrary-byte payload safe without delimiters.
type storedEvent struct {
	ID      string `json:"id"`
	Payload []byte `json:"p"`
}

func (h *History) key(connID string) string {
	return h.prefix + connID
}

// Append implements gateway.SSEHistory. It appends the event to the connection's list,
// bounds the list to the most recent perConn events, and refreshes the connection key's idle
// TTL - all in one atomic Redis script.
func (h *History) Append(ctx context.Context, connID, eventID string, payload []byte) error {
	encoded, err := json.Marshal(storedEvent{ID: eventID, Payload: payload})
	if err != nil {
		return err
	}
	return appendScript.Run(ctx, h.client,
		[]string{h.key(connID)},
		encoded,
		strconv.Itoa(h.perConn),
		strconv.FormatInt(h.ttl.Milliseconds(), 10),
	).Err()
}

// AppendGenerational implements gateway.GenerationalHistory. See appendGenerationalScript for
// the atomic accept/reject/sequence-assignment logic. The payload is base64-encoded here,
// before the script ever sees it: cjson has no notion of Go's raw-bytes-as-base64-JSON-string
// convention for a []byte field, so producing that convention in Go (rather than in Lua) is
// what lets Since's plain encoding/json.Unmarshal decode an AppendGenerational-written element
// exactly like one Append wrote.
func (h *History) AppendGenerational(ctx context.Context, connID, eventID string, payload []byte, generation uint64) (uint64, error) {
	result, err := appendGenerationalScript.Run(ctx, h.client,
		[]string{h.key(connID)},
		eventID,
		base64.StdEncoding.EncodeToString(payload),
		strconv.FormatUint(generation, 10),
		strconv.Itoa(h.perConn),
		strconv.FormatInt(h.ttl.Milliseconds(), 10),
	).Result()
	if err != nil {
		return 0, err
	}

	accepted, seq, err := decodeGenerationalResult(result)
	if err != nil {
		return 0, err
	}
	if accepted == 0 {
		return 0, gateway.ErrStaleGeneration
	}
	return seq, nil
}

// AdvanceGeneration implements gateway.GenerationalHistory. See advanceGenerationScript.
func (h *History) AdvanceGeneration(ctx context.Context, connID string, generation uint64) error {
	return advanceGenerationScript.Run(ctx, h.client,
		[]string{h.key(connID)},
		generationMarkerEventID,
		strconv.FormatUint(generation, 10),
		strconv.Itoa(h.perConn),
		strconv.FormatInt(h.ttl.Milliseconds(), 10),
	).Err()
}

// decodeGenerationalResult unpacks the {accepted, seq} array appendGenerationalScript returns.
// go-redis decodes a Lua array reply into a []interface{} of int64 elements; anything else
// means the script or the client library changed shape out from under this code.
func decodeGenerationalResult(result any) (accepted int64, seq uint64, err error) {
	values, ok := result.([]any)
	if !ok || len(values) != 2 {
		return 0, 0, fmt.Errorf("gateway/ssehistory/redis: unexpected AppendGenerational script result %#v", result)
	}
	accepted, ok = values[0].(int64)
	if !ok {
		return 0, 0, fmt.Errorf("gateway/ssehistory/redis: unexpected AppendGenerational script result %#v", result)
	}
	rawSeq, ok := values[1].(int64)
	if !ok {
		return 0, 0, fmt.Errorf("gateway/ssehistory/redis: unexpected AppendGenerational script result %#v", result)
	}
	return accepted, uint64(rawSeq), nil
}

// Since implements gateway.SSEHistory. It reads the connection's currently retained events in
// wire order and applies the three Last-Event-ID cases: an empty lastEventID returns
// everything with no error; a known lastEventID returns the events strictly after it; an
// unknown lastEventID returns everything still retained together with gateway.ErrHistoryGap,
// so the caller can replay what survives and tell the client that earlier events are gone.
// Elements written by AdvanceGeneration carry no real event and are filtered out before the
// Last-Event-ID logic ever sees them, so they are invisible to every caller of this method.
func (h *History) Since(ctx context.Context, connID, lastEventID string) ([]gateway.SSEEvent, error) {
	raw, err := h.client.LRange(ctx, h.key(connID), 0, -1).Result()
	if err != nil {
		return nil, err
	}

	events := make([]gateway.SSEEvent, 0, len(raw))
	for _, item := range raw {
		var stored storedEvent
		if err := json.Unmarshal([]byte(item), &stored); err != nil {
			return nil, err
		}
		if stored.ID == generationMarkerEventID {
			continue
		}
		events = append(events, gateway.SSEEvent{ID: stored.ID, Payload: stored.Payload})
	}

	if lastEventID == "" {
		return emptyToNil(events), nil
	}
	for i, event := range events {
		if event.ID == lastEventID {
			return emptyToNil(events[i+1:]), nil
		}
	}
	return emptyToNil(events), gateway.ErrHistoryGap
}

// RefreshTTL re-arms the connection key's idle TTL without appending an event. The gateway
// SSEHandler calls it on every keepalive, so a live but low-traffic stream - one whose
// application produces no real event for longer than the TTL - keeps its buffer for as long as
// the connection stays up instead of having Redis reclaim it and then answering the client's
// reconnect with a false gateway.ErrHistoryGap. PEXPIRE on a key that does not exist is a
// no-op returning 0, so a connection that has not appended anything yet, or was already
// reclaimed, is left untouched exactly as gateway.MemorySSEHistory leaves an unknown one.
func (h *History) RefreshTTL(ctx context.Context, connID string) error {
	return h.client.PExpire(ctx, h.key(connID), h.ttl).Err()
}

// emptyToNil normalizes an empty result to a nil slice, matching gateway.MemorySSEHistory so
// callers observe the two backends identically.
func emptyToNil(events []gateway.SSEEvent) []gateway.SSEEvent {
	if len(events) == 0 {
		return nil
	}
	return events
}
