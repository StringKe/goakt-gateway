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

// Package redis provides a Redis- or Valkey-backed gateway.Outbox, so a connection's
// unacknowledged tail survives a node failure: a client that reconnects to a different
// process still finds the messages the original process had sent but not yet had
// acknowledged, and they are redelivered. It is a separate package specifically so that
// importing the root gateway package never pulls in github.com/redis/go-redis/v9 for
// applications that use gateway.MemoryOutbox, or no Outbox at all.
//
// # Data model
//
// Each connection id is one Redis hash. Its message fields are keyed by a message id this
// Outbox mints (see Outbox.Append) and hold the value "<seq>:<payload>", where seq is the
// decimal sequence and payload is the raw bytes after the first colon (a colon never appears
// in the decimal prefix, so the split is unambiguous and payload stays binary-safe). Two
// reserved fields never collide with a minted message id (see seqField): seqField holds the
// per-connection sequence counter, and ackGenField holds the Ack generation fencing floor
// (see gateway.Outbox.Ack). The hash shape is chosen over a sorted set because
// gateway.Outbox.Ack removes by message id, which a hash does in one HDEL; a sorted set
// keyed by Seq would force a scan to find the member carrying a given id. Ordering by Seq,
// which Unacked must return, is recovered by sorting the (small, in-flight) tail on the
// client after HGETALL rather than paid for on every write.
//
// The sequence is assigned server-side by HINCRBY on seqField inside the append script, not
// by the calling process, so it stays monotonic across every process and node appending to
// the same connection through this Redis and across a restart of any of them. That is what
// makes the persisted Seq trustworthy for a client that dedupes on it: a per-process counter
// would restart at 1 on reboot and collide with a still-stored message. The counter lives in
// the same hash as the messages, so it is reclaimed together with them by DropConn's DEL or
// the TTL, and every operation still touches exactly one key, so a Redis Cluster routes each
// to one slot with no hash tags.
//
// The Ack generation floor recorded in ackGenField is checked and raised atomically with the
// message removal it gates, inside ackScript, so an Ack whose generation trails a previously
// accepted one for the same connection is rejected rather than applied - see
// gateway.ErrStaleOwner. ackScript is a true no-op for a connection hash that does not exist,
// so a stream of acks for connection ids this Outbox never appended to cannot create keys.
//
// Only HINCRBY/HSET/HGET/HGETALL/HDEL/EXISTS/DEL and PEXPIRE are used, all present and
// identical on Redis 7.2 and Valkey 8.
//
// # Expiry
//
// With WithTTL, the per-connection hash carries a Redis TTL re-armed on every Append, so a
// connection that never reconnects to drain or DropConn its tail is reclaimed by the server
// rather than leaking. Without it (the default) a hash lives until Ack empties it or
// DropConn deletes it. The TTL bounds at-least-once storage, not delivery: a message whose
// key expires before the client reconnects is simply not redelivered, the same outcome as
// if the client had never come back.
package redis

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	goredis "github.com/redis/go-redis/v9"

	gateway "github.com/StringKe/goakt-gateway"
)

// DefaultKeyPrefix namespaces outbox keys away from every other kind of key a gateway
// deployment may keep in the same Redis database (notably the presence and coordinator
// backends).
const DefaultKeyPrefix = "gateway:outbox:"

// seqField is the reserved hash field holding a connection's sequence counter. It begins
// with a NUL byte so it can never collide with a message id: Append mints message ids as
// UUIDs, which never contain a NUL. Unacked skips this field when reconstructing the message
// tail.
const seqField = "\x00seq"

// ackGenField is the reserved hash field holding a connection's Ack generation fencing floor
// (see gateway.Outbox.Ack). Like seqField it begins with a NUL byte so it can never collide
// with a minted message id. Unacked skips this field when reconstructing the message tail.
const ackGenField = "\x00ackgen"

// appendScript assigns the next sequence for the connection with HINCRBY, stores the message
// as "<seq>:<payload>" under its id, and, when a positive TTL is configured, re-arms the
// connection hash's expiry from the same round trip so a write, its sequence, and its TTL
// bump can never be observed apart. Assigning the sequence here rather than in the calling
// process is what keeps it monotonic across processes, nodes, and restarts. The reserved
// sequence field is passed as an argument, not inlined, so its NUL byte never has to survive
// the Lua source lexer.
//
// KEYS[1] connection key. ARGV[1] message id, ARGV[2] raw payload, ARGV[3] ttl in
// milliseconds (0 for no expiry), ARGV[4] reserved sequence field.
var appendScript = goredis.NewScript(`
local seq = redis.call("HINCRBY", KEYS[1], ARGV[4], 1)
redis.call("HSET", KEYS[1], ARGV[1], seq .. ":" .. ARGV[2])
local ttl = tonumber(ARGV[3])
if ttl > 0 then
	redis.call("PEXPIRE", KEYS[1], ttl)
end
return seq
`)

// ackScript rejects an Ack whose generation trails the floor already recorded for the
// connection (see gateway.Outbox.Ack and gateway.ErrStaleOwner), and otherwise removes the
// named message and raises the floor to the accepted generation, all in one round trip so the
// floor check and the mutation it gates can never be observed apart. It is a true no-op - it
// creates no key and records no floor - for a connection hash that does not exist, so a
// stream of acks for connection ids this Outbox has never appended to cannot create storage.
//
// KEYS[1] connection key. ARGV[1] message id, ARGV[2] generation (decimal string), ARGV[3]
// reserved ack generation field. Returns 1 if accepted (message removed or absent, floor
// raised), 0 if rejected as stale.
var ackScript = goredis.NewScript(`
if redis.call("EXISTS", KEYS[1]) == 0 then
	return 1
end
local floor = redis.call("HGET", KEYS[1], ARGV[3])
if floor and tonumber(ARGV[2]) < tonumber(floor) then
	return 0
end
redis.call("HSET", KEYS[1], ARGV[3], ARGV[2])
redis.call("HDEL", KEYS[1], ARGV[1])
return 1
`)

// advanceGenerationScript is gateway.OutboxGenerationAdvancer.AdvanceGeneration's atomic
// implementation: a no-op when the connection has no Outbox state or the generation is not
// strictly greater than the floor already recorded. Owner registration must not create an empty
// Redis hash solely for fencing.
//
// KEYS[1] connection key. ARGV[1] generation (decimal string), ARGV[2] reserved ack generation
// field, ARGV[3] ttl in milliseconds (0 for no expiry).
var advanceGenerationScript = goredis.NewScript(`
if redis.call("EXISTS", KEYS[1]) == 0 then
	return 0
end
local floor = redis.call("HGET", KEYS[1], ARGV[2])
if not floor or tonumber(ARGV[1]) > tonumber(floor) then
	redis.call("HSET", KEYS[1], ARGV[2], ARGV[1])
end
local ttl = tonumber(ARGV[3])
if ttl > 0 then
	redis.call("PEXPIRE", KEYS[1], ttl)
end
return 1
`)

// Outbox is a gateway.Outbox backed by a Redis or Valkey client. It is safe for concurrent
// use.
type Outbox struct {
	client goredis.UniversalClient
	prefix string
	ttl    time.Duration
}

// Option configures an Outbox created with New.
type Option func(*Outbox)

// WithKeyPrefix namespaces every key this Outbox reads or writes, so multiple gateway
// deployments (or unrelated applications) can share one Redis instance/database without
// colliding. Defaults to DefaultKeyPrefix.
func WithKeyPrefix(prefix string) Option {
	return func(o *Outbox) { o.prefix = prefix }
}

// WithTTL bounds how long an unacknowledged tail is retained after its last Append, so a
// connection that never reconnects to drain it cannot leak storage. The TTL is re-armed on
// every Append. A non-positive duration (the default) means the tail lives until Ack empties
// it or DropConn deletes it.
func WithTTL(d time.Duration) Option {
	return func(o *Outbox) { o.ttl = d }
}

// New creates an Outbox backed by client. client may be a *redis.Client,
// *redis.ClusterClient, *redis.Ring, or any other goredis.UniversalClient implementation,
// pointed at either a Redis or a Valkey server; every operation touches exactly one key, so
// the cluster case needs no hash tags.
func New(client goredis.UniversalClient, opts ...Option) *Outbox {
	o := &Outbox{client: client, prefix: DefaultKeyPrefix}
	for _, opt := range opts {
		opt(o)
	}
	return o
}

// enforce compilation error
var (
	_ gateway.Outbox                   = (*Outbox)(nil)
	_ gateway.OutboxGenerationAdvancer = (*Outbox)(nil)
)

// key maps a connection id to its Redis hash key.
func (o *Outbox) key(connID string) string {
	return o.prefix + connID
}

// Append records payload as unacknowledged for connID under a freshly minted message id,
// stamping it with a server-assigned sequence. It re-arms the connection key's TTL when one
// is configured.
func (o *Outbox) Append(ctx context.Context, connID string, payload []byte) (string, uint64, error) {
	msgID := uuid.NewString()
	seq, err := appendScript.Run(ctx, o.client,
		[]string{o.key(connID)},
		msgID,
		payload,
		strconv.FormatInt(o.ttl.Milliseconds(), 10),
		seqField,
	).Int64()
	if err != nil {
		return "", 0, fmt.Errorf("gateway: failed to append outbox message %q for connection %q: %w", msgID, connID, err)
	}
	return msgID, uint64(seq), nil
}

// Unacked returns every message still unacknowledged for connID in ascending Seq order. The
// hash yields the tail unordered; it is sorted here because Redis stores no order across
// hash fields and the in-flight tail is small.
func (o *Outbox) Unacked(ctx context.Context, connID string) ([]gateway.PersistedMessage, error) {
	fields, err := o.client.HGetAll(ctx, o.key(connID)).Result()
	if err != nil {
		return nil, fmt.Errorf("gateway: failed to read outbox for connection %q: %w", connID, err)
	}
	if len(fields) == 0 {
		return nil, nil
	}
	msgs := make([]gateway.PersistedMessage, 0, len(fields))
	for id, raw := range fields {
		if id == seqField || id == ackGenField {
			continue
		}
		colon := strings.IndexByte(raw, ':')
		if colon < 0 {
			return nil, fmt.Errorf("gateway: stored outbox message %q for connection %q is missing its sequence prefix", id, connID)
		}
		seq, err := strconv.ParseUint(raw[:colon], 10, 64)
		if err != nil {
			return nil, fmt.Errorf("gateway: stored outbox message %q for connection %q has an invalid sequence: %w", id, connID, err)
		}
		msgs = append(msgs, gateway.PersistedMessage{ID: id, Seq: seq, Payload: []byte(raw[colon+1:])})
	}
	sort.Slice(msgs, func(i, j int) bool { return msgs[i].Seq < msgs[j].Seq })
	return msgs, nil
}

// Ack removes the message identified by msgID from connID, fenced by generation (see the
// gateway.Outbox.Ack doc comment): a generation trailing the floor already recorded for
// connID is rejected with gateway.ErrStaleOwner instead of applied. It is otherwise
// idempotent: acking an unknown id on a connection this Outbox has appended to (already
// dropped, expired, or never present) deletes nothing and is not an error, and acking a
// connID this Outbox has never appended to at all is a true no-op that records no fencing
// state and creates no key.
func (o *Outbox) Ack(ctx context.Context, connID, msgID string, generation uint64) error {
	accepted, err := ackScript.Run(ctx, o.client,
		[]string{o.key(connID)},
		msgID,
		strconv.FormatUint(generation, 10),
		ackGenField,
	).Int64()
	if err != nil {
		return fmt.Errorf("gateway: failed to ack outbox message %q for connection %q: %w", msgID, connID, err)
	}
	if accepted == 0 {
		return gateway.ErrStaleOwner
	}
	return nil
}

// AdvanceGeneration implements gateway.OutboxGenerationAdvancer. See advanceGenerationScript.
func (o *Outbox) AdvanceGeneration(ctx context.Context, connID string, generation uint64) error {
	_, err := advanceGenerationScript.Run(ctx, o.client,
		[]string{o.key(connID)},
		strconv.FormatUint(generation, 10),
		ackGenField,
		strconv.FormatInt(o.ttl.Milliseconds(), 10),
	).Result()
	if err != nil {
		return fmt.Errorf("gateway: failed to advance outbox ack generation floor for connection %q: %w", connID, err)
	}
	return nil
}

// DropConn removes all outbox state for connID. It is idempotent: dropping a connection with
// no stored tail is not an error.
func (o *Outbox) DropConn(ctx context.Context, connID string) error {
	if err := o.client.Del(ctx, o.key(connID)).Err(); err != nil {
		return fmt.Errorf("gateway: failed to drop outbox for connection %q: %w", connID, err)
	}
	return nil
}
