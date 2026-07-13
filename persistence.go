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
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"sort"
	"sync"

	"github.com/google/uuid"
)

// PersistedMessage is one message an Outbox holds for a connection until it is acknowledged.
// It is the return shape of Outbox.Unacked and, as ID and Seq, also of Outbox.Append.
//
// ID and Seq are Outbox-internal bookkeeping, minted by the Outbox itself rather than chosen
// by the caller: Append takes only connID and payload. A caller that writes payload to the
// socket embeds ID and Seq in the wire envelope it sends (see EncodeOutboxEnvelope), so a
// client can report ID back verbatim when it acknowledges receipt, and Registry.Ack /
// Outbox.Ack match on that ID to remove the stored message. Seq is the internal per-connection
// order redelivery replays in, ascending; ReplayTailSeq reads the high-water mark of a batch
// Unacked returns. Both fields must stay unique and monotonic across every process, node, and
// restart that shares one backend, which is why the Outbox - not the caller - assigns them.
type PersistedMessage struct {
	// ID is the Outbox-minted token Registry.Ack and Outbox.Ack match on to drop this stored
	// message. Append mints it and returns it so the caller can embed it in the envelope
	// written to the socket (see EncodeOutboxEnvelope); a client echoes it back verbatim when
	// acknowledging, and the server passes that value straight through to Registry.Ack.
	ID string

	// Seq is the Outbox-internal per-connection order redelivery replays in, ascending. The
	// Outbox assigns it on Append. See ReplayTailSeq for reading the high-water mark of a
	// batch Unacked returns.
	Seq uint64

	// Payload is the exact application bytes that were, or will be, written to the socket.
	// Unacked returns an independent copy so a caller may mutate it without touching stored
	// state. Any client-facing dedupe id lives in these bytes, chosen by the application.
	Payload []byte
}

// Outbox stores the messages a connection has been sent but has not yet acknowledged, so
// they can be redelivered after a reconnect. It turns the Registry's default at-most-once
// delivery into at-least-once for the connections whose sends go through it: a message is
// appended before it is written to the socket, redelivered on the next Register if still
// unacknowledged, and removed only once the client acks it.
//
// An Outbox is opt-in (see WithOutbox); a Registry without one never calls it and pays none
// of its storage or latency cost.
type Outbox interface {
	// Append records payload as unacknowledged for connID, minting a unique message id and
	// stamping it with a per-connection sequence number, both returned so the caller can embed
	// them in the wire envelope it writes to the socket (see EncodeOutboxEnvelope) before the
	// payload ever reaches the client. It is called before the payload is written to the
	// socket, so a send that never reaches the client still leaves a record to redeliver.
	//
	// The Outbox, not the caller, allocates both the id and the Seq, from its own durable and
	// shared state: only that keeps them unique and monotonic across every process and node
	// that appends to the same connID through the same backend, and across a restart of any of
	// them. The sequence is reclaimed only by DropConn (or a backend TTL), so a connection that
	// acks its whole tail still keeps counting up rather than reusing a Seq a still-connected
	// client already saw.
	Append(ctx context.Context, connID string, payload []byte) (msgID string, seq uint64, err error)

	// Unacked returns every message still unacknowledged for connID, in ascending Seq
	// order. The Registry calls it when connID registers, to redeliver what the previous
	// socket did not ack. A caller enforcing a replay-then-realtime delivery order reads the
	// tail's high-water mark off the result with ReplayTailSeq.
	Unacked(ctx context.Context, connID string) ([]PersistedMessage, error)

	// Ack removes the message identified by msgID from connID's unacknowledged set, fenced by
	// generation: once an Ack for connID has been accepted under a given generation, a later
	// call carrying a lower one is rejected with ErrStaleOwner instead of applied, so a node
	// whose owner lease (see WithOwnerLease) has already been taken over by another node
	// cannot keep draining the tail out from under its successor. Without a configured lease
	// callers always pass generation 0, which can never be fenced - the zero-cost,
	// single-instance default every opt-in feature in this package shares.
	//
	// Acking an unknown id on a connID the Outbox has never held a message for is not an error
	// and establishes no fencing state: an ack can arrive for a message a prior DropConn or
	// TTL already removed, or for a connID this Outbox has simply never seen.
	Ack(ctx context.Context, connID, msgID string, generation uint64) error

	// DropConn removes all state for connID, including any generation fencing floor Ack has
	// recorded. Applications call it when a connection is gone for good and its unacknowledged
	// tail should not be redelivered to a future reconnect.
	DropConn(ctx context.Context, connID string) error
}

// OutboxGenerationAdvancer is an optional Outbox extension for backends that participate in
// owner-lease generation fencing (see WithOwnerLease and Outbox.Ack): AdvanceGeneration raises a
// connection's Ack fencing floor the moment a takeover's lease acquisition is confirmed, before
// traffic resumes on the new owner - mirroring GenerationalHistory.AdvanceGeneration in
// sse_history.go. Without it, the floor is only ever raised by an Ack call itself, so a stale
// owner's in-flight Ack at its own (lower) generation can still be accepted after a takeover,
// simply because the new owner has not yet Acked anything to raise the floor against it.
//
// MemoryOutbox and persistence/redis.Outbox both implement it. An Outbox that does not is used
// unfenced by generation exactly as before WithOwnerLease existed - the same opt-in-capability
// pattern GenerationalHistory and PresenceFencer already establish elsewhere in this package.
type OutboxGenerationAdvancer interface {
	Outbox

	// AdvanceGeneration raises connID's Ack fencing floor to generation without acking any
	// message. It is a no-op, not an error, when generation is not strictly greater than the
	// floor already recorded for connID - that means a takeover (this one or a still newer
	// one) already advanced it past this call, so there is nothing to do.
	AdvanceGeneration(ctx context.Context, connID string, generation uint64) error
}

// ReplayTailSeq returns the highest Seq among msgs, the replay tail's high-water mark, or 0 if
// msgs is empty. A caller that redelivers an Outbox's unacknowledged tail on reconnect and
// must not let a fresh, real-time send overtake it on the wire uses this as the sequence a
// subsequent real-time envelope's Seq must exceed. Unacked already returns msgs sorted
// ascending by Seq, so this is just its last element; it is exposed here so that ordering
// invariant is not an implicit contract every caller has to independently rediscover.
func ReplayTailSeq(msgs []PersistedMessage) uint64 {
	if len(msgs) == 0 {
		return 0
	}
	return msgs[len(msgs)-1].Seq
}

// Outbox envelope wire format.
//
// EncodeOutboxEnvelope and DecodeOutboxEnvelope define the stable, language-neutral frame a
// caller wraps a payload in once an Outbox is configured (see WithOutbox), so a client that
// receives it can report the message id back on Registry.Ack without the application having
// to invent and thread its own id through the payload. It is a public wire contract a
// WebSocket/SSE client on the other end of the socket parses directly, distinct from the
// internal cluster fan-out framing this package uses for its own topic/group bridging.
//
// Layout, all integers big-endian, all offsets in bytes:
//
//	[0]        version, currently envelopeVersion (1)
//	[1:3]      uint16 length N of the message id
//	[3:3+N]    message id, raw bytes (Append mints ASCII/UTF-8 ids, e.g. a UUID string)
//	[3+N:11+N] uint64 seq
//	[11+N:]    payload, the exact bytes passed to Outbox.Append
//
// A client decodes an inbound frame by reading the version at offset 0 (rejecting one it does
// not recognize), N as the big-endian uint16 at [1:3], the message id as the following N
// bytes, seq as the big-endian uint64 that follows, and payload as everything after that. It
// hands payload to the application and, once processed, reports the message id back to the
// server on whatever channel the application defines for acks (a WebSocket message, an SSE
// POST, etc.); the server passes that id straight through to Registry.Ack. seq is an
// already-ordered hint for the client - one that only needs to feed messages to its
// application in order does not need to interpret it itself, only the server behind
// Unacked/Ack does.
//
// EncodeOutboxEnvelope/DecodeOutboxEnvelope are provided as the stable format a future sender
// wires in; this package does not yet wrap either Registry.SendToConnection's real-time write
// or Registry.resendUnacked's redelivery in it. Both paths must adopt the same choice
// together - a socket that sees a raw payload on one write and an envelope on the next has no
// way to tell them apart - so the switch belongs with whichever call site also teaches the
// client how to recover a message id to ack, not with either persisted-message path alone.
const envelopeVersion = 1

// envelopeHeaderLen is the fixed portion of the header: version (1 byte) + message id length
// (2 bytes) + seq (8 bytes), before the variable-length message id and payload.
const envelopeHeaderLen = 1 + 2 + 8

var (
	// errEnvelopeTooShort means data is shorter than the fixed header EncodeOutboxEnvelope
	// always writes, so it cannot possibly be one of its frames.
	errEnvelopeTooShort = errors.New("gateway: outbox envelope is shorter than its fixed header")

	// errEnvelopeUnsupportedVersion means data's version byte does not match a version this
	// build of DecodeOutboxEnvelope knows how to parse.
	errEnvelopeUnsupportedVersion = errors.New("gateway: outbox envelope has an unsupported version")

	// errEnvelopeTruncated means data's declared message id length runs past the bytes
	// actually present, so the frame was cut short somewhere between encode and decode.
	errEnvelopeTruncated = errors.New("gateway: outbox envelope is truncated")
)

// EncodeOutboxEnvelope frames payload with msgID and seq using the wire format documented
// above. It returns an error only if msgID is too long to fit the format's 16-bit length
// prefix; an id minted by Outbox.Append (a UUID string) never is, so this only matters to a
// caller supplying its own id.
func EncodeOutboxEnvelope(msgID string, seq uint64, payload []byte) ([]byte, error) {
	if len(msgID) > math.MaxUint16 {
		return nil, fmt.Errorf("gateway: outbox envelope message id is %d bytes, exceeding the %d byte limit", len(msgID), math.MaxUint16)
	}
	idLen := len(msgID)
	buf := make([]byte, envelopeHeaderLen+idLen+len(payload))
	buf[0] = envelopeVersion
	binary.BigEndian.PutUint16(buf[1:3], uint16(idLen))
	copy(buf[3:3+idLen], msgID)
	binary.BigEndian.PutUint64(buf[3+idLen:envelopeHeaderLen+idLen], seq)
	copy(buf[envelopeHeaderLen+idLen:], payload)
	return buf, nil
}

// DecodeOutboxEnvelope parses a frame written by EncodeOutboxEnvelope. The returned payload
// aliases data's backing array; a caller that retains it past data's lifetime must copy it. A
// malformed frame is reported as such rather than guessed at, since it can only come from a
// build running an incompatible version.
func DecodeOutboxEnvelope(data []byte) (msgID string, seq uint64, payload []byte, err error) {
	if len(data) < 3 {
		return "", 0, nil, errEnvelopeTooShort
	}
	if data[0] != envelopeVersion {
		return "", 0, nil, errEnvelopeUnsupportedVersion
	}
	idLen := int(binary.BigEndian.Uint16(data[1:3]))
	if len(data) < envelopeHeaderLen+idLen {
		return "", 0, nil, errEnvelopeTruncated
	}
	msgID = string(data[3 : 3+idLen])
	seq = binary.BigEndian.Uint64(data[3+idLen : envelopeHeaderLen+idLen])
	payload = data[envelopeHeaderLen+idLen:]
	return msgID, seq, payload, nil
}

// WithOutbox attaches an Outbox, upgrading SendToConnection to at-least-once delivery: each
// payload is persisted before it is written to the socket, and any still-unacknowledged
// message is redelivered when the connection registers again. Acknowledge messages with
// Registry.Ack. Without an Outbox the Registry keeps its default at-most-once,
// fire-and-forget behaviour and stores nothing.
func WithOutbox(o Outbox) RegistryOption {
	return func(r *Registry) { r.outbox = o }
}

// Ack removes a message from connID's outbox once the client confirms it received it. It is
// how the at-least-once loop terminates: an unacknowledged message is redelivered on the next
// Register, an acknowledged one is not. Without an Outbox configured it is a no-op that
// returns nil, so application ack handling can stay unconditional.
//
// The generation Ack forwards to the Outbox is whatever this node's local entry for connID
// currently holds - 0 when WithOwnerLease is not configured, and also 0 when this node holds
// no local entry for connID at all, which fences the ack exactly as if it came from a node
// whose generation has already been superseded. Only the node actually serving a connection's
// socket ever receives that connection's ack frame, so the local generation is always the
// caller's own current one; it is never something the caller has to supply.
func (r *Registry) Ack(ctx context.Context, connID, msgID string) error {
	if r.outbox == nil {
		return nil
	}
	r.mu.RLock()
	entry, ok := r.conns[connID]
	r.mu.RUnlock()
	var generation uint64
	if ok {
		generation = entry.generation.Load()
	}
	return r.outbox.Ack(ctx, connID, msgID, generation)
}

// resendUnacked redelivers every message the Outbox still holds for a freshly registered
// connection, in Seq order, writing directly to the socket. It runs after registration
// completes so a reconnecting client catches up on the tail its previous socket never
// acknowledged. It writes the same raw msg.Payload SendToConnection's real-time path writes
// (see the package-level EncodeOutboxEnvelope doc comment): both paths must agree on whether
// the wire carries a raw payload or an envelope, and wiring that choice is the sender's call,
// made once by whichever caller of Registry.Ack recovers the message id - not by this
// function alone, since it enveloping the replay tail while the real-time path stays raw
// would hand two different wire shapes to the same client. Redelivery failures are logged,
// not retried inline: the message stays in the Outbox and will be redelivered again on the
// next reconnect.
func (r *Registry) resendUnacked(ctx context.Context, entry *connEntry) {
	if r.outbox == nil {
		return
	}
	msgs, err := r.outbox.Unacked(ctx, entry.id)
	if err != nil {
		r.logger.Warnf("gateway: failed to read unacked messages for connection %q: %v", entry.id, err)
		return
	}
	for _, msg := range msgs {
		if err := entry.send(msg.Payload); err != nil {
			r.logger.Warnf("gateway: failed to redeliver unacked message %q to connection %q: %v", msg.ID, entry.id, err)
		}
	}
}

// MemoryOutbox is an in-process Outbox. It fits a single-node deployment and tests; in a
// cluster it is the wrong choice, because a connection that reconnects to a different node
// finds none of the unacknowledged messages the original node held.
type MemoryOutbox struct {
	mu    sync.Mutex
	conns map[string][]PersistedMessage
	// seqs is the per-connection sequence counter. Its durability horizon is the process
	// lifetime, which is all a single-process Outbox can offer; an entry is kept until
	// DropConn retires the id so Seq stays monotonic while the connection lives, even across
	// a fully drained (all-acked) tail. Its presence also marks whether this Outbox has ever
	// held state for a connID at all, which Ack uses to decide whether an ack is a true no-op.
	seqs map[string]uint64
	// generations is the per-connection Ack fencing floor (see Outbox.Ack): the highest
	// generation any accepted Ack or AdvanceGeneration call has carried for a connID. An Ack
	// for a connID this Outbox has never held a message for stays a true no-op and creates no
	// entry here, so a stream of acks for connection ids this Outbox never heard of cannot grow
	// this map without bound; AdvanceGeneration is exempt from that gate because it is only ever
	// called by Registry itself for a connection id it is actually registering, which is bounded
	// by the number of real connections, not attacker-controlled input. It is cleared by
	// DropConn along with everything else.
	generations map[string]uint64
}

// enforce compilation error
var (
	_ Outbox                   = (*MemoryOutbox)(nil)
	_ OutboxGenerationAdvancer = (*MemoryOutbox)(nil)
)

// NewMemoryOutbox creates an in-process Outbox.
func NewMemoryOutbox() *MemoryOutbox {
	return &MemoryOutbox{
		conns:       make(map[string][]PersistedMessage),
		seqs:        make(map[string]uint64),
		generations: make(map[string]uint64),
	}
}

// Append records payload as unacknowledged for connID under a freshly minted message id,
// stamping it with the next per-connection sequence number. The payload is copied before it is
// stored: the caller (and SendToConnection, which reuses the same slice to write the socket)
// may mutate or reuse the backing array afterwards, and the stored redelivery snapshot must
// not change with it. A serializing backend such as persistence/redis copies inherently;
// MemoryOutbox matches that contract explicitly.
func (o *MemoryOutbox) Append(_ context.Context, connID string, payload []byte) (string, uint64, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	msgID := uuid.NewString()
	o.seqs[connID]++
	seq := o.seqs[connID]
	o.conns[connID] = append(o.conns[connID], PersistedMessage{ID: msgID, Seq: seq, Payload: clonePayload(payload)})
	return msgID, seq, nil
}

// Unacked returns every unacknowledged message for connID in ascending Seq order. Each
// returned message carries an independent copy of its payload: copying the PersistedMessage
// struct alone would alias the stored slice's backing array, so a caller mutating a returned
// Payload (or two concurrent readers touching theirs) would corrupt the stored snapshot and
// each other. The tail is small, so cloning it per read is cheap.
func (o *MemoryOutbox) Unacked(_ context.Context, connID string) ([]PersistedMessage, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	src := o.conns[connID]
	out := make([]PersistedMessage, len(src))
	for i, msg := range src {
		msg.Payload = clonePayload(msg.Payload)
		out[i] = msg
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Seq < out[j].Seq })
	return out, nil
}

// clonePayload returns an independent copy of p, preserving the nil/empty distinction so a
// round-tripped nil payload stays nil rather than becoming a non-nil empty slice.
func clonePayload(p []byte) []byte {
	if p == nil {
		return nil
	}
	dup := make([]byte, len(p))
	copy(dup, p)
	return dup
}

// Ack removes the message identified by msgID from connID, fenced by generation (see the
// Outbox.Ack doc comment). Acking a connID this Outbox has never appended to is a true no-op:
// nothing is created, and no fencing floor is recorded. Acking a known connID with an unknown
// msgID still raises the fencing floor to generation (if higher) even though no message is
// removed, so a later, lower-generation ack for that same connID is correctly rejected as
// stale regardless of which specific message it names.
func (o *MemoryOutbox) Ack(_ context.Context, connID, msgID string, generation uint64) error {
	o.mu.Lock()
	defer o.mu.Unlock()

	if _, everAppended := o.seqs[connID]; !everAppended {
		return nil
	}

	if floor, ok := o.generations[connID]; ok && generation < floor {
		return ErrStaleOwner
	}
	o.generations[connID] = generation

	msgs, ok := o.conns[connID]
	if !ok {
		return nil
	}
	for i, msg := range msgs {
		if msg.ID != msgID {
			continue
		}
		o.conns[connID] = append(msgs[:i], msgs[i+1:]...)
		if len(o.conns[connID]) == 0 {
			delete(o.conns, connID)
		}
		return nil
	}
	return nil
}

// AdvanceGeneration implements OutboxGenerationAdvancer: it raises connID's Ack fencing floor
// to generation, creating tracking state for connID if none exists yet (unlike Ack, which
// never creates state for a connID it has no message for - see the generations field doc
// comment for why that gate does not apply here).
func (o *MemoryOutbox) AdvanceGeneration(_ context.Context, connID string, generation uint64) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	if floor, ok := o.generations[connID]; !ok || generation > floor {
		o.generations[connID] = generation
	}
	return nil
}

// DropConn removes all state for connID, including its sequence counter and generation
// fencing floor, so a future connection reusing the id starts fresh. It is the only thing
// that reclaims the counter: Ack deliberately leaves it in place so Seq keeps rising while the
// id is live.
func (o *MemoryOutbox) DropConn(_ context.Context, connID string) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	delete(o.conns, connID)
	delete(o.seqs, connID)
	delete(o.generations, connID)
	return nil
}
