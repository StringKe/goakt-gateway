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

package gateway_test

import (
	"context"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	gateway "github.com/StringKe/goakt-gateway"
)

// TestMemoryOutboxAppendCopiesPayload proves Append snapshots the bytes rather than aliasing
// the caller's slice. SendToConnection hands the very same slice to Append and then to the
// socket write, so a caller that reuses the backing array after the send must not be able to
// rewrite what redelivery replays.
func TestMemoryOutboxAppendCopiesPayload(t *testing.T) {
	o := gateway.NewMemoryOutbox()
	ctx := context.Background()

	payload := []byte("original")
	_, _, err := o.Append(ctx, "conn", payload)
	require.NoError(t, err)

	// Mutate every byte of the input after Append has returned.
	for i := range payload {
		payload[i] = 'X'
	}

	msgs, err := o.Unacked(ctx, "conn")
	require.NoError(t, err)
	require.Len(t, msgs, 1)
	require.Equal(t, []byte("original"), msgs[0].Payload)
}

// TestMemoryOutboxUnackedReturnsIndependentCopies proves a caller may mutate a returned
// Payload without corrupting the stored snapshot or another reader's view. Copying the
// PersistedMessage struct alone would alias the stored backing array; the fix clones the
// payload per read.
func TestMemoryOutboxUnackedReturnsIndependentCopies(t *testing.T) {
	o := gateway.NewMemoryOutbox()
	ctx := context.Background()
	_, _, err := o.Append(ctx, "conn", []byte("keepme"))
	require.NoError(t, err)

	first, err := o.Unacked(ctx, "conn")
	require.NoError(t, err)
	require.Len(t, first, 1)
	for i := range first[0].Payload {
		first[0].Payload[i] = 'Z'
	}

	second, err := o.Unacked(ctx, "conn")
	require.NoError(t, err)
	require.Len(t, second, 1)
	require.Equal(t, []byte("keepme"), second[0].Payload, "a mutated returned payload must not leak into the stored snapshot")
}

// TestMemoryOutboxUnackedConcurrentReadersIsolated runs many readers mutating their own
// returned payloads at once. With the aliasing bug they would race on the shared backing
// array; with per-read copies each reader owns its bytes. Run under -race to catch the data
// race the shared slice would otherwise produce.
func TestMemoryOutboxUnackedConcurrentReadersIsolated(t *testing.T) {
	o := gateway.NewMemoryOutbox()
	ctx := context.Background()
	_, _, err := o.Append(ctx, "conn", []byte("shared"))
	require.NoError(t, err)

	var wg sync.WaitGroup
	for range 32 {
		wg.Go(func() {
			msgs, err := o.Unacked(ctx, "conn")
			require.NoError(t, err)
			require.Len(t, msgs, 1)
			for i := range msgs[0].Payload {
				msgs[0].Payload[i] = 'Q'
			}
		})
	}
	wg.Wait()

	msgs, err := o.Unacked(ctx, "conn")
	require.NoError(t, err)
	require.Equal(t, []byte("shared"), msgs[0].Payload)
}

// TestMemoryOutboxAppendMintsDistinctIDs proves Append, not the caller, is what mints the
// message id, and that two Appends never collide: two sends with identical bytes are two
// distinct entries under two distinct Outbox-minted ids, and acking one id leaves the other
// and its payload untouched.
func TestMemoryOutboxAppendMintsDistinctIDs(t *testing.T) {
	o := gateway.NewMemoryOutbox()
	ctx := context.Background()

	idA, seqA, err := o.Append(ctx, "conn", []byte("same-bytes"))
	require.NoError(t, err)
	idB, seqB, err := o.Append(ctx, "conn", []byte("same-bytes"))
	require.NoError(t, err)
	require.NotEmpty(t, idA)
	require.NotEmpty(t, idB)
	require.NotEqual(t, idA, idB, "two Appends must never mint the same id")

	msgs, err := o.Unacked(ctx, "conn")
	require.NoError(t, err)
	require.Len(t, msgs, 2, "identical payloads under distinct ids are distinct entries; the Outbox does not dedupe on content")

	require.NoError(t, o.Ack(ctx, "conn", idA, 0))

	msgs, err = o.Unacked(ctx, "conn")
	require.NoError(t, err)
	require.Equal(t, []gateway.PersistedMessage{{ID: idB, Seq: seqB, Payload: []byte("same-bytes")}}, msgs)
	require.Less(t, seqA, seqB)
}

// TestMemoryOutboxAckRejectsStaleGeneration proves the owner-lease fencing contract: once an
// Ack has been accepted under a given generation, a later Ack for the same connection carrying
// a lower generation is rejected with ErrStaleOwner instead of removing its message. This is
// what stops a node an owner-lease takeover has already superseded from continuing to drain a
// connection's Outbox tail after a successor has taken over.
func TestMemoryOutboxAckRejectsStaleGeneration(t *testing.T) {
	o := gateway.NewMemoryOutbox()
	ctx := context.Background()

	id1, _, err := o.Append(ctx, "conn", []byte("a"))
	require.NoError(t, err)
	id2, _, err := o.Append(ctx, "conn", []byte("b"))
	require.NoError(t, err)

	require.NoError(t, o.Ack(ctx, "conn", id1, 2))
	err = o.Ack(ctx, "conn", id2, 1)
	require.ErrorIs(t, err, gateway.ErrStaleOwner)

	msgs, err := o.Unacked(ctx, "conn")
	require.NoError(t, err)
	require.Equal(t, []gateway.PersistedMessage{{ID: id2, Seq: 2, Payload: []byte("b")}}, msgs, "a stale-rejected ack must not remove its message")
}

// TestMemoryOutboxAckOnNeverAppendedConnIsNoOp proves Ack against a connID this Outbox has
// never held a message for is a genuine no-op: it neither errors nor establishes any fencing
// floor, so a stray ack for an unknown connection cannot grow storage or later reject a
// legitimate first ack at a low generation.
func TestMemoryOutboxAckOnNeverAppendedConnIsNoOp(t *testing.T) {
	o := gateway.NewMemoryOutbox()
	ctx := context.Background()

	require.NoError(t, o.Ack(ctx, "conn-unknown", "m1", 0))
	require.NoError(t, o.Ack(ctx, "conn-unknown", "m1", 999))

	_, seq, err := o.Append(ctx, "conn-unknown", []byte("a"))
	require.NoError(t, err)
	require.EqualValues(t, 1, seq, "no floor or state should have survived the earlier no-op acks")
}

// TestMemoryOutboxAdvanceGenerationFencesSubsequentStaleAck is the P1 regression:
// OutboxGenerationAdvancer.AdvanceGeneration must raise the Ack fencing floor immediately, by
// itself, so a stale owner's in-flight Ack at its own (lower) generation is rejected even before
// the new owner has ever Acked anything of its own to raise the floor. Before this existed, an
// Ack at exactly the pre-takeover generation was accepted regardless, because nothing but a
// prior Ack call ever raised the floor.
func TestMemoryOutboxAdvanceGenerationFencesSubsequentStaleAck(t *testing.T) {
	o := gateway.NewMemoryOutbox()
	ctx := context.Background()

	id, _, err := o.Append(ctx, "conn", []byte("a"))
	require.NoError(t, err)

	// A takeover bumps the owner lease to generation 6 before the new owner has acked anything.
	require.NoError(t, o.AdvanceGeneration(ctx, "conn", 6))

	// The old owner's in-flight ack, still carrying its pre-takeover generation 5, must be
	// rejected rather than silently removing the message the new owner's client never acked.
	err = o.Ack(ctx, "conn", id, 5)
	require.ErrorIs(t, err, gateway.ErrStaleOwner)

	msgs, err := o.Unacked(ctx, "conn")
	require.NoError(t, err)
	require.Len(t, msgs, 1, "a stale-rejected ack after AdvanceGeneration must not remove its message")

	// The legitimate new owner's own ack, at generation 6, is accepted normally.
	require.NoError(t, o.Ack(ctx, "conn", id, 6))
	msgs, err = o.Unacked(ctx, "conn")
	require.NoError(t, err)
	require.Empty(t, msgs)
}

// TestMemoryOutboxAdvanceGenerationIsNoOpWhenNotHigher proves AdvanceGeneration never lowers an
// already-recorded floor: a takeover's own advance (or a still newer one) must not be undone by
// a stale, delayed AdvanceGeneration call carrying an older generation.
func TestMemoryOutboxAdvanceGenerationIsNoOpWhenNotHigher(t *testing.T) {
	o := gateway.NewMemoryOutbox()
	ctx := context.Background()

	_, _, err := o.Append(ctx, "conn", []byte("a"))
	require.NoError(t, err)

	require.NoError(t, o.AdvanceGeneration(ctx, "conn", 6))
	require.NoError(t, o.AdvanceGeneration(ctx, "conn", 3), "a lower generation must not lower the floor")

	err = o.Ack(ctx, "conn", "whatever", 4)
	require.ErrorIs(t, err, gateway.ErrStaleOwner, "the floor must still be 6, not 3, after the stale advance")
}

// TestReplayTailSeq proves ReplayTailSeq reads the last (highest) Seq of an ascending-ordered
// batch, which is the contract a caller enforcing replay-then-realtime ordering across a
// takeover depends on, and that an empty tail reports 0 rather than panicking.
func TestReplayTailSeq(t *testing.T) {
	require.EqualValues(t, 0, gateway.ReplayTailSeq(nil))
	require.EqualValues(t, 0, gateway.ReplayTailSeq([]gateway.PersistedMessage{}))

	msgs := []gateway.PersistedMessage{
		{ID: "a", Seq: 1},
		{ID: "b", Seq: 2},
		{ID: "c", Seq: 7},
	}
	require.EqualValues(t, 7, gateway.ReplayTailSeq(msgs))
}

// TestOutboxEnvelopeRoundTrip proves EncodeOutboxEnvelope/DecodeOutboxEnvelope are exact
// inverses across the message id, seq, and payload a client needs to recover, including the
// nil-vs-empty-payload distinction and a message id containing bytes that are not printable
// ASCII, since a message id must round-trip byte for byte regardless of its content.
func TestOutboxEnvelopeRoundTrip(t *testing.T) {
	cases := []struct {
		name    string
		msgID   string
		seq     uint64
		payload []byte
	}{
		{"typical", "5b1f6e8e-6f1e-4e9a-9b0e-2f6a6e8e6f1e", 42, []byte("hello world")},
		{"nil payload", "m1", 1, nil},
		{"empty payload", "m1", 1, []byte{}},
		{"zero seq", "m1", 0, []byte("x")},
		{"empty id", "", 3, []byte("x")},
		{"id with NUL and high bytes", "id\x00\xff\x01", 9, []byte{0, 1, 2, 255}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			frame, err := gateway.EncodeOutboxEnvelope(tc.msgID, tc.seq, tc.payload)
			require.NoError(t, err)

			gotID, gotSeq, gotPayload, err := gateway.DecodeOutboxEnvelope(frame)
			require.NoError(t, err)
			require.Equal(t, tc.msgID, gotID)
			require.Equal(t, tc.seq, gotSeq)
			if len(tc.payload) == 0 {
				require.Empty(t, gotPayload)
			} else {
				require.Equal(t, tc.payload, gotPayload)
			}
		})
	}
}

// TestOutboxEnvelopeEncodeRejectsOversizedMsgID proves EncodeOutboxEnvelope reports an error
// rather than silently truncating a message id too long for its 16-bit length prefix.
func TestOutboxEnvelopeEncodeRejectsOversizedMsgID(t *testing.T) {
	oversized := strings.Repeat("x", 1<<16)
	_, err := gateway.EncodeOutboxEnvelope(oversized, 1, nil)
	require.Error(t, err)
}

// TestOutboxEnvelopeDecodeRejectsShortData proves a frame shorter than the fixed header is
// reported as malformed rather than panicking on an out-of-range slice.
func TestOutboxEnvelopeDecodeRejectsShortData(t *testing.T) {
	_, _, _, err := gateway.DecodeOutboxEnvelope([]byte{1})
	require.Error(t, err)
	_, _, _, err = gateway.DecodeOutboxEnvelope(nil)
	require.Error(t, err)
}

// TestOutboxEnvelopeDecodeRejectsUnsupportedVersion proves a frame whose version byte this
// build does not recognize is rejected rather than misparsed.
func TestOutboxEnvelopeDecodeRejectsUnsupportedVersion(t *testing.T) {
	frame, err := gateway.EncodeOutboxEnvelope("m1", 1, []byte("x"))
	require.NoError(t, err)
	frame[0] = 0xEE

	_, _, _, err = gateway.DecodeOutboxEnvelope(frame)
	require.Error(t, err)
}

// TestOutboxEnvelopeDecodeRejectsTruncatedFrame proves a frame cut short between its declared
// message id length and the bytes actually present is reported as malformed, since guessing at
// its contents would deliver garbage to a socket or an application.
func TestOutboxEnvelopeDecodeRejectsTruncatedFrame(t *testing.T) {
	frame, err := gateway.EncodeOutboxEnvelope("message-id", 1, []byte("payload"))
	require.NoError(t, err)

	_, _, _, err = gateway.DecodeOutboxEnvelope(frame[:5])
	require.Error(t, err)
}

// TestOutboxEnvelopeReplayMatchesUnackedOrdering proves the replay path end to end: envelopes
// built from a real Outbox's Unacked tail decode back to the exact id/seq/payload triples the
// Outbox assigned, in the same ascending order, which is what lets a redelivered message be
// acked by the id its envelope carries exactly like a real-time one.
func TestOutboxEnvelopeReplayMatchesUnackedOrdering(t *testing.T) {
	o := gateway.NewMemoryOutbox()
	ctx := context.Background()

	var ids []string
	var seqs []uint64
	for _, payload := range []string{"a", "b", "c"} {
		id, seq, err := o.Append(ctx, "conn", []byte(payload))
		require.NoError(t, err)
		ids = append(ids, id)
		seqs = append(seqs, seq)
	}

	msgs, err := o.Unacked(ctx, "conn")
	require.NoError(t, err)
	require.Len(t, msgs, 3)
	require.EqualValues(t, seqs[2], gateway.ReplayTailSeq(msgs))

	for i, msg := range msgs {
		frame, err := gateway.EncodeOutboxEnvelope(msg.ID, msg.Seq, msg.Payload)
		require.NoError(t, err)

		gotID, gotSeq, gotPayload, err := gateway.DecodeOutboxEnvelope(frame)
		require.NoError(t, err)
		require.Equal(t, ids[i], gotID)
		require.Equal(t, seqs[i], gotSeq)
		require.Equal(t, msg.Payload, gotPayload)
	}
}
