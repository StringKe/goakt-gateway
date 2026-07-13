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

// Package conformance is a shared test suite every gateway.Outbox implementation must pass,
// so gateway.MemoryOutbox and persistence/redis.Outbox (and any third-party implementation)
// are held to the exact same at-least-once contract: a message is retained once appended,
// stamped with a per-connection id and sequence the Outbox itself mints in append order,
// remains readable until acknowledged, is redeliverable in Seq order, disappears only on Ack
// or DropConn, and rejects an Ack whose generation trails one already accepted for the same
// connection. An implementation that drops an unacknowledged message early would silently
// lose the tail a reconnecting client depends on; one that let two messages share a Seq would
// let a Seq-deduping client silently discard one; one that accepted a stale-generation Ack
// would let a node already fenced out by an owner-lease takeover (see gateway.WithOwnerLease)
// keep draining the tail out from under its successor.
package conformance

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	gateway "github.com/StringKe/goakt-gateway"
)

// Run exercises factory() against the gateway.Outbox contract. factory must return a fresh,
// empty Outbox; Run calls it once per subtest so implementations backed by a shared external
// service (e.g. Redis) do not see state leak between subtests as long as factory picks a
// fresh key namespace or database per call.
func Run(t *testing.T, factory func(t *testing.T) gateway.Outbox) {
	t.Helper()

	t.Run("Unacked of an unknown connection is empty", func(t *testing.T) {
		o := factory(t)
		msgs, err := o.Unacked(context.Background(), "conn-absent")
		require.NoError(t, err)
		require.Empty(t, msgs)
	})

	t.Run("Append mints a non-empty id and returns Seq 1 for the first message", func(t *testing.T) {
		o := factory(t)
		ctx := context.Background()
		msgID, seq, err := o.Append(ctx, "conn-a", []byte("hello"))
		require.NoError(t, err)
		require.NotEmpty(t, msgID)
		require.EqualValues(t, 1, seq)
	})

	t.Run("Append then Unacked returns the message intact under the minted id", func(t *testing.T) {
		o := factory(t)
		ctx := context.Background()
		msgID, seq, err := o.Append(ctx, "conn-a", []byte("hello"))
		require.NoError(t, err)

		msgs, err := o.Unacked(ctx, "conn-a")
		require.NoError(t, err)
		require.Equal(t, []gateway.PersistedMessage{{ID: msgID, Seq: seq, Payload: []byte("hello")}}, msgs)
	})

	t.Run("Unacked returns messages in append order under an ascending Seq", func(t *testing.T) {
		o := factory(t)
		ctx := context.Background()
		id1, seq1, err := o.Append(ctx, "conn-a", []byte("c"))
		require.NoError(t, err)
		id2, seq2, err := o.Append(ctx, "conn-a", []byte("a"))
		require.NoError(t, err)
		id3, seq3, err := o.Append(ctx, "conn-a", []byte("b"))
		require.NoError(t, err)
		require.Less(t, seq1, seq2)
		require.Less(t, seq2, seq3)

		msgs, err := o.Unacked(ctx, "conn-a")
		require.NoError(t, err)
		require.Equal(t, []gateway.PersistedMessage{
			{ID: id1, Seq: seq1, Payload: []byte("c")},
			{ID: id2, Seq: seq2, Payload: []byte("a")},
			{ID: id3, Seq: seq3, Payload: []byte("b")},
		}, msgs)
		require.Equal(t, seq3, gateway.ReplayTailSeq(msgs), "the replay tail's high-water mark is its last, highest Seq")
	})

	t.Run("an unacked message survives being read repeatedly", func(t *testing.T) {
		// The whole point of at-least-once: reading the tail does not consume it, so a
		// redelivery that itself never gets acked is redelivered again on the next reconnect.
		o := factory(t)
		ctx := context.Background()
		msgID, seq, err := o.Append(ctx, "conn-a", []byte("x"))
		require.NoError(t, err)
		want := gateway.PersistedMessage{ID: msgID, Seq: seq, Payload: []byte("x")}

		for range 3 {
			msgs, err := o.Unacked(ctx, "conn-a")
			require.NoError(t, err)
			require.Equal(t, []gateway.PersistedMessage{want}, msgs)
		}
	})

	t.Run("Seq keeps rising after the tail is fully drained", func(t *testing.T) {
		// A connection that acks everything and is sent more must not reuse a Seq a still
		// live client already saw: the counter is reclaimed only by DropConn, not by an
		// emptied tail.
		o := factory(t)
		ctx := context.Background()
		id1, seq1, err := o.Append(ctx, "conn-a", []byte("a"))
		require.NoError(t, err)
		require.NoError(t, o.Ack(ctx, "conn-a", id1, 0))
		id2, seq2, err := o.Append(ctx, "conn-a", []byte("b"))
		require.NoError(t, err)
		require.Greater(t, seq2, seq1)

		msgs, err := o.Unacked(ctx, "conn-a")
		require.NoError(t, err)
		require.Equal(t, []gateway.PersistedMessage{{ID: id2, Seq: seq2, Payload: []byte("b")}}, msgs)
	})

	t.Run("DropConn resets the sequence for a reused id", func(t *testing.T) {
		// DropConn retires the id, so a future connection reusing it is a fresh logical
		// connection and starts its sequence over.
		o := factory(t)
		ctx := context.Background()
		_, seq1, err := o.Append(ctx, "conn-a", []byte("a"))
		require.NoError(t, err)
		require.NoError(t, o.DropConn(ctx, "conn-a"))
		id2, seq2, err := o.Append(ctx, "conn-a", []byte("b"))
		require.NoError(t, err)
		require.Equal(t, seq1, seq2, "a reused id after DropConn starts its sequence over")

		msgs, err := o.Unacked(ctx, "conn-a")
		require.NoError(t, err)
		require.Equal(t, []gateway.PersistedMessage{{ID: id2, Seq: seq2, Payload: []byte("b")}}, msgs)
	})

	t.Run("Ack removes only the named message", func(t *testing.T) {
		o := factory(t)
		ctx := context.Background()
		id1, _, err := o.Append(ctx, "conn-a", []byte("a"))
		require.NoError(t, err)
		id2, seq2, err := o.Append(ctx, "conn-a", []byte("b"))
		require.NoError(t, err)

		require.NoError(t, o.Ack(ctx, "conn-a", id1, 0))

		msgs, err := o.Unacked(ctx, "conn-a")
		require.NoError(t, err)
		require.Equal(t, []gateway.PersistedMessage{{ID: id2, Seq: seq2, Payload: []byte("b")}}, msgs)
	})

	t.Run("Ack of every message empties the connection", func(t *testing.T) {
		o := factory(t)
		ctx := context.Background()
		msgID, _, err := o.Append(ctx, "conn-a", []byte("a"))
		require.NoError(t, err)
		require.NoError(t, o.Ack(ctx, "conn-a", msgID, 0))

		msgs, err := o.Unacked(ctx, "conn-a")
		require.NoError(t, err)
		require.Empty(t, msgs)
	})

	t.Run("Ack of an unknown message id is a no-op", func(t *testing.T) {
		o := factory(t)
		ctx := context.Background()
		require.NoError(t, o.Ack(ctx, "conn-absent", "m-absent", 0))

		msgID, _, err := o.Append(ctx, "conn-a", []byte("a"))
		require.NoError(t, err)
		require.NoError(t, o.Ack(ctx, "conn-a", "m-absent", 0))
		require.NoError(t, o.Ack(ctx, "conn-a", msgID, 0))
		require.NoError(t, o.Ack(ctx, "conn-a", msgID, 0), "a second Ack of the same id must not error")

		msgs, err := o.Unacked(ctx, "conn-a")
		require.NoError(t, err)
		require.Empty(t, msgs)
	})

	t.Run("DropConn removes the entire tail", func(t *testing.T) {
		o := factory(t)
		ctx := context.Background()
		_, _, err := o.Append(ctx, "conn-a", []byte("a"))
		require.NoError(t, err)
		_, _, err = o.Append(ctx, "conn-a", []byte("b"))
		require.NoError(t, err)

		require.NoError(t, o.DropConn(ctx, "conn-a"))

		msgs, err := o.Unacked(ctx, "conn-a")
		require.NoError(t, err)
		require.Empty(t, msgs)
	})

	t.Run("DropConn of an unknown connection is a no-op", func(t *testing.T) {
		o := factory(t)
		require.NoError(t, o.DropConn(context.Background(), "conn-absent"))
	})

	t.Run("connections are isolated from one another", func(t *testing.T) {
		o := factory(t)
		ctx := context.Background()
		_, _, err := o.Append(ctx, "conn-a", []byte("a"))
		require.NoError(t, err)
		id2, seq2, err := o.Append(ctx, "conn-b", []byte("b"))
		require.NoError(t, err)

		require.NoError(t, o.DropConn(ctx, "conn-a"))

		msgs, err := o.Unacked(ctx, "conn-b")
		require.NoError(t, err)
		require.Equal(t, []gateway.PersistedMessage{{ID: id2, Seq: seq2, Payload: []byte("b")}}, msgs)
	})

	t.Run("an empty payload round-trips", func(t *testing.T) {
		o := factory(t)
		ctx := context.Background()
		msgID, _, err := o.Append(ctx, "conn-a", nil)
		require.NoError(t, err)

		msgs, err := o.Unacked(ctx, "conn-a")
		require.NoError(t, err)
		require.Len(t, msgs, 1)
		require.Equal(t, msgID, msgs[0].ID)
		require.Empty(t, msgs[0].Payload)
	})

	t.Run("concurrent Append and Ack leave a consistent tail", func(t *testing.T) {
		o := factory(t)
		ctx := context.Background()

		const messages = 30
		var mu sync.Mutex
		acked := make(map[string]bool, messages)
		var wg sync.WaitGroup
		for i := range messages {
			wg.Go(func() {
				id, _, err := o.Append(ctx, "conn-a", []byte(fmt.Sprintf("m%d", i)))
				require.NoError(t, err)
				// Half the messages are acked again: whichever way the two operations
				// interleave, exactly the other half must survive.
				if i%2 == 0 {
					require.NoError(t, o.Ack(ctx, "conn-a", id, 0))
					mu.Lock()
					acked[id] = true
					mu.Unlock()
				}
			})
		}
		wg.Wait()

		msgs, err := o.Unacked(ctx, "conn-a")
		require.NoError(t, err)
		require.Len(t, msgs, messages/2)
		seen := make(map[uint64]bool, len(msgs))
		for _, m := range msgs {
			require.False(t, acked[m.ID], "an acked message must not survive")
			require.False(t, seen[m.Seq], "the Outbox must assign every concurrent Append a distinct Seq")
			seen[m.Seq] = true
		}
	})

	t.Run("Ack accepts a rising generation and rejects a lower one afterwards", func(t *testing.T) {
		// Models the owner-lease takeover fencing gateway.Outbox.Ack exists for: once a
		// higher-generation node's ack has been accepted for a connection, a lower-generation
		// node - one an owner-lease takeover has already superseded - must not be able to
		// drain the same connection's tail out from under it.
		o := factory(t)
		ctx := context.Background()
		id1, _, err := o.Append(ctx, "conn-a", []byte("a"))
		require.NoError(t, err)
		id2, _, err := o.Append(ctx, "conn-a", []byte("b"))
		require.NoError(t, err)

		require.NoError(t, o.Ack(ctx, "conn-a", id1, 5), "a higher generation must be accepted")
		require.ErrorIs(t, o.Ack(ctx, "conn-a", id2, 3), gateway.ErrStaleOwner, "a lower generation must be rejected as stale once a higher one has been accepted")

		msgs, err := o.Unacked(ctx, "conn-a")
		require.NoError(t, err)
		require.Equal(t, []gateway.PersistedMessage{{ID: id2, Seq: 2, Payload: []byte("b")}}, msgs, "the rejected ack must not have removed its message")
	})

	t.Run("Ack accepts a repeated equal generation", func(t *testing.T) {
		o := factory(t)
		ctx := context.Background()
		id1, _, err := o.Append(ctx, "conn-a", []byte("a"))
		require.NoError(t, err)
		id2, _, err := o.Append(ctx, "conn-a", []byte("b"))
		require.NoError(t, err)

		require.NoError(t, o.Ack(ctx, "conn-a", id1, 7))
		require.NoError(t, o.Ack(ctx, "conn-a", id2, 7), "the same generation acking again must not be treated as stale")

		msgs, err := o.Unacked(ctx, "conn-a")
		require.NoError(t, err)
		require.Empty(t, msgs)
	})

	t.Run("a stale-rejected Ack raises no fencing floor of its own", func(t *testing.T) {
		o := factory(t)
		ctx := context.Background()
		id1, _, err := o.Append(ctx, "conn-a", []byte("a"))
		require.NoError(t, err)
		id2, _, err := o.Append(ctx, "conn-a", []byte("b"))
		require.NoError(t, err)

		require.NoError(t, o.Ack(ctx, "conn-a", id1, 10))
		require.ErrorIs(t, o.Ack(ctx, "conn-a", id2, 4), gateway.ErrStaleOwner)
		// The floor must still be 10, not 4: a later ack at 10 must still succeed.
		require.NoError(t, o.Ack(ctx, "conn-a", id2, 10))

		msgs, err := o.Unacked(ctx, "conn-a")
		require.NoError(t, err)
		require.Empty(t, msgs)
	})

	t.Run("Ack generation fencing is per connection", func(t *testing.T) {
		o := factory(t)
		ctx := context.Background()
		idA, _, err := o.Append(ctx, "conn-a", []byte("a"))
		require.NoError(t, err)
		idB, _, err := o.Append(ctx, "conn-b", []byte("b"))
		require.NoError(t, err)

		require.NoError(t, o.Ack(ctx, "conn-a", idA, 100))
		// conn-b has never seen a generation above 0, so a low generation here must still be
		// accepted: conn-a's floor must not leak onto conn-b.
		require.NoError(t, o.Ack(ctx, "conn-b", idB, 1))
	})

	t.Run("Ack of a connection this Outbox has never appended to is a true no-op regardless of generation", func(t *testing.T) {
		o := factory(t)
		ctx := context.Background()
		require.NoError(t, o.Ack(ctx, "conn-never-seen", "m1", 0))
		require.NoError(t, o.Ack(ctx, "conn-never-seen", "m1", 999), "no prior state means nothing to fence against")

		// Proves no floor or key was created by the acks above: a fresh Append still starts
		// its sequence at 1, exactly as an entirely untouched connection would.
		_, seq, err := o.Append(ctx, "conn-never-seen", []byte("a"))
		require.NoError(t, err)
		require.EqualValues(t, 1, seq)
	})

	t.Run("without a configured lease every Ack passes generation 0 and none are ever stale", func(t *testing.T) {
		// Pins down the zero-cost default: a caller that never configures an owner lease
		// always passes generation 0 (see gateway.Registry.Ack), and 0 can never be rejected
		// as stale against a floor that itself never rises above 0.
		o := factory(t)
		ctx := context.Background()
		id1, _, err := o.Append(ctx, "conn-a", []byte("a"))
		require.NoError(t, err)
		require.NoError(t, o.Ack(ctx, "conn-a", id1, 0))
		id2, _, err := o.Append(ctx, "conn-a", []byte("b"))
		require.NoError(t, err)
		require.NoError(t, o.Ack(ctx, "conn-a", id2, 0))
	})
}
