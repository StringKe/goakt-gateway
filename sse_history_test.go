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
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	gateway "github.com/StringKe/goakt-gateway"
)

func TestMemorySSEHistorySinceReturnsEventsAfterID(t *testing.T) {
	ctx := context.Background()
	history := gateway.NewMemorySSEHistory(10)

	for i := 1; i <= 4; i++ {
		require.NoError(t, history.Append(ctx, "c1", fmt.Sprintf("c1-%d", i), []byte(fmt.Sprintf("e%d", i))))
	}

	events, err := history.Since(ctx, "c1", "c1-2")
	require.NoError(t, err)
	require.Equal(t, []gateway.SSEEvent{
		{ID: "c1-3", Payload: []byte("e3")},
		{ID: "c1-4", Payload: []byte("e4")},
	}, events)

	// the client is already up to date: no events, no gap.
	events, err = history.Since(ctx, "c1", "c1-4")
	require.NoError(t, err)
	require.Empty(t, events)
}

func TestMemorySSEHistorySinceEmptyIDReturnsEverything(t *testing.T) {
	ctx := context.Background()
	history := gateway.NewMemorySSEHistory(10)

	require.NoError(t, history.Append(ctx, "c1", "c1-1", []byte("e1")))
	require.NoError(t, history.Append(ctx, "c1", "c1-2", []byte("e2")))

	events, err := history.Since(ctx, "c1", "")
	require.NoError(t, err)
	require.Len(t, events, 2)
	require.Equal(t, "c1-1", events[0].ID)
	require.Equal(t, "c1-2", events[1].ID)
}

// TestMemorySSEHistoryEvictedIDReportsGap pins the contract for a Last-Event-ID that fell out
// of the ring buffer: everything still retained is returned together with ErrHistoryGap, so
// the caller replays what it can and knows the rest is unrecoverable.
func TestMemorySSEHistoryEvictedIDReportsGap(t *testing.T) {
	ctx := context.Background()
	history := gateway.NewMemorySSEHistory(2)

	for i := 1; i <= 4; i++ {
		require.NoError(t, history.Append(ctx, "c1", fmt.Sprintf("c1-%d", i), []byte(fmt.Sprintf("e%d", i))))
	}

	events, err := history.Since(ctx, "c1", "c1-1")
	require.ErrorIs(t, err, gateway.ErrHistoryGap)
	require.Equal(t, []gateway.SSEEvent{
		{ID: "c1-3", Payload: []byte("e3")},
		{ID: "c1-4", Payload: []byte("e4")},
	}, events)
}

func TestMemorySSEHistoryUnknownConnection(t *testing.T) {
	ctx := context.Background()
	history := gateway.NewMemorySSEHistory(10)

	// nothing was ever retained and nothing is asked for: a fresh client, not a gap.
	events, err := history.Since(ctx, "ghost", "")
	require.NoError(t, err)
	require.Empty(t, events)

	// a client claiming a position in a stream we have no record of is a gap.
	events, err = history.Since(ctx, "ghost", "ghost-3")
	require.ErrorIs(t, err, gateway.ErrHistoryGap)
	require.Empty(t, events)
}

// TestMemorySSEHistoryCopiesPayload guards the buffer ownership rule: the writer reuses its
// payload slice, so the history must not alias it.
func TestMemorySSEHistoryCopiesPayload(t *testing.T) {
	ctx := context.Background()
	history := gateway.NewMemorySSEHistory(10)

	payload := []byte("original")
	require.NoError(t, history.Append(ctx, "c1", "c1-1", payload))
	copy(payload, "MANGLED!")

	events, err := history.Since(ctx, "c1", "")
	require.NoError(t, err)
	require.Len(t, events, 1)
	require.Equal(t, []byte("original"), events[0].Payload)
}

// TestMemorySSEHistoryEvictsLeastRecentlyUsedConnection covers the reclamation strategy:
// buffers cannot be dropped on disconnect (replay happens after a disconnect), so what bounds
// them is the LRU over connections.
func TestMemorySSEHistoryEvictsLeastRecentlyUsedConnection(t *testing.T) {
	ctx := context.Background()
	history := gateway.NewMemorySSEHistory(10, gateway.WithSSEHistoryMaxConnections(2))

	require.NoError(t, history.Append(ctx, "c1", "c1-1", []byte("e1")))
	require.NoError(t, history.Append(ctx, "c2", "c2-1", []byte("e1")))

	// touching c1 makes c2 the least recently used one.
	_, err := history.Since(ctx, "c1", "")
	require.NoError(t, err)

	require.NoError(t, history.Append(ctx, "c3", "c3-1", []byte("e1")))
	require.Equal(t, 2, history.Len())

	events, err := history.Since(ctx, "c1", "")
	require.NoError(t, err)
	require.Len(t, events, 1)

	_, err = history.Since(ctx, "c2", "c2-1")
	require.ErrorIs(t, err, gateway.ErrHistoryGap)
}

func TestMemorySSEHistoryDrop(t *testing.T) {
	ctx := context.Background()
	history := gateway.NewMemorySSEHistory(10)

	require.NoError(t, history.Append(ctx, "c1", "c1-1", []byte("e1")))
	require.Equal(t, 1, history.Len())

	history.Drop("c1")
	require.Equal(t, 0, history.Len())

	_, err := history.Since(ctx, "c1", "c1-1")
	require.ErrorIs(t, err, gateway.ErrHistoryGap)

	// dropping an unknown connection is a no-op.
	history.Drop("c1")
	require.Equal(t, 0, history.Len())
}

// TestMemorySSEHistoryAppendGenerationalAssignsIncreasingSeq pins the sequence counter
// contract: every accepted AppendGenerational call, regardless of the generation it names,
// advances the connection's sequence by exactly one, with no gaps and no repeats.
func TestMemorySSEHistoryAppendGenerationalAssignsIncreasingSeq(t *testing.T) {
	ctx := context.Background()
	history := gateway.NewMemorySSEHistory(10)

	var generational gateway.GenerationalHistory = history

	seq1, err := generational.AppendGenerational(ctx, "c1", "e-1", []byte("one"), 1)
	require.NoError(t, err)
	require.EqualValues(t, 1, seq1)

	seq2, err := generational.AppendGenerational(ctx, "c1", "e-2", []byte("two"), 1)
	require.NoError(t, err)
	require.EqualValues(t, 2, seq2)

	// A later, higher generation is accepted and continues the same counter.
	seq3, err := generational.AppendGenerational(ctx, "c1", "e-3", []byte("three"), 2)
	require.NoError(t, err)
	require.EqualValues(t, 3, seq3)
}

// TestMemorySSEHistoryAppendGenerationalRejectsStaleGeneration proves a write naming a
// generation lower than one already accepted for the connection is rejected outright, not
// recorded, and does not disturb the sequence counter.
func TestMemorySSEHistoryAppendGenerationalRejectsStaleGeneration(t *testing.T) {
	ctx := context.Background()
	history := gateway.NewMemorySSEHistory(10)
	var generational gateway.GenerationalHistory = history

	_, err := generational.AppendGenerational(ctx, "c1", "e-1", []byte("one"), 5)
	require.NoError(t, err)

	_, err = generational.AppendGenerational(ctx, "c1", "e-stale", []byte("stale"), 4)
	require.ErrorIs(t, err, gateway.ErrStaleGeneration)

	// the rejected write must not appear in replay, and the sequence must not have moved.
	events, err := history.Since(ctx, "c1", "")
	require.NoError(t, err)
	require.Equal(t, []gateway.SSEEvent{{ID: "e-1", Payload: []byte("one")}}, events)

	seq, err := generational.AppendGenerational(ctx, "c1", "e-2", []byte("two"), 5)
	require.NoError(t, err)
	require.EqualValues(t, 2, seq, "the stale rejection must not have consumed a sequence number")
}

// TestMemorySSEHistoryAdvanceGenerationFencesSubsequentStaleAppend covers the takeover entry
// point: bumping the generation with no accompanying event still fences the very next
// AppendGenerational call that names the superseded generation.
func TestMemorySSEHistoryAdvanceGenerationFencesSubsequentStaleAppend(t *testing.T) {
	ctx := context.Background()
	history := gateway.NewMemorySSEHistory(10)
	var generational gateway.GenerationalHistory = history

	require.NoError(t, generational.AdvanceGeneration(ctx, "c1", 2))

	// the old owner, still on generation 1, is rejected on its very first write - it never
	// gets to race the new owner to append one more event first.
	_, err := generational.AppendGenerational(ctx, "c1", "e-stale", []byte("stale"), 1)
	require.ErrorIs(t, err, gateway.ErrStaleGeneration)

	// the new owner's write at generation 2 is accepted and continues the sequence.
	seq, err := generational.AppendGenerational(ctx, "c1", "e-1", []byte("one"), 2)
	require.NoError(t, err)
	require.EqualValues(t, 1, seq)

	// AdvanceGeneration never itself appears in replay.
	events, err := history.Since(ctx, "c1", "")
	require.NoError(t, err)
	require.Equal(t, []gateway.SSEEvent{{ID: "e-1", Payload: []byte("one")}}, events)
}

// TestMemorySSEHistoryAdvanceGenerationIsNoOpWhenNotGreater proves AdvanceGeneration is a
// silent no-op, not an error, when generation does not exceed what is already recorded -
// covering both an untouched connection queried indirectly (via a same-generation
// AppendGenerational succeeding) and an explicit regression attempt.
func TestMemorySSEHistoryAdvanceGenerationIsNoOpWhenNotGreater(t *testing.T) {
	ctx := context.Background()
	history := gateway.NewMemorySSEHistory(10)
	var generational gateway.GenerationalHistory = history

	require.NoError(t, generational.AdvanceGeneration(ctx, "c1", 5))
	require.NoError(t, generational.AdvanceGeneration(ctx, "c1", 5), "equal generation must be a no-op, not an error")
	require.NoError(t, generational.AdvanceGeneration(ctx, "c1", 3), "a regression must be a no-op, not an error")

	// generation 5 must still be enforced: a write at 4 is still stale.
	_, err := generational.AppendGenerational(ctx, "c1", "e-stale", []byte("stale"), 4)
	require.ErrorIs(t, err, gateway.ErrStaleGeneration)
}

func TestMemorySSEHistoryConcurrentAccess(t *testing.T) {
	ctx := context.Background()
	history := gateway.NewMemorySSEHistory(8, gateway.WithSSEHistoryMaxConnections(4))

	var wg sync.WaitGroup
	for worker := range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			connID := fmt.Sprintf("c%d", worker%3)
			for i := 1; i <= 50; i++ {
				_ = history.Append(ctx, connID, fmt.Sprintf("%s-%d", connID, i), []byte("payload"))
				_, _ = history.Since(ctx, connID, fmt.Sprintf("%s-%d", connID, i))
				history.Len()
			}
		}()
	}
	wg.Wait()
}
