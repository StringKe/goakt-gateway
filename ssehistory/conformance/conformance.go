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

// Package conformance is a shared test suite every gateway.SSEHistory implementation must
// pass, so gateway.MemorySSEHistory and ssehistory/redis.History (and any third-party
// implementation) are held to the exact same Last-Event-ID contract - most importantly the
// three Since cases, because an implementation that reports a gap where there is none (or
// hides a real gap) makes EventSource reconnect either replay nothing or silently skip
// events the client never saw.
package conformance

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	gateway "github.com/StringKe/goakt-gateway"
)

// Run exercises newHistory() against the gateway.SSEHistory contract. newHistory must return
// a fresh, empty history that retains at least 8 events per connection; the suite never
// appends more than that to a single connection, so it asserts ordering and the three Since
// cases without depending on any implementation's exact per-connection cap. The overflow
// eviction bound is implementation-specific and is covered by each backend's own tests, not
// here.
func Run(t *testing.T, newHistory func() gateway.SSEHistory) {
	t.Helper()

	t.Run("Since empty on an unknown connection returns nothing and no error", func(t *testing.T) {
		h := newHistory()
		events, err := h.Since(context.Background(), "conn-absent", "")
		require.NoError(t, err)
		require.Empty(t, events)
	})

	t.Run("Since with a Last-Event-ID on an unknown connection reports a gap", func(t *testing.T) {
		// A reconnect that names an id the backend has never heard of must surface a gap so
		// the client learns earlier events are unrecoverable, not silently get nothing.
		h := newHistory()
		events, err := h.Since(context.Background(), "conn-absent", "e-1")
		require.ErrorIs(t, err, gateway.ErrHistoryGap)
		require.Empty(t, events)
	})

	t.Run("Since empty returns every retained event in wire order", func(t *testing.T) {
		h := newHistory()
		ctx := context.Background()
		require.NoError(t, h.Append(ctx, "conn-1", "e-1", []byte("one")))
		require.NoError(t, h.Append(ctx, "conn-1", "e-2", []byte("two")))
		require.NoError(t, h.Append(ctx, "conn-1", "e-3", []byte("three")))

		events, err := h.Since(ctx, "conn-1", "")
		require.NoError(t, err)
		require.Equal(t, []gateway.SSEEvent{
			{ID: "e-1", Payload: []byte("one")},
			{ID: "e-2", Payload: []byte("two")},
			{ID: "e-3", Payload: []byte("three")},
		}, events)
	})

	t.Run("Since a known id returns only the events after it", func(t *testing.T) {
		h := newHistory()
		ctx := context.Background()
		require.NoError(t, h.Append(ctx, "conn-1", "e-1", []byte("one")))
		require.NoError(t, h.Append(ctx, "conn-1", "e-2", []byte("two")))
		require.NoError(t, h.Append(ctx, "conn-1", "e-3", []byte("three")))

		events, err := h.Since(ctx, "conn-1", "e-1")
		require.NoError(t, err)
		require.Equal(t, []gateway.SSEEvent{
			{ID: "e-2", Payload: []byte("two")},
			{ID: "e-3", Payload: []byte("three")},
		}, events)
	})

	t.Run("Since the latest id returns nothing and no error", func(t *testing.T) {
		h := newHistory()
		ctx := context.Background()
		require.NoError(t, h.Append(ctx, "conn-1", "e-1", []byte("one")))
		require.NoError(t, h.Append(ctx, "conn-1", "e-2", []byte("two")))

		events, err := h.Since(ctx, "conn-1", "e-2")
		require.NoError(t, err)
		require.Empty(t, events)
	})

	t.Run("Since an unknown id returns everything retained plus a gap", func(t *testing.T) {
		h := newHistory()
		ctx := context.Background()
		require.NoError(t, h.Append(ctx, "conn-1", "e-1", []byte("one")))
		require.NoError(t, h.Append(ctx, "conn-1", "e-2", []byte("two")))

		events, err := h.Since(ctx, "conn-1", "e-unknown")
		require.ErrorIs(t, err, gateway.ErrHistoryGap)
		require.Equal(t, []gateway.SSEEvent{
			{ID: "e-1", Payload: []byte("one")},
			{ID: "e-2", Payload: []byte("two")},
		}, events)
	})

	t.Run("Append copies the payload so a later mutation of the caller's slice is not observed", func(t *testing.T) {
		h := newHistory()
		ctx := context.Background()
		payload := []byte("original")
		require.NoError(t, h.Append(ctx, "conn-1", "e-1", payload))

		// The caller owns its buffer and may reuse it after Append returns.
		for i := range payload {
			payload[i] = 'x'
		}

		events, err := h.Since(ctx, "conn-1", "")
		require.NoError(t, err)
		require.Equal(t, []gateway.SSEEvent{{ID: "e-1", Payload: []byte("original")}}, events)
	})

	t.Run("a payload of arbitrary bytes round-trips intact", func(t *testing.T) {
		// Newlines and NULs are exactly what a delimiter-based encoding would corrupt.
		h := newHistory()
		ctx := context.Background()
		payload := []byte{0x00, '\n', 0xff, '\r', 0x7f, '{', '}', ':'}
		require.NoError(t, h.Append(ctx, "conn-1", "e-1", payload))

		events, err := h.Since(ctx, "conn-1", "")
		require.NoError(t, err)
		require.Equal(t, []gateway.SSEEvent{{ID: "e-1", Payload: payload}}, events)
	})

	t.Run("connections are isolated from one another", func(t *testing.T) {
		h := newHistory()
		ctx := context.Background()
		require.NoError(t, h.Append(ctx, "conn-1", "e-1", []byte("a")))
		require.NoError(t, h.Append(ctx, "conn-2", "e-1", []byte("b")))

		events, err := h.Since(ctx, "conn-1", "")
		require.NoError(t, err)
		require.Equal(t, []gateway.SSEEvent{{ID: "e-1", Payload: []byte("a")}}, events)

		events, err = h.Since(ctx, "conn-2", "")
		require.NoError(t, err)
		require.Equal(t, []gateway.SSEEvent{{ID: "e-1", Payload: []byte("b")}}, events)
	})

	t.Run("concurrent Append and Since are safe", func(t *testing.T) {
		// Guards against data races; run under -race. Each connection is written by exactly
		// one goroutine (as the real writer goroutine per connection is), while others read.
		h := newHistory()
		ctx := context.Background()

		const connections = 16
		var wg sync.WaitGroup
		for i := range connections {
			wg.Go(func() {
				connID := fmt.Sprintf("conn-%d", i)
				for j := range 8 {
					require.NoError(t, h.Append(ctx, connID, fmt.Sprintf("e-%d", j), []byte("payload")))
				}
			})
			wg.Go(func() {
				connID := fmt.Sprintf("conn-%d", i)
				for range 8 {
					_, err := h.Since(ctx, connID, "")
					require.NoError(t, err)
				}
			})
		}
		wg.Wait()

		for i := range connections {
			connID := fmt.Sprintf("conn-%d", i)
			events, err := h.Since(ctx, connID, "")
			require.NoError(t, err)
			require.Len(t, events, 8, "every appended event on a fully written connection must survive")
		}
	})

	t.Run("Since returns a fresh slice the caller may retain across further Appends", func(t *testing.T) {
		// Once a caller holds a Since result, a later Append must not mutate it out from under
		// them - the two backends must behave identically here.
		h := newHistory()
		ctx := context.Background()
		require.NoError(t, h.Append(ctx, "conn-1", "e-1", []byte("one")))

		snapshot, err := h.Since(ctx, "conn-1", "")
		require.NoError(t, err)
		require.NoError(t, h.Append(ctx, "conn-1", "e-2", []byte("two")))

		require.Equal(t, []gateway.SSEEvent{{ID: "e-1", Payload: []byte("one")}}, snapshot)
	})
}
