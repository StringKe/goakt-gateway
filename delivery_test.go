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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/tochemey/goakt/v4/actor"
	"github.com/tochemey/goakt/v4/log"

	gateway "github.com/StringKe/goakt-gateway"
)

func TestDeliveryResultTotalAndNone(t *testing.T) {
	require.True(t, gateway.DeliveryResult{}.None())
	require.Equal(t, 0, gateway.DeliveryResult{}.Total())

	dropped := gateway.DeliveryResult{Dropped: 1}
	require.False(t, dropped.None(), "a dropped message still means the identity was reachable")
	require.Equal(t, 1, dropped.Total())

	mixed := gateway.DeliveryResult{Delivered: 2, Dropped: 1, Remote: 3}
	require.Equal(t, 6, mixed.Total())
	require.False(t, mixed.None())
}

// TestDeliveryResultBroadcastCounters verifies that Broadcast's counters distinguish the
// members that took the payload from the ones whose buffer was full.
func TestDeliveryResultBroadcastCounters(t *testing.T) {
	system := newTestSystem(t, actor.WithPubSub())
	observer := newRecordingObserver()
	registry := gateway.NewRegistry(system, log.DiscardLogger, gateway.WithObserver(observer))
	ctx := context.Background()

	healthy := &recorder{}
	slow := &recorder{err: gateway.ErrBackpressure}
	broken := &recorder{err: gateway.ErrConnectionClosed}

	require.NoError(t, registry.Register(ctx, "healthy", healthy.send, gateway.WithConnTopics("room-1")))
	require.NoError(t, registry.Register(ctx, "slow", slow.send, gateway.WithConnTopics("room-1")))
	require.NoError(t, registry.Register(ctx, "broken", broken.send, gateway.WithConnTopics("room-1")))

	result, err := registry.Broadcast(ctx, "room-1", []byte("hi"))
	require.NoError(t, err)
	require.Equal(t, 1, result.Delivered)
	require.Equal(t, 2, result.Dropped, "both the backpressured and the failed connection lost the payload")
	require.Equal(t, 0, result.Remote)
	require.Equal(t, 3, result.Total())

	observer.snapshot(func(o *recordingObserver) {
		require.Equal(t, []string{"slow"}, o.dropped)
		require.Len(t, o.failed, 1)
		require.ErrorIs(t, o.failed[0], gateway.ErrConnectionClosed)
		require.Equal(t, 3, o.fanouts["room-1"])
	})
}
