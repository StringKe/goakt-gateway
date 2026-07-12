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
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/tochemey/goakt/v4/actor"
	"github.com/tochemey/goakt/v4/log"

	gateway "github.com/StringKe/goakt-gateway"
)

func TestRegistrySendToConnectionLocalHit(t *testing.T) {
	system := newTestSystem(t)
	registry := gateway.NewRegistry(system, log.DiscardLogger)

	var mu sync.Mutex
	var received [][]byte
	send := func(payload []byte) error {
		mu.Lock()
		defer mu.Unlock()
		received = append(received, payload)
		return nil
	}

	ctx := context.Background()
	require.NoError(t, registry.Register(ctx, "conn-1", send))
	require.True(t, registry.Has("conn-1"))

	require.NoError(t, registry.SendToConnection(ctx, "conn-1", []byte("hello")))

	mu.Lock()
	require.Len(t, received, 1)
	require.Equal(t, []byte("hello"), received[0])
	mu.Unlock()

	require.NoError(t, registry.Unregister(ctx, "conn-1"))
	require.False(t, registry.Has("conn-1"))
}

func TestRegistrySendToConnectionMiss(t *testing.T) {
	system := newTestSystem(t)
	registry := gateway.NewRegistry(system, log.DiscardLogger)

	err := registry.SendToConnection(context.Background(), "does-not-exist", []byte("hello"))
	require.ErrorIs(t, err, gateway.ErrConnectionNotFound)
}

func TestRegistryRegisterTwiceFails(t *testing.T) {
	system := newTestSystem(t)
	registry := gateway.NewRegistry(system, log.DiscardLogger)

	send := func([]byte) error { return nil }
	ctx := context.Background()
	require.NoError(t, registry.Register(ctx, "dup", send))

	err := registry.Register(ctx, "dup", send)
	require.ErrorIs(t, err, gateway.ErrConnectionExists)

	require.NoError(t, registry.Unregister(ctx, "dup"))
}

func TestRegistryBroadcastLocalMembersOnly(t *testing.T) {
	system := newTestSystem(t, actor.WithPubSub())
	registry := gateway.NewRegistry(system, log.DiscardLogger)
	ctx := context.Background()

	var mu sync.Mutex
	received := map[string]int{}
	makeSend := func(id string) func([]byte) error {
		return func([]byte) error {
			mu.Lock()
			defer mu.Unlock()
			received[id]++
			return nil
		}
	}

	require.NoError(t, registry.Register(ctx, "a", makeSend("a"), "room-1"))
	require.NoError(t, registry.Register(ctx, "b", makeSend("b"), "room-1"))
	require.NoError(t, registry.Register(ctx, "c", makeSend("c")))

	require.NoError(t, registry.Broadcast(ctx, "room-1", []byte("hi")))

	time.Sleep(300 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, 1, received["a"])
	require.Equal(t, 1, received["b"])
	require.Equal(t, 0, received["c"])
}

func TestRegistryJoinLeave(t *testing.T) {
	system := newTestSystem(t, actor.WithPubSub())
	registry := gateway.NewRegistry(system, log.DiscardLogger)
	ctx := context.Background()

	var count int
	var mu sync.Mutex
	send := func([]byte) error {
		mu.Lock()
		count++
		mu.Unlock()
		return nil
	}

	require.NoError(t, registry.Register(ctx, "leaver", send))
	require.NoError(t, registry.Join(ctx, "leaver", "topic-x"))

	require.NoError(t, registry.Broadcast(ctx, "topic-x", []byte("one")))
	time.Sleep(200 * time.Millisecond)

	require.NoError(t, registry.Leave(ctx, "leaver", "topic-x"))
	require.NoError(t, registry.Broadcast(ctx, "topic-x", []byte("two")))
	time.Sleep(200 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, 1, count)
}

func TestRegistryJoinUnknownConnection(t *testing.T) {
	system := newTestSystem(t)
	registry := gateway.NewRegistry(system, log.DiscardLogger)

	err := registry.Join(context.Background(), "ghost", "topic")
	require.ErrorIs(t, err, gateway.ErrConnectionNotFound)
}

// TestRegistrySendToConnection_Backpressure mirrors the bounded-outbound-channel pattern
// the WS/SSE handlers use for their send closures: with a buffer of 1 and nothing
// draining it, a second send must report ErrBackpressure instead of blocking forever.
func TestRegistrySendToConnection_Backpressure(t *testing.T) {
	system := newTestSystem(t)
	registry := gateway.NewRegistry(system, log.DiscardLogger)
	ctx := context.Background()

	outbound := make(chan []byte, 1)
	send := func(payload []byte) error {
		select {
		case outbound <- payload:
			return nil
		default:
			return gateway.ErrBackpressure
		}
	}

	require.NoError(t, registry.Register(ctx, "backpressured", send))

	require.NoError(t, registry.SendToConnection(ctx, "backpressured", []byte("first")))
	err := registry.SendToConnection(ctx, "backpressured", []byte("second"))
	require.ErrorIs(t, err, gateway.ErrBackpressure)
}

// TestRegistrySendToConnectionClosedPropagatesError verifies that SendToConnection
// forwards whatever error a locally registered connection's own send function reports -
// in particular ErrConnectionClosed, the error the WS/SSE handlers' send closures return
// once their socket has already torn down but before Unregister has run.
func TestRegistrySendToConnectionClosedPropagatesError(t *testing.T) {
	system := newTestSystem(t)
	registry := gateway.NewRegistry(system, log.DiscardLogger)
	ctx := context.Background()

	send := func([]byte) error { return gateway.ErrConnectionClosed }
	require.NoError(t, registry.Register(ctx, "closed-right-after", send))

	err := registry.SendToConnection(ctx, "closed-right-after", []byte("hello"))
	require.ErrorIs(t, err, gateway.ErrConnectionClosed)
}

// TestRegistryBroadcastZeroMembers verifies that Broadcast on a topic nobody has joined
// is a no-op: no panic from ranging over the topic's (absent) membership set, and no
// delivery attempted anywhere.
func TestRegistryBroadcastZeroMembers(t *testing.T) {
	system := newTestSystem(t)
	registry := gateway.NewRegistry(system, log.DiscardLogger)

	require.NotPanics(t, func() {
		err := registry.Broadcast(context.Background(), "nobody-home", []byte("hello"))
		require.NoError(t, err)
	})
}
