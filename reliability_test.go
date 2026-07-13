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

// recordingSend returns a send function that appends every delivered payload to a
// slice, plus a snapshot accessor, so tests can assert on what a connection received.
func recordingSend() (func([]byte) error, func() [][]byte) {
	var mu sync.Mutex
	var got [][]byte
	send := func(payload []byte) error {
		mu.Lock()
		defer mu.Unlock()
		cp := make([]byte, len(payload))
		copy(cp, payload)
		got = append(got, cp)
		return nil
	}
	snapshot := func() [][]byte {
		mu.Lock()
		defer mu.Unlock()
		out := make([][]byte, len(got))
		copy(out, got)
		return out
	}
	return send, snapshot
}

// recordingOffline records every OfflineChannel.Deliver call.
type recordingOffline struct {
	mu    sync.Mutex
	calls []offlineCall
}

type offlineCall struct {
	group   string
	payload []byte
}

func (o *recordingOffline) Deliver(_ context.Context, group string, payload []byte) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.calls = append(o.calls, offlineCall{group: group, payload: payload})
	return nil
}

func (o *recordingOffline) snapshot() []offlineCall {
	o.mu.Lock()
	defer o.mu.Unlock()
	out := make([]offlineCall, len(o.calls))
	copy(out, o.calls)
	return out
}

func TestSendToGroupOfflineFallbackWhenNoneOnline(t *testing.T) {
	system := newTestSystem(t)
	offline := &recordingOffline{}
	registry := gateway.NewRegistry(system, log.DiscardLogger,
		gateway.WithPresence(gateway.NewMemoryPresence()),
		gateway.WithOfflineChannel(offline),
	)
	ctx := context.Background()
	t.Cleanup(func() { _ = registry.Close(ctx) })

	result, err := registry.SendToGroup(ctx, "user:absent", []byte("ping"))
	require.NoError(t, err)
	require.True(t, result.None())

	require.Eventually(t, func() bool {
		calls := offline.snapshot()
		return len(calls) == 1 && calls[0].group == "user:absent" && string(calls[0].payload) == "ping"
	}, time.Second, 10*time.Millisecond)
}

func TestSendToGroupNoOfflineFallbackWhenOnline(t *testing.T) {
	system := newTestSystem(t, actor.WithPubSub())
	offline := &recordingOffline{}
	registry := gateway.NewRegistry(system, log.DiscardLogger,
		gateway.WithPresence(gateway.NewMemoryPresence()),
		gateway.WithOfflineChannel(offline),
	)
	ctx := context.Background()
	t.Cleanup(func() { _ = registry.Close(ctx) })

	send, _ := recordingSend()
	require.NoError(t, registry.Register(ctx, "c1", send, gateway.WithConnGroup("user:present")))

	result, err := registry.SendToGroup(ctx, "user:present", []byte("ping"))
	require.NoError(t, err)
	require.Equal(t, 1, result.Delivered)
	require.False(t, result.None())

	// Give any (erroneous) fallback goroutine a chance to run before asserting none did.
	time.Sleep(100 * time.Millisecond)
	require.Empty(t, offline.snapshot())
}

func TestOutboxAtLeastOnceRedeliverAndAck(t *testing.T) {
	system := newTestSystem(t)
	outbox := gateway.NewMemoryOutbox()
	registry := gateway.NewRegistry(system, log.DiscardLogger, gateway.WithOutbox(outbox))
	ctx := context.Background()

	send1, got1 := recordingSend()
	require.NoError(t, registry.Register(ctx, "conn", send1))

	require.NoError(t, registry.SendToConnection(ctx, "conn", []byte("m1")))
	require.Equal(t, [][]byte{[]byte("m1")}, got1())

	// The message is persisted as unacknowledged.
	unacked, err := outbox.Unacked(ctx, "conn")
	require.NoError(t, err)
	require.Len(t, unacked, 1)
	require.Equal(t, []byte("m1"), unacked[0].Payload)
	require.Equal(t, uint64(1), unacked[0].Seq)
	msgID := unacked[0].ID

	// A reconnect before the ack redelivers the unacknowledged tail.
	require.NoError(t, registry.Unregister(ctx, "conn"))
	send2, got2 := recordingSend()
	require.NoError(t, registry.Register(ctx, "conn", send2))
	require.Equal(t, [][]byte{[]byte("m1")}, got2())

	// After the client acks, a further reconnect redelivers nothing.
	require.NoError(t, registry.Ack(ctx, "conn", msgID))
	remaining, err := outbox.Unacked(ctx, "conn")
	require.NoError(t, err)
	require.Empty(t, remaining)

	require.NoError(t, registry.Unregister(ctx, "conn"))
	send3, got3 := recordingSend()
	require.NoError(t, registry.Register(ctx, "conn", send3))
	require.Empty(t, got3())
}

func TestOutboxAckIsNoOpWithoutOutbox(t *testing.T) {
	system := newTestSystem(t)
	registry := gateway.NewRegistry(system, log.DiscardLogger)
	require.NoError(t, registry.Ack(context.Background(), "conn", "whatever"))
}

func TestDisconnectRunsLocalCloseHook(t *testing.T) {
	system := newTestSystem(t)
	registry := gateway.NewRegistry(system, log.DiscardLogger)
	ctx := context.Background()

	var mu sync.Mutex
	var gotReason string
	var called int
	hook := func(reason string) {
		mu.Lock()
		defer mu.Unlock()
		gotReason = reason
		called++
	}

	send, _ := recordingSend()
	require.NoError(t, registry.Register(ctx, "conn", send, gateway.WithConnCloseHook(hook)))

	require.NoError(t, registry.Disconnect(ctx, "conn", "kicked"))
	mu.Lock()
	require.Equal(t, 1, called)
	require.Equal(t, "kicked", gotReason)
	mu.Unlock()

	// An unknown connection is reported as not found.
	require.ErrorIs(t, registry.Disconnect(ctx, "nope", "x"), gateway.ErrConnectionNotFound)
}

func TestDisconnectGroupRunsEveryLocalCloseHook(t *testing.T) {
	system := newTestSystem(t, actor.WithPubSub())
	registry := gateway.NewRegistry(system, log.DiscardLogger)
	ctx := context.Background()

	var mu sync.Mutex
	reasons := map[string]string{}
	makeHook := func(id string) func(string) {
		return func(reason string) {
			mu.Lock()
			defer mu.Unlock()
			reasons[id] = reason
		}
	}

	s1, _ := recordingSend()
	s2, _ := recordingSend()
	require.NoError(t, registry.Register(ctx, "a", s1, gateway.WithConnGroup("g"), gateway.WithConnCloseHook(makeHook("a"))))
	require.NoError(t, registry.Register(ctx, "b", s2, gateway.WithConnGroup("g"), gateway.WithConnCloseHook(makeHook("b"))))

	n, err := registry.DisconnectGroup(ctx, "g", "bye")
	require.NoError(t, err)
	require.Equal(t, 2, n)

	mu.Lock()
	require.Equal(t, "bye", reasons["a"])
	require.Equal(t, "bye", reasons["b"])
	mu.Unlock()
}

func TestWatchPresenceReceivesJoinAndLeave(t *testing.T) {
	system := newTestSystem(t, actor.WithPubSub())
	presence := gateway.NewMemoryPresence()
	registry := gateway.NewRegistry(system, log.DiscardLogger, gateway.WithPresence(presence))
	ctx := context.Background()
	t.Cleanup(func() { _ = registry.Close(ctx) })

	events, cancel, err := registry.WatchPresence(ctx, "team")
	require.NoError(t, err)
	defer cancel()

	send, _ := recordingSend()
	require.NoError(t, registry.Register(ctx, "member", send, gateway.WithConnGroup("team")))

	join := receivePresenceEvent(t, events)
	require.Equal(t, gateway.PresenceJoin, join.Kind)
	require.Equal(t, "member", join.ConnID)
	require.Equal(t, "team", join.Group)

	require.NoError(t, registry.Unregister(ctx, "member"))

	leave := receivePresenceEvent(t, events)
	require.Equal(t, gateway.PresenceLeave, leave.Kind)
	require.Equal(t, "member", leave.ConnID)
}

func TestWatchPresenceUnsupportedWithoutBackend(t *testing.T) {
	system := newTestSystem(t)
	registry := gateway.NewRegistry(system, log.DiscardLogger)
	_, _, err := registry.WatchPresence(context.Background(), "team")
	require.ErrorIs(t, err, gateway.ErrPresenceWatchUnsupported)
}

func TestGroupMembersCarriesMetaFromPresence(t *testing.T) {
	system := newTestSystem(t, actor.WithPubSub())
	presence := gateway.NewMemoryPresence()
	registry := gateway.NewRegistry(system, log.DiscardLogger, gateway.WithPresence(presence))
	ctx := context.Background()
	t.Cleanup(func() { _ = registry.Close(ctx) })

	send, _ := recordingSend()
	require.NoError(t, registry.Register(ctx, "member", send,
		gateway.WithConnGroup("team"),
		gateway.WithConnMeta(map[string]string{"role": "admin"}),
	))

	entries, err := registry.GroupMembers(ctx, "team")
	require.NoError(t, err)
	require.Len(t, entries, 1)
	require.Equal(t, "member", entries[0].ConnID)
	require.Equal(t, "admin", entries[0].Meta["role"])
}

func TestGroupMembersFallsBackToLocalWithoutDirectory(t *testing.T) {
	system := newTestSystem(t, actor.WithPubSub())
	registry := gateway.NewRegistry(system, log.DiscardLogger)
	ctx := context.Background()

	send, _ := recordingSend()
	require.NoError(t, registry.Register(ctx, "member", send,
		gateway.WithConnGroup("team"),
		gateway.WithConnMeta(map[string]string{"role": "guest"}),
	))

	entries, err := registry.GroupMembers(ctx, "team")
	require.NoError(t, err)
	require.Len(t, entries, 1)
	require.Equal(t, "member", entries[0].ConnID)
	require.Equal(t, "guest", entries[0].Meta["role"])
}

// receivePresenceEvent waits for one event or fails the test.
func receivePresenceEvent(t *testing.T, events <-chan gateway.PresenceEvent) gateway.PresenceEvent {
	t.Helper()
	select {
	case ev := <-events:
		return ev
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for a presence event")
		return gateway.PresenceEvent{}
	}
}
