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
	"sort"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/tochemey/goakt/v4/actor"
	"github.com/tochemey/goakt/v4/log"

	gateway "github.com/StringKe/goakt-gateway"
)

// recorder is a test send function that captures every payload written to it.
type recorder struct {
	mu       sync.Mutex
	payloads [][]byte
	err      error
}

func (r *recorder) send(payload []byte) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.err != nil {
		return r.err
	}
	r.payloads = append(r.payloads, payload)
	return nil
}

func (r *recorder) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.payloads)
}

// recordingObserver captures the Registry events a test cares about.
type recordingObserver struct {
	mu           sync.Mutex
	registered   []string
	unregistered []string
	replaced     []string
	dropped      []string
	failed       []error
	fanouts      map[string]int
}

var _ gateway.Observer = (*recordingObserver)(nil)

func newRecordingObserver() *recordingObserver {
	return &recordingObserver{fanouts: map[string]int{}}
}

func (o *recordingObserver) ConnectionRegistered(id, _ string) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.registered = append(o.registered, id)
}

func (o *recordingObserver) ConnectionUnregistered(id, _ string) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.unregistered = append(o.unregistered, id)
}

func (o *recordingObserver) ConnectionReplaced(id, _ string) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.replaced = append(o.replaced, id)
}

func (o *recordingObserver) DeliveryDropped(id, _ string) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.dropped = append(o.dropped, id)
}

func (o *recordingObserver) DeliveryFailed(_ string, err error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.failed = append(o.failed, err)
}

func (o *recordingObserver) BroadcastFanout(topic string, localMembers int) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.fanouts[topic] = localMembers
}

func (o *recordingObserver) snapshot(f func(*recordingObserver)) {
	o.mu.Lock()
	defer o.mu.Unlock()
	f(o)
}

// TestRegistryTakeover pins the reconnect semantics: registering an id that is already
// held, with WithReplaceExisting, must evict the old connection and leave exactly one
// registration standing - the new one.
func TestRegistryTakeover(t *testing.T) {
	system := newTestSystem(t, actor.WithPubSub())
	observer := newRecordingObserver()
	registry := gateway.NewRegistry(system, log.DiscardLogger, gateway.WithObserver(observer))
	ctx := context.Background()

	old := &recorder{}
	fresh := &recorder{}

	require.NoError(t, registry.Register(ctx, "device-1", old.send, gateway.WithConnGroup("user:1")))
	require.NoError(t, registry.Register(ctx, "device-1", fresh.send,
		gateway.WithConnGroup("user:1"),
		gateway.WithReplaceExisting(),
	))

	require.True(t, registry.Has("device-1"))
	require.Equal(t, 1, registry.Len())
	require.Equal(t, []string{"device-1"}, registry.LocalConnectionsOf("user:1"))

	require.NoError(t, registry.SendToConnection(ctx, "device-1", []byte("after-takeover")))
	require.Equal(t, 0, old.count(), "the evicted connection must receive nothing")
	require.Equal(t, 1, fresh.count())

	observer.snapshot(func(o *recordingObserver) {
		require.Equal(t, []string{"device-1"}, o.replaced)
		require.Equal(t, []string{"device-1"}, o.unregistered)
		require.Equal(t, []string{"device-1", "device-1"}, o.registered)
	})
}

// TestRegistryTakeoverRequiresOptIn verifies that a duplicate id without
// WithReplaceExisting still fails, and leaves the original connection untouched.
func TestRegistryTakeoverRequiresOptIn(t *testing.T) {
	system := newTestSystem(t, actor.WithPubSub())
	registry := gateway.NewRegistry(system, log.DiscardLogger)
	ctx := context.Background()

	old := &recorder{}
	fresh := &recorder{}

	require.NoError(t, registry.Register(ctx, "device-1", old.send, gateway.WithConnGroup("user:1")))
	err := registry.Register(ctx, "device-1", fresh.send, gateway.WithConnGroup("user:1"))
	require.ErrorIs(t, err, gateway.ErrConnectionExists)

	require.NoError(t, registry.SendToConnection(ctx, "device-1", []byte("still-mine")))
	require.Equal(t, 1, old.count())
	require.Equal(t, 0, fresh.count())
}

// TestRegistrySendToGroupLocalFanout covers the multi-device case: one identity, several
// sockets on this node, one SendToGroup reaching all of them exactly once, and none of the
// sockets of any other identity.
func TestRegistrySendToGroupLocalFanout(t *testing.T) {
	system := newTestSystem(t, actor.WithPubSub())
	registry := gateway.NewRegistry(system, log.DiscardLogger)
	ctx := context.Background()

	phone := &recorder{}
	laptop := &recorder{}
	stranger := &recorder{}

	require.NoError(t, registry.Register(ctx, "phone", phone.send, gateway.WithConnGroup("user:1")))
	require.NoError(t, registry.Register(ctx, "laptop", laptop.send, gateway.WithConnGroup("user:1")))
	require.NoError(t, registry.Register(ctx, "stranger", stranger.send, gateway.WithConnGroup("user:2")))

	result, err := registry.SendToGroup(ctx, "user:1", []byte("ping"))
	require.NoError(t, err)
	require.Equal(t, 2, result.Delivered)
	require.Equal(t, 0, result.Dropped)
	require.Equal(t, 0, result.Remote, "a non-clustered registry has nowhere to fan out to")
	require.Equal(t, 2, result.Total())
	require.False(t, result.None())

	require.Equal(t, 1, phone.count())
	require.Equal(t, 1, laptop.count())
	require.Equal(t, 0, stranger.count())

	members := registry.LocalConnectionsOf("user:1")
	sort.Strings(members)
	require.Equal(t, []string{"laptop", "phone"}, members)
}

// TestRegistrySendToGroupUnknownGroup verifies the signal an application relies on to fall
// back to an offline channel: nothing local, nothing remote, DeliveryResult.None.
func TestRegistrySendToGroupUnknownGroup(t *testing.T) {
	system := newTestSystem(t, actor.WithPubSub())
	registry := gateway.NewRegistry(system, log.DiscardLogger)

	result, err := registry.SendToGroup(context.Background(), "user:nobody", []byte("ping"))
	require.NoError(t, err)
	require.True(t, result.None())
}

// TestRegistrySendToGroupBackpressure verifies that a group member whose outbound buffer
// is full is counted as dropped rather than delivered, and reported to the Observer.
func TestRegistrySendToGroupBackpressure(t *testing.T) {
	system := newTestSystem(t, actor.WithPubSub())
	observer := newRecordingObserver()
	registry := gateway.NewRegistry(system, log.DiscardLogger, gateway.WithObserver(observer))
	ctx := context.Background()

	healthy := &recorder{}
	slow := &recorder{err: gateway.ErrBackpressure}

	require.NoError(t, registry.Register(ctx, "healthy", healthy.send, gateway.WithConnGroup("user:1")))
	require.NoError(t, registry.Register(ctx, "slow", slow.send, gateway.WithConnGroup("user:1")))

	result, err := registry.SendToGroup(ctx, "user:1", []byte("ping"))
	require.NoError(t, err)
	require.Equal(t, 1, result.Delivered)
	require.Equal(t, 1, result.Dropped)
	require.Equal(t, 2, result.Total())

	observer.snapshot(func(o *recordingObserver) {
		require.Equal(t, []string{"slow"}, o.dropped)
	})
}

// TestRegistryBroadcastWithExclude verifies that the sender's own connection can be kept
// out of a broadcast it triggered.
func TestRegistryBroadcastWithExclude(t *testing.T) {
	system := newTestSystem(t, actor.WithPubSub())
	observer := newRecordingObserver()
	registry := gateway.NewRegistry(system, log.DiscardLogger, gateway.WithObserver(observer))
	ctx := context.Background()

	sender := &recorder{}
	peer := &recorder{}

	require.NoError(t, registry.Register(ctx, "sender", sender.send, gateway.WithConnTopics("room-1")))
	require.NoError(t, registry.Register(ctx, "peer", peer.send, gateway.WithConnTopics("room-1")))

	result, err := registry.Broadcast(ctx, "room-1", []byte("hi"), gateway.WithExclude("sender"))
	require.NoError(t, err)
	require.Equal(t, 1, result.Delivered)
	require.Equal(t, 0, result.Dropped)

	require.Equal(t, 0, sender.count(), "an excluded connection must not receive the broadcast")
	require.Equal(t, 1, peer.count())

	observer.snapshot(func(o *recordingObserver) {
		require.Equal(t, 1, o.fanouts["room-1"], "the excluded member must not count towards the local fan-out")
	})
}

// TestRegistryJoinWithoutPubSubFails verifies that a topic whose cluster bridge cannot be
// established is reported to the caller instead of leaving the connection silently unable
// to receive broadcasts.
func TestRegistryJoinWithoutPubSubFails(t *testing.T) {
	system := newTestSystem(t)
	registry := gateway.NewRegistry(system, log.DiscardLogger)
	ctx := context.Background()

	require.NoError(t, registry.Register(ctx, "conn-1", func([]byte) error { return nil }))

	err := registry.Join(ctx, "conn-1", "room-1")
	require.ErrorIs(t, err, gateway.ErrPubSubUnavailable)
}

// TestRegistryRegisterRollsBackOnTopicFailure verifies that a registration that cannot be
// completed leaves nothing behind: no half-wired connection in the table.
func TestRegistryRegisterRollsBackOnTopicFailure(t *testing.T) {
	system := newTestSystem(t)
	registry := gateway.NewRegistry(system, log.DiscardLogger)
	ctx := context.Background()

	err := registry.Register(ctx, "conn-1", func([]byte) error { return nil }, gateway.WithConnTopics("room-1"))
	require.ErrorIs(t, err, gateway.ErrPubSubUnavailable)
	require.False(t, registry.Has("conn-1"))
	require.Equal(t, 0, registry.Len())
}
