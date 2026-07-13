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

package redis_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	gateway "github.com/StringKe/goakt-gateway"
	gatewayredis "github.com/StringKe/goakt-gateway/presence/redis"
)

// recvEvent waits for one event on ch or fails the test on timeout, so a broken publish path
// surfaces as a clear failure rather than a hang.
func recvEvent(t *testing.T, ch <-chan gateway.PresenceEvent) gateway.PresenceEvent {
	t.Helper()
	select {
	case event, ok := <-ch:
		require.True(t, ok, "watch channel closed before an event arrived")
		return event
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for a presence event")
		return gateway.PresenceEvent{}
	}
}

// TestRedisPresenceWatchJoinLeave proves a subscriber sees a PresenceJoin when a member
// joins and a PresenceLeave when it leaves, across the Pub/Sub channel.
func TestRedisPresenceWatchJoinLeave(t *testing.T) {
	client := testClient(t)
	ctx := context.Background()

	var counter atomic.Int64
	prefix := uniquePrefix(&counter)
	presence := gatewayredis.NewPresence(client, gatewayredis.WithKeyPrefix(prefix))
	t.Cleanup(func() { _ = client.Del(ctx, memberKey(prefix, "user:1")).Err() })

	events, cancel, err := presence.Watch(ctx, "user:1")
	require.NoError(t, err)
	t.Cleanup(cancel)

	require.NoError(t, presence.Join(ctx, "user:1", "conn-a", time.Minute))
	join := recvEvent(t, events)
	require.Equal(t, gateway.PresenceEvent{Group: "user:1", ConnID: "conn-a", Kind: gateway.PresenceJoin}, join)

	require.NoError(t, presence.Leave(ctx, "user:1", "conn-a"))
	leave := recvEvent(t, events)
	require.Equal(t, gateway.PresenceEvent{Group: "user:1", ConnID: "conn-a", Kind: gateway.PresenceLeave}, leave)
}

// TestRedisPresenceWatchNoDuplicateJoin proves that re-joining a live member and refreshing
// it do not emit a second PresenceJoin: only the effective transitions produce events.
func TestRedisPresenceWatchNoDuplicateJoin(t *testing.T) {
	client := testClient(t)
	ctx := context.Background()

	var counter atomic.Int64
	prefix := uniquePrefix(&counter)
	presence := gatewayredis.NewPresence(client, gatewayredis.WithKeyPrefix(prefix))
	t.Cleanup(func() { _ = client.Del(ctx, memberKey(prefix, "user:1")).Err() })

	events, cancel, err := presence.Watch(ctx, "user:1")
	require.NoError(t, err)
	t.Cleanup(cancel)

	require.NoError(t, presence.Join(ctx, "user:1", "conn-a", time.Minute))
	require.Equal(t, gateway.PresenceJoin, recvEvent(t, events).Kind)

	// A re-join of a live member and a refresh must be silent. Prove it by triggering them and
	// then a leave, and asserting the next event is the leave, not a spurious join.
	require.NoError(t, presence.Join(ctx, "user:1", "conn-a", time.Minute))
	require.NoError(t, presence.Refresh(ctx, "user:1", "conn-a", time.Minute))
	require.NoError(t, presence.Leave(ctx, "user:1", "conn-a"))

	next := recvEvent(t, events)
	require.Equal(t, gateway.PresenceLeave, next.Kind, "a re-join and a refresh of a live member must not emit events")
}

// TestRedisPresenceDirectoryEntries proves Entries returns the metadata recorded through
// JoinWithMeta, and that it comes back cluster-wide: a second Presence over the same Redis
// (standing in for another node) reads the metadata the first one wrote.
func TestRedisPresenceDirectoryEntries(t *testing.T) {
	client := testClient(t)
	ctx := context.Background()

	var counter atomic.Int64
	prefix := uniquePrefix(&counter)
	nodeA := gatewayredis.NewPresence(client, gatewayredis.WithKeyPrefix(prefix))
	nodeB := gatewayredis.NewPresence(client, gatewayredis.WithKeyPrefix(prefix))
	t.Cleanup(func() { _ = client.Del(ctx, memberKey(prefix, "user:1"), metaKey(prefix, "user:1")).Err() })

	meta := map[string]string{"device": "ios", "app": "1.2.3"}
	require.NoError(t, nodeA.JoinWithMeta(ctx, "user:1", "conn-a", meta, time.Minute))
	require.NoError(t, nodeA.Join(ctx, "user:1", "conn-plain", time.Minute))

	entries, err := nodeB.Entries(ctx, "user:1")
	require.NoError(t, err)
	require.Len(t, entries, 2)

	byConn := make(map[string]map[string]string, len(entries))
	for _, entry := range entries {
		byConn[entry.ConnID] = entry.Meta
	}
	require.Equal(t, meta, byConn["conn-a"], "metadata recorded on one node must be visible on another")
	require.Nil(t, byConn["conn-plain"], "a member joined without metadata must report none")
}

// TestRedisPresenceDirectoryDropsLeftMember proves Entries reflects a member that has left:
// its metadata is dropped and it no longer appears, so a stale metadata entry can never
// resurrect a gone connection.
func TestRedisPresenceDirectoryDropsLeftMember(t *testing.T) {
	client := testClient(t)
	ctx := context.Background()

	var counter atomic.Int64
	prefix := uniquePrefix(&counter)
	presence := gatewayredis.NewPresence(client, gatewayredis.WithKeyPrefix(prefix))
	t.Cleanup(func() { _ = client.Del(ctx, memberKey(prefix, "user:1"), metaKey(prefix, "user:1")).Err() })

	require.NoError(t, presence.JoinWithMeta(ctx, "user:1", "conn-a", map[string]string{"k": "v"}, time.Minute))
	require.NoError(t, presence.Leave(ctx, "user:1", "conn-a"))

	entries, err := presence.Entries(ctx, "user:1")
	require.NoError(t, err)
	require.Empty(t, entries)
}

// TestRedisPresenceMetaGroupKeyCollision is adversarial: it proves a group cannot be named
// so that its member sorted set aliases another group's metadata HASH. A naive scheme that
// parked metadata at prefix+"meta:"+group would let a plain Join on a group literally named
// "meta:user:1" ZADD onto the HASH holding group "user:1"'s metadata (a WRONGTYPE clash and
// a cross-group data leak). The disjoint infix namespaces must keep them fully separate.
func TestRedisPresenceMetaGroupKeyCollision(t *testing.T) {
	client := testClient(t)
	ctx := context.Background()

	var counter atomic.Int64
	prefix := uniquePrefix(&counter)
	presence := gatewayredis.NewPresence(client, gatewayredis.WithKeyPrefix(prefix))
	t.Cleanup(func() {
		_ = client.Del(ctx,
			memberKey(prefix, "user:1"), metaKey(prefix, "user:1"),
			memberKey(prefix, "meta:user:1"), memberKey(prefix, "h:{user:1}"),
		).Err()
	})

	meta := map[string]string{"device": "ios"}
	require.NoError(t, presence.JoinWithMeta(ctx, "user:1", "conn-a", meta, time.Minute))

	// A plain Join on groups whose names are crafted to alias group "user:1"'s metadata key -
	// under both a "meta:" suffix scheme and the actual "h:{...}" infix - must each land on an
	// independent member set, not error against or corrupt the metadata HASH.
	require.NoError(t, presence.Join(ctx, "meta:user:1", "conn-x", time.Minute))
	require.NoError(t, presence.Join(ctx, "h:{user:1}", "conn-y", time.Minute))

	members, err := presence.Members(ctx, "meta:user:1")
	require.NoError(t, err)
	require.Equal(t, []string{"conn-x"}, members, "a look-alike group keeps its own members")

	entries, err := presence.Entries(ctx, "user:1")
	require.NoError(t, err)
	require.Len(t, entries, 1)
	require.Equal(t, "conn-a", entries[0].ConnID)
	require.Equal(t, meta, entries[0].Meta, "a look-alike group must not corrupt another group's metadata")
}

// TestRedisPresenceMetaRefreshAcrossNodes proves a Refresh issued by a node that never
// recorded metadata still re-arms the metadata key's TTL, because the lifecycle is driven by
// Redis state and not by any per-process flag. Two independent clients stand in for two
// processes, so node B's local state can never carry node A's "metadata is in use" bit.
func TestRedisPresenceMetaRefreshAcrossNodes(t *testing.T) {
	clientA := testClient(t)
	clientB := secondTestClient(t)
	ctx := context.Background()

	var counter atomic.Int64
	prefix := uniquePrefix(&counter)
	nodeA := gatewayredis.NewPresence(clientA, gatewayredis.WithKeyPrefix(prefix))
	nodeB := gatewayredis.NewPresence(clientB, gatewayredis.WithKeyPrefix(prefix))
	t.Cleanup(func() { _ = clientA.Del(ctx, memberKey(prefix, "user:1"), metaKey(prefix, "user:1")).Err() })

	meta := map[string]string{"device": "ios"}
	require.NoError(t, nodeA.JoinWithMeta(ctx, "user:1", "conn-a", meta, 2*time.Second))

	// Node B refreshes with a far longer lease. If the metadata TTL were gated on node B's
	// (empty) local state it would keep the original ~2s lease and lapse; driven by Redis it
	// must track the new lease.
	require.NoError(t, nodeB.Refresh(ctx, "user:1", "conn-a", time.Hour))

	ttl, err := clientA.PTTL(ctx, metaKey(prefix, "user:1")).Result()
	require.NoError(t, err)
	require.Greater(t, ttl, 30*time.Minute, "a cross-node Refresh must extend the metadata key's TTL, not only the member's")

	entries, err := nodeB.Entries(ctx, "user:1")
	require.NoError(t, err)
	require.Len(t, entries, 1)
	require.Equal(t, meta, entries[0].Meta, "metadata must survive a cross-node refresh")
}

// TestRedisPresenceMetaLeaveAcrossNodes proves a Leave issued by a node that never recorded
// metadata still reclaims it, so a departing connection cannot leak its metadata just because
// the node that observes the leave is not the one that wrote it.
func TestRedisPresenceMetaLeaveAcrossNodes(t *testing.T) {
	clientA := testClient(t)
	clientB := secondTestClient(t)
	ctx := context.Background()

	var counter atomic.Int64
	prefix := uniquePrefix(&counter)
	nodeA := gatewayredis.NewPresence(clientA, gatewayredis.WithKeyPrefix(prefix))
	nodeB := gatewayredis.NewPresence(clientB, gatewayredis.WithKeyPrefix(prefix))
	t.Cleanup(func() { _ = clientA.Del(ctx, memberKey(prefix, "user:1"), metaKey(prefix, "user:1")).Err() })

	require.NoError(t, nodeA.JoinWithMeta(ctx, "user:1", "conn-a", map[string]string{"k": "v"}, time.Minute))

	require.NoError(t, nodeB.Leave(ctx, "user:1", "conn-a"))

	exists, err := clientA.HExists(ctx, metaKey(prefix, "user:1"), "conn-a").Result()
	require.NoError(t, err)
	require.False(t, exists, "a cross-node Leave must delete the metadata another node recorded")

	entries, err := nodeA.Entries(ctx, "user:1")
	require.NoError(t, err)
	require.Empty(t, entries)
}
