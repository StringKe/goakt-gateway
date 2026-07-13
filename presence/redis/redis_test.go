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

// This suite requires a real Redis instance and is skipped unless TEST_REDIS_ADDR is
// set, so CI does not need a Redis daemon by default:
//
//	TEST_REDIS_ADDR=localhost:6379 go test ./presence/redis/...
package redis_test

import (
	"context"
	"encoding/hex"
	"fmt"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"

	gateway "github.com/StringKe/goakt-gateway"
	"github.com/StringKe/goakt-gateway/presence/conformance"
	gatewayredis "github.com/StringKe/goakt-gateway/presence/redis"
)

// testClient dials the Redis instance named by TEST_REDIS_ADDR, skipping the test when it
// is not configured.
func testClient(t *testing.T) *redis.Client {
	t.Helper()

	addr := os.Getenv("TEST_REDIS_ADDR")
	if addr == "" {
		t.Skip("TEST_REDIS_ADDR not set; skipping Redis-backed Presence tests")
	}

	client := redis.NewClient(&redis.Options{Addr: addr})
	t.Cleanup(func() { _ = client.Close() })
	require.NoError(t, client.Ping(context.Background()).Err(), "failed to reach Redis at TEST_REDIS_ADDR")
	return client
}

// secondTestClient dials a second, independent connection to the same Redis instance, so a
// test can stand up two Presence values whose process-local state cannot be shared - the
// faithful way to prove a behaviour is driven by Redis state rather than an in-process map.
// It assumes TEST_REDIS_ADDR is set, which the caller has already checked via testClient.
func secondTestClient(t *testing.T) *redis.Client {
	t.Helper()

	client := redis.NewClient(&redis.Options{Addr: os.Getenv("TEST_REDIS_ADDR")})
	t.Cleanup(func() { _ = client.Close() })
	require.NoError(t, client.Ping(context.Background()).Err(), "failed to reach Redis at TEST_REDIS_ADDR")
	return client
}

// uniquePrefix hands every Presence its own key namespace, so subtests sharing one Redis
// instance never see each other's state.
func uniquePrefix(counter *atomic.Int64) string {
	return fmt.Sprintf("goakt-gateway-test-%d:presence:", counter.Add(1))
}

// hashTag mirrors the package's internal Redis Cluster hash tag encoding (see redis.go's
// hashTag): a fixed "g" prefix plus the group's hex encoding, so the tag body can never be
// empty or contain '{'/'}' regardless of what the group name is.
func hashTag(group string) string { return "g" + hex.EncodeToString([]byte(group)) }

// memberKey, metaKey and generationKey mirror the package's internal key scheme so a test
// can probe the exact Redis keys a group lands on: a group's member sorted set, metadata
// HASH and generation-fencing HASH live in disjoint infix namespaces, each carrying the same
// cluster hash tag.
func memberKey(prefix, group string) string     { return prefix + "m:{" + hashTag(group) + "}" }
func metaKey(prefix, group string) string       { return prefix + "h:{" + hashTag(group) + "}" }
func generationKey(prefix, group string) string { return prefix + "s:{" + hashTag(group) + "}" }

func TestRedisPresenceConformance(t *testing.T) {
	client := testClient(t)

	var counter atomic.Int64
	conformance.Run(t, func(*testing.T) gateway.Presence {
		return gatewayredis.NewPresence(client, gatewayredis.WithKeyPrefix(uniquePrefix(&counter)))
	})
}

// TestRedisPresenceKeyLayout pins the key shape down: one sorted set per group, under the
// configured prefix, whose members are the connection ids.
func TestRedisPresenceKeyLayout(t *testing.T) {
	client := testClient(t)
	ctx := context.Background()

	var counter atomic.Int64
	prefix := uniquePrefix(&counter)
	presence := gatewayredis.NewPresence(client, gatewayredis.WithKeyPrefix(prefix))
	key := memberKey(prefix, "user:1")
	t.Cleanup(func() { _ = client.Del(ctx, key).Err() })

	require.NoError(t, presence.Join(ctx, "user:1", "conn-a", time.Minute))

	kind, err := client.Type(ctx, key).Result()
	require.NoError(t, err)
	require.Equal(t, "zset", kind)

	members, err := client.ZRange(ctx, key, 0, -1).Result()
	require.NoError(t, err)
	require.Equal(t, []string{"conn-a"}, members)
}

// TestRedisPresenceGroupKeyExpires proves the group key is reclaimed by Redis itself once
// its last lease lapses, so a group nobody ever reads again cannot leak. Without this, a
// crashed node's groups would sit in Redis forever.
func TestRedisPresenceGroupKeyExpires(t *testing.T) {
	client := testClient(t)
	ctx := context.Background()

	var counter atomic.Int64
	prefix := uniquePrefix(&counter)
	presence := gatewayredis.NewPresence(client, gatewayredis.WithKeyPrefix(prefix))
	key := memberKey(prefix, "user:1")
	t.Cleanup(func() { _ = client.Del(ctx, key).Err() })

	require.NoError(t, presence.Join(ctx, "user:1", "conn-a", time.Minute))

	ttl, err := client.PTTL(ctx, key).Result()
	require.NoError(t, err)
	require.Positive(t, ttl, "the group key must carry its own TTL")
	require.Greater(t, ttl, time.Minute, "the key TTL must outlive its longest lease by the grace margin")
}

// TestRedisPresenceNeverExpiringMemberPersistsKey covers the other half of the key TTL
// rule: a member with no deadline must strip the key's TTL, or Redis would drop a group
// that is still online.
func TestRedisPresenceNeverExpiringMemberPersistsKey(t *testing.T) {
	client := testClient(t)
	ctx := context.Background()

	var counter atomic.Int64
	prefix := uniquePrefix(&counter)
	presence := gatewayredis.NewPresence(client, gatewayredis.WithKeyPrefix(prefix))
	key := memberKey(prefix, "user:1")
	t.Cleanup(func() { _ = client.Del(ctx, key).Err() })

	require.NoError(t, presence.Join(ctx, "user:1", "conn-a", time.Minute))
	require.NoError(t, presence.Join(ctx, "user:1", "conn-forever", 0))

	ttl, err := client.PTTL(ctx, key).Result()
	require.NoError(t, err)
	require.EqualValues(t, -1, ttl, "a never-expiring member must remove the key's TTL")
}

// TestRedisPresenceSweepsLapsedEntries proves the lazy sweep actually removes the lapsed
// entry from Redis rather than merely filtering it out of the response.
func TestRedisPresenceSweepsLapsedEntries(t *testing.T) {
	client := testClient(t)
	ctx := context.Background()

	var counter atomic.Int64
	prefix := uniquePrefix(&counter)
	presence := gatewayredis.NewPresence(client, gatewayredis.WithKeyPrefix(prefix))
	key := memberKey(prefix, "user:1")
	t.Cleanup(func() { _ = client.Del(ctx, key).Err() })

	require.NoError(t, presence.Join(ctx, "user:1", "conn-dead", 100*time.Millisecond))
	require.NoError(t, presence.Join(ctx, "user:1", "conn-live", time.Minute))

	require.Eventually(t, func() bool {
		members, err := presence.Members(ctx, "user:1")
		return err == nil && len(members) == 1 && members[0] == "conn-live"
	}, 5*time.Second, 20*time.Millisecond)

	raw, err := client.ZRange(ctx, key, 0, -1).Result()
	require.NoError(t, err)
	require.Equal(t, []string{"conn-live"}, raw, "the lapsed member must be gone from the sorted set, not just from the response")
}

// TestRedisPresenceSharesOneBackend is the multi-node case the memory backend cannot cover:
// two Presence values (standing in for two processes) over the same Redis see one another's
// members.
func TestRedisPresenceSharesOneBackend(t *testing.T) {
	client := testClient(t)
	ctx := context.Background()

	var counter atomic.Int64
	prefix := uniquePrefix(&counter)
	nodeA := gatewayredis.NewPresence(client, gatewayredis.WithKeyPrefix(prefix))
	nodeB := gatewayredis.NewPresence(client, gatewayredis.WithKeyPrefix(prefix))
	t.Cleanup(func() { _ = client.Del(ctx, memberKey(prefix, "user:1")).Err() })

	require.NoError(t, nodeA.Join(ctx, "user:1", "conn-a", time.Minute))
	require.NoError(t, nodeB.Join(ctx, "user:1", "conn-b", time.Minute))

	members, err := nodeA.Members(ctx, "user:1")
	require.NoError(t, err)
	require.ElementsMatch(t, []string{"conn-a", "conn-b"}, members)

	online, err := nodeB.Online(ctx, "user:1")
	require.NoError(t, err)
	require.True(t, online)

	require.NoError(t, nodeB.Leave(ctx, "user:1", "conn-b"))

	members, err = nodeA.Members(ctx, "user:1")
	require.NoError(t, err)
	require.Equal(t, []string{"conn-a"}, members)
}

// TestRedisPresencePrefixIsolation proves two deployments sharing one Redis database do not
// see each other's connections.
func TestRedisPresencePrefixIsolation(t *testing.T) {
	client := testClient(t)
	ctx := context.Background()

	var counter atomic.Int64
	prefixA := uniquePrefix(&counter)
	prefixB := uniquePrefix(&counter)
	deploymentA := gatewayredis.NewPresence(client, gatewayredis.WithKeyPrefix(prefixA))
	deploymentB := gatewayredis.NewPresence(client, gatewayredis.WithKeyPrefix(prefixB))
	t.Cleanup(func() { _ = client.Del(ctx, memberKey(prefixA, "user:1"), memberKey(prefixB, "user:1")).Err() })

	require.NoError(t, deploymentA.Join(ctx, "user:1", "conn-a", time.Minute))

	online, err := deploymentB.Online(ctx, "user:1")
	require.NoError(t, err)
	require.False(t, online, "a different key prefix must not observe another deployment's members")
}
