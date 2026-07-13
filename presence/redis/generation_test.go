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

// This suite requires a real Redis instance and is skipped unless TEST_REDIS_ADDR is set;
// see redis_test.go's testClient for the same convention every other suite in this package
// follows.
package redis_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"

	gateway "github.com/StringKe/goakt-gateway"
	gatewayredis "github.com/StringKe/goakt-gateway/presence/redis"
)

// TestRedisPresenceRefreshGenerationRejectsStaleGeneration proves the core generation-fencing
// promise: a Refresh carrying a generation lower than one already recorded (by a later
// takeover) is rejected rather than silently re-recording the member, which would let a
// delayed refresh from a node whose owner lease a takeover has already superseded resurrect
// membership state a newer owner has moved past.
func TestRedisPresenceRefreshGenerationRejectsStaleGeneration(t *testing.T) {
	client := testClient(t)
	ctx := context.Background()

	var counter atomic.Int64
	prefix := uniquePrefix(&counter)
	presence := gatewayredis.NewPresence(client, gatewayredis.WithKeyPrefix(prefix))
	t.Cleanup(func() {
		_ = client.Del(ctx, memberKey(prefix, "user:1"), generationKey(prefix, "user:1")).Err()
	})

	// Generation 2 (the "new owner") records a refresh first, establishing the watermark.
	require.NoError(t, presence.RefreshGeneration(ctx, "user:1", "conn-a", 2, time.Minute))

	// Generation 1 (the "stale, superseded owner") retries a delayed refresh. It must be
	// rejected, and it must not touch the member's lease at all.
	err := presence.RefreshGeneration(ctx, "user:1", "conn-a", 1, time.Minute)
	require.ErrorIs(t, err, gatewayredis.ErrStaleGeneration)

	// A later generation must still be able to proceed normally.
	require.NoError(t, presence.RefreshGeneration(ctx, "user:1", "conn-a", 3, time.Minute))
}

// TestRedisPresenceLeaveGenerationRejectsStaleGeneration proves the same fencing rule for
// Leave: a delayed leave carrying a superseded generation must not be able to remove a
// membership record a newer owner (re)established since.
func TestRedisPresenceLeaveGenerationRejectsStaleGeneration(t *testing.T) {
	client := testClient(t)
	ctx := context.Background()

	var counter atomic.Int64
	prefix := uniquePrefix(&counter)
	presence := gatewayredis.NewPresence(client, gatewayredis.WithKeyPrefix(prefix))
	t.Cleanup(func() {
		_ = client.Del(ctx, memberKey(prefix, "user:1"), generationKey(prefix, "user:1")).Err()
	})

	require.NoError(t, presence.RefreshGeneration(ctx, "user:1", "conn-a", 5, time.Minute))

	// A stale leave at generation 4 (superseded by the generation-5 refresh above) must not
	// remove the member.
	err := presence.LeaveGeneration(ctx, "user:1", "conn-a", 4)
	require.ErrorIs(t, err, gatewayredis.ErrStaleGeneration)

	members, err := presence.Members(ctx, "user:1")
	require.NoError(t, err)
	require.Equal(t, []string{"conn-a"}, members, "a stale-generation leave must not remove the member")

	// The current generation can still leave normally.
	require.NoError(t, presence.LeaveGeneration(ctx, "user:1", "conn-a", 5))
	members, err = presence.Members(ctx, "user:1")
	require.NoError(t, err)
	require.Empty(t, members)
}

// TestRedisPresenceRefreshGenAndLeaveGenSatisfyGatewayPresenceFencer proves *Presence adapts
// onto gateway.PresenceFencer (the root package's name/error contract for this capability, used
// by a future Registry type-assertion) rather than only exposing its own differently-named
// RefreshGeneration/LeaveGeneration: RefreshGen/LeaveGen must exist with PresenceFencer's exact
// signatures and translate this package's ErrStaleGeneration to gateway.ErrStaleOwner, the
// sentinel PresenceFencer's own doc comment promises callers.
func TestRedisPresenceRefreshGenAndLeaveGenSatisfyGatewayPresenceFencer(t *testing.T) {
	client := testClient(t)
	ctx := context.Background()

	var counter atomic.Int64
	prefix := uniquePrefix(&counter)
	presence := gatewayredis.NewPresence(client, gatewayredis.WithKeyPrefix(prefix))
	t.Cleanup(func() {
		_ = client.Del(ctx, memberKey(prefix, "user:1"), generationKey(prefix, "user:1")).Err()
	})

	var fencer gateway.PresenceFencer = presence

	require.NoError(t, fencer.RefreshGen(ctx, "user:1", "conn-a", 2, time.Minute))

	err := fencer.RefreshGen(ctx, "user:1", "conn-a", 1, time.Minute)
	require.ErrorIs(t, err, gateway.ErrStaleOwner)
	require.NotErrorIs(t, err, gatewayredis.ErrStaleGeneration,
		"PresenceFencer callers should not need to know this backend's own error type")

	err = fencer.LeaveGen(ctx, "user:1", "conn-a", 1)
	require.ErrorIs(t, err, gateway.ErrStaleOwner)

	require.NoError(t, fencer.LeaveGen(ctx, "user:1", "conn-a", 2))
	members, err := presence.Members(ctx, "user:1")
	require.NoError(t, err)
	require.Empty(t, members)
}

// TestRedisPresenceLeaveRearmsTTLAfterPermanentMemberLeaves is the "permanent lease residue"
// reproduction and fix proof (see leaveScript's TTL recompute): a never-expiring member
// PERSISTs the group's keys (removes their TTL). Without leaveScript recomputing the TTL on
// removal, once that permanent member leaves, the key stays PERSISTed forever even though
// every remaining member has a normal, finite lease - breaking the promise that a group
// nobody ever reads again is still reclaimed by Redis.
func TestRedisPresenceLeaveRearmsTTLAfterPermanentMemberLeaves(t *testing.T) {
	client := testClient(t)
	ctx := context.Background()

	var counter atomic.Int64
	prefix := uniquePrefix(&counter)
	presence := gatewayredis.NewPresence(client, gatewayredis.WithKeyPrefix(prefix))
	key := memberKey(prefix, "user:1")
	t.Cleanup(func() { _ = client.Del(ctx, key, metaKey(prefix, "user:1")).Err() })

	require.NoError(t, presence.Join(ctx, "user:1", "conn-finite", time.Minute))
	require.NoError(t, presence.Join(ctx, "user:1", "conn-forever", 0))

	ttl, err := client.PTTL(ctx, key).Result()
	require.NoError(t, err)
	require.EqualValues(t, -1, ttl, "the permanent member must strip the key's TTL")

	require.NoError(t, presence.Leave(ctx, "user:1", "conn-forever"))

	ttl, err = client.PTTL(ctx, key).Result()
	require.NoError(t, err)
	require.Positive(t, ttl, "once the permanent member leaves, the key must carry a TTL again, derived from the remaining finite member")
	require.LessOrEqual(t, ttl, time.Minute+30*time.Second, "the recomputed TTL must reflect the remaining member's actual lease, not linger unbounded")
}

// TestRedisPresenceMembersSweepReclaimsOrphanMetadata is the orphan-metadata reproduction and
// fix proof: a member that simply times out (the common case - a crashed node, no explicit
// Leave ever arrives) must have its metadata reclaimed by the same lazy sweep that drops it
// from the member set, not leak forever in the metadata HASH.
func TestRedisPresenceMembersSweepReclaimsOrphanMetadata(t *testing.T) {
	client := testClient(t)
	ctx := context.Background()

	var counter atomic.Int64
	prefix := uniquePrefix(&counter)
	presence := gatewayredis.NewPresence(client, gatewayredis.WithKeyPrefix(prefix))
	meta := metaKey(prefix, "user:1")
	t.Cleanup(func() { _ = client.Del(ctx, memberKey(prefix, "user:1"), meta).Err() })

	require.NoError(t, presence.JoinWithMeta(ctx, "user:1", "conn-dead", map[string]string{"k": "v"}, 100*time.Millisecond))

	exists, err := client.HExists(ctx, meta, "conn-dead").Result()
	require.NoError(t, err)
	require.True(t, exists, "metadata must be recorded immediately after JoinWithMeta")

	// No Leave is ever called: the member's lease simply lapses, as it would for a crashed
	// node. A read that sweeps the member must reclaim its metadata in the same call.
	require.Eventually(t, func() bool {
		members, err := presence.Members(ctx, "user:1")
		return err == nil && len(members) == 0
	}, 5*time.Second, 20*time.Millisecond)

	exists, err = client.HExists(ctx, meta, "conn-dead").Result()
	require.NoError(t, err)
	require.False(t, exists, "a lazily swept member's metadata must not outlive it")
}

// TestRedisPresenceExpiryDerivesFromServerClock is the clock-skew reproduction and fix proof.
// Before the fix, every score written and every sweep cutoff was computed from this
// process's time.Now(): a node whose local clock ran ahead of the Redis server's could sweep
// out (or persist) another node's still-valid member simply because its own clock disagreed
// with the server everyone else's writes are actually judged against. Since the fix reads
// redis.call("TIME") inside the script instead, there is no longer a client-supplied
// timestamp for a skewed local clock to inject in the first place - the vulnerable code path
// does not exist any more. This test proves the positive half of that: the score a Join
// actually writes tracks the Redis server's own clock (via the same TIME command a
// zero-skew client would report), not whatever this process's clock says, by allowing only a
// tight tolerance around the server's independently observed time.
func TestRedisPresenceExpiryDerivesFromServerClock(t *testing.T) {
	client := testClient(t)
	ctx := context.Background()

	var counter atomic.Int64
	prefix := uniquePrefix(&counter)
	presence := gatewayredis.NewPresence(client, gatewayredis.WithKeyPrefix(prefix))
	key := memberKey(prefix, "user:1")
	t.Cleanup(func() { _ = client.Del(ctx, key).Err() })

	ttl := time.Minute
	require.NoError(t, presence.Join(ctx, "user:1", "conn-a", ttl))

	serverNow, err := client.Time(ctx).Result()
	require.NoError(t, err)

	scores, err := client.ZScore(ctx, key, "conn-a").Result()
	require.NoError(t, err)

	wantMs := float64(serverNow.Add(ttl).UnixMilli())
	require.InDelta(t, wantMs, scores, 2000, "the recorded expiry must track the Redis server's own clock (within round-trip tolerance), not this process's")
}

// TestRedisPresenceRefreshOfLapsedMemberKeepsOwnMetadata is a regression test for a self-
// inflicted edge case in the orphan-metadata sweep fix: the sweep that reclaims *other*
// members' metadata when they are found lapsed must not reclaim the metadata of the very
// connection id this call is reviving, or Refresh's documented "keeps any metadata a prior
// JoinWithMeta recorded alive" contract would silently break specifically for the recovery
// path (a refresh delayed past the full ttl) it exists to cover.
//
// Deliberately no Members/Online call happens between the lapse and the revival: either of
// those is itself a legitimate sweep that would reclaim the metadata as a *different*,
// correctly-working part of the same fix (a lapsed member observed by any reader is fair
// game to reclaim). This test isolates the case where Refresh itself is the first operation
// to notice the lapse, which is the case its self-exclusion guards.
func TestRedisPresenceRefreshOfLapsedMemberKeepsOwnMetadata(t *testing.T) {
	client := testClient(t)
	ctx := context.Background()

	var counter atomic.Int64
	prefix := uniquePrefix(&counter)
	presence := gatewayredis.NewPresence(client, gatewayredis.WithKeyPrefix(prefix))
	meta := metaKey(prefix, "user:1")
	t.Cleanup(func() { _ = client.Del(ctx, memberKey(prefix, "user:1"), meta).Err() })

	require.NoError(t, presence.JoinWithMeta(ctx, "user:1", "conn-a", map[string]string{"device": "ios"}, 100*time.Millisecond))

	// Let the lease lapse without anyone reading it, so Refresh below is the first operation
	// to observe the expiry - the case its self-exclusion is for.
	time.Sleep(300 * time.Millisecond)

	require.NoError(t, presence.Refresh(ctx, "user:1", "conn-a", time.Minute))

	entries, err := presence.Entries(ctx, "user:1")
	require.NoError(t, err)
	require.Len(t, entries, 1)
	require.Equal(t, map[string]string{"device": "ios"}, entries[0].Meta, "a Refresh that is itself the first to notice a lapsed member must keep its previously recorded metadata")
}

// commandCounter counts every top-level Redis command a client issues, so a test can prove
// an operation is a single round trip rather than inferring it from timing.
type commandCounter struct {
	count atomic.Int64
}

func (c *commandCounter) DialHook(next redis.DialHook) redis.DialHook { return next }

func (c *commandCounter) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		c.count.Add(1)
		return next(ctx, cmd)
	}
}

func (c *commandCounter) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return func(ctx context.Context, cmds []redis.Cmder) error {
		c.count.Add(int64(len(cmds)))
		return next(ctx, cmds)
	}
}

// TestRedisPresenceJoinWithMetaIsOneRoundTrip is the JoinWithMeta atomicity reproduction and
// fix proof. Before the fix, JoinWithMeta issued an HSET for the metadata followed by a
// separate script call for the membership write: two independent round trips, so a failure
// (crash, network partition) between them could leave metadata recorded for a connection
// that never actually joined, with no TTL of its own to reclaim it (a fresh group's metadata
// key gets its TTL from the join script that follows the HSET, which is exactly the call that
// did not happen). Counting commands proves the fix directly: metadata and membership now
// travel in the same Lua script call.
func TestRedisPresenceJoinWithMetaIsOneRoundTrip(t *testing.T) {
	addrClient := testClient(t)
	ctx := context.Background()

	var counter atomic.Int64
	prefix := uniquePrefix(&counter)

	counted := redis.NewClient(&redis.Options{Addr: addrClient.Options().Addr})
	t.Cleanup(func() { _ = counted.Close() })
	require.NoError(t, counted.Ping(ctx).Err())

	presence := gatewayredis.NewPresence(counted, gatewayredis.WithKeyPrefix(prefix))
	t.Cleanup(func() {
		_ = addrClient.Del(ctx, memberKey(prefix, "user:1"), metaKey(prefix, "user:1"), memberKey(prefix, "warmup")).Err()
	})

	// Warm the script cache first (Script.Run falls back from EVALSHA to EVAL, an extra
	// round trip, only on the very first use of a given script source), so the counted call
	// below reflects steady-state behaviour rather than a one-time cache miss.
	require.NoError(t, presence.Join(ctx, "warmup", "conn-warmup", time.Minute))

	hook := &commandCounter{}
	counted.AddHook(hook)

	require.NoError(t, presence.JoinWithMeta(ctx, "user:1", "conn-a", map[string]string{"k": "v"}, time.Minute))

	require.EqualValues(t, 1, hook.count.Load(), "JoinWithMeta must be exactly one Redis round trip, not a separate metadata HSET plus a join script call")
}
