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

// This suite requires a real Redis instance and is skipped unless TEST_REDIS_ADDR is set,
// so CI does not need a Redis daemon by default:
//
//	TEST_REDIS_ADDR=localhost:6379 go test ./persistence/redis/...
package redis_test

import (
	"context"
	"fmt"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"

	gateway "github.com/StringKe/goakt-gateway"
	"github.com/StringKe/goakt-gateway/persistence/conformance"
	gatewayredis "github.com/StringKe/goakt-gateway/persistence/redis"
)

// testClient dials the Redis instance named by TEST_REDIS_ADDR, skipping the test when it is
// not configured.
func testClient(t *testing.T) *redis.Client {
	t.Helper()

	addr := os.Getenv("TEST_REDIS_ADDR")
	if addr == "" {
		t.Skip("TEST_REDIS_ADDR not set; skipping Redis-backed Outbox tests")
	}

	client := redis.NewClient(&redis.Options{Addr: addr})
	t.Cleanup(func() { _ = client.Close() })
	require.NoError(t, client.Ping(context.Background()).Err(), "failed to reach Redis at TEST_REDIS_ADDR")
	return client
}

// processRunID makes prefixes unique per test process. The Outbox now holds a per-connection
// sequence counter in Redis that HINCRBY accumulates, so a bare counter resetting to zero
// every process would reuse a prior run's prefix and continue incrementing onto the sequence
// that run left behind, breaking tests that assert an exact Seq. Seeding with the process
// start time keeps two runs against the same Redis instance from colliding.
var processRunID = time.Now().UnixNano()

// uniquePrefix hands every Outbox its own key namespace, so neither subtests sharing one Redis
// instance nor separate test runs against it ever see each other's state.
func uniquePrefix(counter *atomic.Int64) string {
	return fmt.Sprintf("goakt-gateway-test-%d-%d:outbox:", processRunID, counter.Add(1))
}

func TestRedisOutboxConformance(t *testing.T) {
	client := testClient(t)

	var counter atomic.Int64
	conformance.Run(t, func(*testing.T) gateway.Outbox {
		return gatewayredis.New(client, gatewayredis.WithKeyPrefix(uniquePrefix(&counter)))
	})
}

// TestRedisOutboxKeyLayout pins the key shape down: one hash per connection id, under the
// configured prefix, whose message fields are keyed by the Outbox-minted message id alongside
// one reserved sequence field.
func TestRedisOutboxKeyLayout(t *testing.T) {
	client := testClient(t)
	ctx := context.Background()

	var counter atomic.Int64
	prefix := uniquePrefix(&counter)
	outbox := gatewayredis.New(client, gatewayredis.WithKeyPrefix(prefix))
	key := prefix + "conn-a"
	t.Cleanup(func() { _ = client.Del(ctx, key).Err() })

	msgID, seq, err := outbox.Append(ctx, "conn-a", []byte("a"))
	require.NoError(t, err)
	require.EqualValues(t, 1, seq)

	kind, err := client.Type(ctx, key).Result()
	require.NoError(t, err)
	require.Equal(t, "hash", kind)

	fields, err := client.HKeys(ctx, key).Result()
	require.NoError(t, err)
	require.ElementsMatch(t, []string{msgID, "\x00seq"}, fields, "before any Ack the ack-generation field must not exist yet")

	value, err := client.HGet(ctx, key, msgID).Result()
	require.NoError(t, err)
	require.Equal(t, "1:a", value, "the message value is <seq>:<payload>")
}

// TestRedisOutboxAckAddsGenerationField proves the ack-generation floor field only appears
// once an Ack has actually happened, and holds the accepted generation.
func TestRedisOutboxAckAddsGenerationField(t *testing.T) {
	client := testClient(t)
	ctx := context.Background()

	var counter atomic.Int64
	prefix := uniquePrefix(&counter)
	outbox := gatewayredis.New(client, gatewayredis.WithKeyPrefix(prefix))
	key := prefix + "conn-a"
	t.Cleanup(func() { _ = client.Del(ctx, key).Err() })

	msgID, _, err := outbox.Append(ctx, "conn-a", []byte("a"))
	require.NoError(t, err)
	require.NoError(t, outbox.Ack(ctx, "conn-a", msgID, 5))

	value, err := client.HGet(ctx, key, "\x00ackgen").Result()
	require.NoError(t, err)
	require.Equal(t, "5", value)

	fields, err := client.HKeys(ctx, key).Result()
	require.NoError(t, err)
	require.ElementsMatch(t, []string{"\x00seq", "\x00ackgen"}, fields, "the acked message field must be gone")
}

func TestRedisOutboxAdvanceGenerationDoesNotCreateEmptyState(t *testing.T) {
	client := testClient(t)
	ctx := context.Background()

	var counter atomic.Int64
	prefix := uniquePrefix(&counter)
	outbox := gatewayredis.New(client, gatewayredis.WithKeyPrefix(prefix))
	key := prefix + "unused"
	t.Cleanup(func() { _ = client.Del(ctx, key).Err() })

	require.NoError(t, outbox.AdvanceGeneration(ctx, "unused", 3))
	exists, err := client.Exists(ctx, key).Result()
	require.NoError(t, err)
	require.Zero(t, exists)

	msgID, _, err := outbox.Append(ctx, "unused", []byte("payload"))
	require.NoError(t, err)
	require.NoError(t, outbox.Ack(ctx, "unused", msgID, 0))
}

// TestRedisOutboxNoTTLByDefault proves an Outbox without WithTTL retains the tail
// indefinitely, so nothing but Ack or DropConn removes it.
func TestRedisOutboxNoTTLByDefault(t *testing.T) {
	client := testClient(t)
	ctx := context.Background()

	var counter atomic.Int64
	prefix := uniquePrefix(&counter)
	outbox := gatewayredis.New(client, gatewayredis.WithKeyPrefix(prefix))
	key := prefix + "conn-a"
	t.Cleanup(func() { _ = client.Del(ctx, key).Err() })

	_, _, err := outbox.Append(ctx, "conn-a", []byte("a"))
	require.NoError(t, err)

	ttl, err := client.PTTL(ctx, key).Result()
	require.NoError(t, err)
	require.EqualValues(t, -1, ttl, "without WithTTL the connection key must carry no expiry")
}

// TestRedisOutboxTTLReArmedOnAppend proves WithTTL puts a sliding expiry on the connection
// key and that every Append re-arms it, so an active connection's tail is never reclaimed out
// from under it while a fully idle one is eventually bounded.
func TestRedisOutboxTTLReArmedOnAppend(t *testing.T) {
	client := testClient(t)
	ctx := context.Background()

	var counter atomic.Int64
	prefix := uniquePrefix(&counter)
	outbox := gatewayredis.New(client, gatewayredis.WithKeyPrefix(prefix), gatewayredis.WithTTL(time.Minute))
	key := prefix + "conn-a"
	t.Cleanup(func() { _ = client.Del(ctx, key).Err() })

	_, _, err := outbox.Append(ctx, "conn-a", []byte("a"))
	require.NoError(t, err)

	first, err := client.PTTL(ctx, key).Result()
	require.NoError(t, err)
	require.Positive(t, first, "WithTTL must put an expiry on the connection key")
	require.LessOrEqual(t, first, time.Minute)

	_, _, err = outbox.Append(ctx, "conn-a", []byte("b"))
	require.NoError(t, err)

	second, err := client.PTTL(ctx, key).Result()
	require.NoError(t, err)
	require.Positive(t, second, "a second Append must re-arm the expiry, not clear it")
}

// TestRedisOutboxSharesOneBackend is the multi-node case the memory backend cannot cover: a
// message appended by one process (before it died) is redelivered by another process reading
// the same Redis, which is the entire reason to run a Redis Outbox over the memory one.
func TestRedisOutboxSharesOneBackend(t *testing.T) {
	client := testClient(t)
	ctx := context.Background()

	var counter atomic.Int64
	prefix := uniquePrefix(&counter)
	nodeA := gatewayredis.New(client, gatewayredis.WithKeyPrefix(prefix))
	nodeB := gatewayredis.New(client, gatewayredis.WithKeyPrefix(prefix))
	t.Cleanup(func() { _ = client.Del(ctx, prefix+"conn-a").Err() })

	msgID, seq, err := nodeA.Append(ctx, "conn-a", []byte("a"))
	require.NoError(t, err)

	msgs, err := nodeB.Unacked(ctx, "conn-a")
	require.NoError(t, err)
	require.Equal(t, []gateway.PersistedMessage{{ID: msgID, Seq: seq, Payload: []byte("a")}}, msgs)

	require.NoError(t, nodeB.Ack(ctx, "conn-a", msgID, 0))

	msgs, err = nodeA.Unacked(ctx, "conn-a")
	require.NoError(t, err)
	require.Empty(t, msgs, "an ack on one node must clear the tail every node sees")
}

// TestRedisOutboxSeqMonotonicAcrossProcesses is the correctness the memory backend cannot
// give: two independent processes (or the same process before and after a restart) appending
// to the same offline connection must never mint the same Seq, because the sequence is held
// in Redis, not in either process. A per-process counter would hand both messages Seq 1 and a
// client that dedupes on Seq would silently drop one.
func TestRedisOutboxSeqMonotonicAcrossProcesses(t *testing.T) {
	client := testClient(t)
	ctx := context.Background()

	var counter atomic.Int64
	prefix := uniquePrefix(&counter)
	nodeA := gatewayredis.New(client, gatewayredis.WithKeyPrefix(prefix))
	nodeB := gatewayredis.New(client, gatewayredis.WithKeyPrefix(prefix))
	t.Cleanup(func() { _ = client.Del(ctx, prefix+"conn-a").Err() })

	idA, seqA, err := nodeA.Append(ctx, "conn-a", []byte("a"))
	require.NoError(t, err)
	idB, seqB, err := nodeB.Append(ctx, "conn-a", []byte("b"))
	require.NoError(t, err)

	msgs, err := nodeA.Unacked(ctx, "conn-a")
	require.NoError(t, err)
	require.Equal(t, []gateway.PersistedMessage{
		{ID: idA, Seq: seqA, Payload: []byte("a")},
		{ID: idB, Seq: seqB, Payload: []byte("b")},
	}, msgs, "the second appender must continue the sequence, not restart it")
}

// TestRedisOutboxAckGenerationFencingSharedAcrossProcesses proves the ack-generation floor is
// visible cross-process through Redis, the same way the message tail and sequence counter
// already are: a lower-generation ack issued from a second Outbox instance against the same
// key must still be rejected once a higher generation has been accepted by the first.
func TestRedisOutboxAckGenerationFencingSharedAcrossProcesses(t *testing.T) {
	client := testClient(t)
	ctx := context.Background()

	var counter atomic.Int64
	prefix := uniquePrefix(&counter)
	nodeA := gatewayredis.New(client, gatewayredis.WithKeyPrefix(prefix))
	nodeB := gatewayredis.New(client, gatewayredis.WithKeyPrefix(prefix))
	t.Cleanup(func() { _ = client.Del(ctx, prefix+"conn-a").Err() })

	id1, _, err := nodeA.Append(ctx, "conn-a", []byte("a"))
	require.NoError(t, err)
	id2, _, err := nodeA.Append(ctx, "conn-a", []byte("b"))
	require.NoError(t, err)

	require.NoError(t, nodeB.Ack(ctx, "conn-a", id1, 10), "the successor node's higher-generation ack must be accepted")
	err = nodeA.Ack(ctx, "conn-a", id2, 3)
	require.ErrorIs(t, err, gateway.ErrStaleOwner, "the fenced-out node's lower-generation ack must be rejected")

	msgs, err := nodeA.Unacked(ctx, "conn-a")
	require.NoError(t, err)
	require.Equal(t, []gateway.PersistedMessage{{ID: id2, Seq: 2, Payload: []byte("b")}}, msgs)
}
