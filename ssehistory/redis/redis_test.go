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
//	TEST_REDIS_ADDR=localhost:6379 go test ./ssehistory/redis/...
package redis_test

import (
	"context"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"

	gateway "github.com/StringKe/goakt-gateway"
	"github.com/StringKe/goakt-gateway/ssehistory/conformance"
	gatewayredis "github.com/StringKe/goakt-gateway/ssehistory/redis"
)

// testClient dials the Redis instance named by TEST_REDIS_ADDR, skipping the test when it
// is not configured.
func testClient(t *testing.T) *redis.Client {
	t.Helper()

	addr := os.Getenv("TEST_REDIS_ADDR")
	if addr == "" {
		t.Skip("TEST_REDIS_ADDR not set; skipping Redis-backed SSEHistory tests")
	}

	client := redis.NewClient(&redis.Options{Addr: addr})
	t.Cleanup(func() { _ = client.Close() })
	require.NoError(t, client.Ping(context.Background()).Err(), "failed to reach Redis at TEST_REDIS_ADDR")
	return client
}

// processRunID makes prefixes unique per test process. Unlike the other Redis backends,
// SSEHistory.Append accumulates (RPUSH) rather than overwriting, so a counter that resets to
// zero every process would reuse a prior run's prefix and append onto the events that run left
// behind under their idle TTL. Seeding with the process start time keeps two runs against the
// same Redis instance from colliding.
var processRunID = time.Now().UnixNano()

// uniquePrefix hands every History its own key namespace, so neither subtests sharing one Redis
// instance nor separate test runs against it ever see each other's state.
func uniquePrefix(counter *atomic.Int64) string {
	return fmt.Sprintf("goakt-gateway-test-%d-%d:ssehistory:", processRunID, counter.Add(1))
}

func TestRedisHistoryConformance(t *testing.T) {
	client := testClient(t)

	var counter atomic.Int64
	conformance.Run(t, func() gateway.SSEHistory {
		// perConn well above the suite's 8-event ceiling, as the suite's factory contract
		// requires, so retention never truncates a conformance assertion.
		return gatewayredis.New(client,
			gatewayredis.WithKeyPrefix(uniquePrefix(&counter)),
			gatewayredis.WithPerConn(32),
		)
	})
}

// TestRedisHistoryKeyLayout pins the key shape down: one LIST per connection, under the
// configured prefix, whose elements are the retained events in wire order.
func TestRedisHistoryKeyLayout(t *testing.T) {
	client := testClient(t)
	ctx := context.Background()

	var counter atomic.Int64
	prefix := uniquePrefix(&counter)
	history := gatewayredis.New(client, gatewayredis.WithKeyPrefix(prefix))
	key := prefix + "conn-1"
	t.Cleanup(func() { _ = client.Del(ctx, key).Err() })

	require.NoError(t, history.Append(ctx, "conn-1", "e-1", []byte("one")))
	require.NoError(t, history.Append(ctx, "conn-1", "e-2", []byte("two")))

	kind, err := client.Type(ctx, key).Result()
	require.NoError(t, err)
	require.Equal(t, "list", kind)

	length, err := client.LLen(ctx, key).Result()
	require.NoError(t, err)
	require.EqualValues(t, 2, length)
}

// TestRedisHistoryPerConnOverflow proves the LTRIM bound drops the oldest events once more
// than perConn have been appended, keeping only the most recent window. This is the
// backend-specific cap the shared conformance suite deliberately does not assert.
func TestRedisHistoryPerConnOverflow(t *testing.T) {
	client := testClient(t)
	ctx := context.Background()

	var counter atomic.Int64
	prefix := uniquePrefix(&counter)
	history := gatewayredis.New(client, gatewayredis.WithKeyPrefix(prefix), gatewayredis.WithPerConn(3))
	t.Cleanup(func() { _ = client.Del(ctx, prefix+"conn-1").Err() })

	for i := 1; i <= 5; i++ {
		require.NoError(t, history.Append(ctx, "conn-1", fmt.Sprintf("e-%d", i), fmt.Appendf(nil, "p%d", i)))
	}

	events, err := history.Since(ctx, "conn-1", "")
	require.NoError(t, err)
	require.Equal(t, []gateway.SSEEvent{
		{ID: "e-3", Payload: []byte("p3")},
		{ID: "e-4", Payload: []byte("p4")},
		{ID: "e-5", Payload: []byte("p5")},
	}, events, "only the most recent perConn events must survive, oldest first")

	// An id that has been trimmed away is now unknown: the caller must be told there is a gap.
	events, err = history.Since(ctx, "conn-1", "e-1")
	require.ErrorIs(t, err, gateway.ErrHistoryGap)
	require.Len(t, events, 3)
}

// TestRedisHistorySinceExcludesGenerationMarkers proves a marker AdvanceGeneration wrote never
// leaks into Since's output, whether the reconnect asks for everything or for events after a
// real id that precedes the marker in wire order - covering the filter running both before and
// interleaved with the Last-Event-ID search.
func TestRedisHistorySinceExcludesGenerationMarkers(t *testing.T) {
	client := testClient(t)
	ctx := context.Background()

	var counter atomic.Int64
	prefix := uniquePrefix(&counter)
	history := gatewayredis.New(client, gatewayredis.WithKeyPrefix(prefix))
	var generational gateway.GenerationalHistory = history
	t.Cleanup(func() { _ = client.Del(ctx, prefix+"conn-1").Err() })

	_, err := generational.AppendGenerational(ctx, "conn-1", "e-1", []byte("one"), 1)
	require.NoError(t, err)
	require.NoError(t, generational.AdvanceGeneration(ctx, "conn-1", 2))
	_, err = generational.AppendGenerational(ctx, "conn-1", "e-2", []byte("two"), 2)
	require.NoError(t, err)

	events, err := history.Since(ctx, "conn-1", "")
	require.NoError(t, err)
	require.Equal(t, []gateway.SSEEvent{
		{ID: "e-1", Payload: []byte("one")},
		{ID: "e-2", Payload: []byte("two")},
	}, events, "the marker AdvanceGeneration wrote between e-1 and e-2 must not appear")

	events, err = history.Since(ctx, "conn-1", "e-1")
	require.NoError(t, err)
	require.Equal(t, []gateway.SSEEvent{{ID: "e-2", Payload: []byte("two")}}, events,
		"the marker between the requested id and the next real event must still be filtered")
}

// TestRedisHistoryTTLReclaims proves the idle TTL reclaims a connection's buffer: after the
// key expires with no further Append, a reconnect naming a once-valid id gets a gap rather
// than a silent empty replay. Uses a short TTL plus a real wait; the margin over the TTL
// keeps the timing robust on a loaded machine.
func TestRedisHistoryTTLReclaims(t *testing.T) {
	client := testClient(t)
	ctx := context.Background()

	var counter atomic.Int64
	prefix := uniquePrefix(&counter)
	history := gatewayredis.New(client,
		gatewayredis.WithKeyPrefix(prefix),
		gatewayredis.WithTTL(time.Second),
	)
	t.Cleanup(func() { _ = client.Del(ctx, prefix+"conn-1").Err() })

	require.NoError(t, history.Append(ctx, "conn-1", "e-1", []byte("one")))

	// Before expiry the event is retained and a known id replays cleanly.
	events, err := history.Since(ctx, "conn-1", "")
	require.NoError(t, err)
	require.Len(t, events, 1)

	require.Eventually(t, func() bool {
		events, err := history.Since(ctx, "conn-1", "")
		return err == nil && len(events) == 0
	}, 5*time.Second, 100*time.Millisecond, "the connection key must be reclaimed once its idle TTL elapses")

	// A reconnect after reclaim names an id the backend no longer has: it must report a gap.
	events, err = history.Since(ctx, "conn-1", "e-1")
	require.ErrorIs(t, err, gateway.ErrHistoryGap)
	require.Empty(t, events)
}

// TestRedisHistoryAppendRefreshesTTL proves each Append re-arms the idle TTL, so an actively
// written connection is never reclaimed mid-stream.
func TestRedisHistoryAppendRefreshesTTL(t *testing.T) {
	client := testClient(t)
	ctx := context.Background()

	var counter atomic.Int64
	prefix := uniquePrefix(&counter)
	history := gatewayredis.New(client,
		gatewayredis.WithKeyPrefix(prefix),
		gatewayredis.WithTTL(30*time.Second),
	)
	key := prefix + "conn-1"
	t.Cleanup(func() { _ = client.Del(ctx, key).Err() })

	require.NoError(t, history.Append(ctx, "conn-1", "e-1", []byte("one")))
	ttl, err := client.PTTL(ctx, key).Result()
	require.NoError(t, err)
	require.Positive(t, ttl, "each connection key must carry the idle TTL")
	require.LessOrEqual(t, ttl, 30*time.Second)
}

// TestRedisHistoryRefreshTTLKeepsLiveStream proves the keepalive path: RefreshTTL re-arms the
// idle TTL without appending, so a still-connected but low-traffic stream keeps its buffer past
// the TTL instead of expiring mid-connection and answering a reconnect with a false gap. Once
// the refreshes stop, the buffer ages out on schedule, so a truly gone connection is still
// reclaimed.
func TestRedisHistoryRefreshTTLKeepsLiveStream(t *testing.T) {
	client := testClient(t)
	ctx := context.Background()

	var counter atomic.Int64
	prefix := uniquePrefix(&counter)
	history := gatewayredis.New(client,
		gatewayredis.WithKeyPrefix(prefix),
		gatewayredis.WithTTL(time.Second),
	)
	t.Cleanup(func() { _ = client.Del(ctx, prefix+"conn-1").Err() })

	require.NoError(t, history.Append(ctx, "conn-1", "e-1", []byte("one")))

	// Span well over the 1s TTL, re-arming every 200ms the way the SSEHandler keepalive does.
	// The event must remain retained throughout, which it cannot if RefreshTTL failed to touch
	// the key.
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()
	deadline := time.After(2500 * time.Millisecond)
loop:
	for {
		select {
		case <-ticker.C:
			require.NoError(t, history.RefreshTTL(ctx, "conn-1"))
			events, err := history.Since(ctx, "conn-1", "")
			require.NoError(t, err)
			require.Len(t, events, 1, "a refreshed buffer must survive past its idle TTL")
		case <-deadline:
			break loop
		}
	}

	// With the keepalives stopped, nothing keeps the buffer alive and the idle TTL reclaims it.
	require.Eventually(t, func() bool {
		events, err := history.Since(ctx, "conn-1", "")
		return err == nil && len(events) == 0
	}, 5*time.Second, 100*time.Millisecond, "after RefreshTTL stops, the idle TTL must reclaim the buffer")

	// RefreshTTL on a reclaimed (absent) key is a no-op, not an error, matching a PEXPIRE that
	// finds nothing.
	require.NoError(t, history.RefreshTTL(ctx, "conn-1"))
	events, err := history.Since(ctx, "conn-1", "e-1")
	require.ErrorIs(t, err, gateway.ErrHistoryGap)
	require.Empty(t, events)
}

// TestRedisHistoryAppendGenerationalAssignsIncreasingSeq proves AppendGenerational's sequence
// counter is monotonic with no gaps or repeats across accepted calls, regardless of which
// generation each one names, matching gateway.GenerationalHistory's documented contract.
func TestRedisHistoryAppendGenerationalAssignsIncreasingSeq(t *testing.T) {
	client := testClient(t)
	ctx := context.Background()

	var counter atomic.Int64
	prefix := uniquePrefix(&counter)
	history := gatewayredis.New(client, gatewayredis.WithKeyPrefix(prefix))
	var generational gateway.GenerationalHistory = history
	t.Cleanup(func() { _ = client.Del(ctx, prefix+"conn-1").Err() })

	seq1, err := generational.AppendGenerational(ctx, "conn-1", "e-1", []byte("one"), 1)
	require.NoError(t, err)
	require.EqualValues(t, 1, seq1)

	seq2, err := generational.AppendGenerational(ctx, "conn-1", "e-2", []byte("two"), 1)
	require.NoError(t, err)
	require.EqualValues(t, 2, seq2)

	// A later, higher generation is accepted and continues the same counter.
	seq3, err := generational.AppendGenerational(ctx, "conn-1", "e-3", []byte("three"), 2)
	require.NoError(t, err)
	require.EqualValues(t, 3, seq3)

	events, err := history.Since(ctx, "conn-1", "")
	require.NoError(t, err)
	require.Equal(t, []gateway.SSEEvent{
		{ID: "e-1", Payload: []byte("one")},
		{ID: "e-2", Payload: []byte("two")},
		{ID: "e-3", Payload: []byte("three")},
	}, events, "generation fencing must not disturb ordinary replay ordering")
}

// TestRedisHistoryAppendGenerationalRejectsStaleGeneration proves a write naming a generation
// lower than one already accepted for the connection is rejected outright - not recorded, and
// does not consume a sequence number - so it can never appear in a later replay.
func TestRedisHistoryAppendGenerationalRejectsStaleGeneration(t *testing.T) {
	client := testClient(t)
	ctx := context.Background()

	var counter atomic.Int64
	prefix := uniquePrefix(&counter)
	history := gatewayredis.New(client, gatewayredis.WithKeyPrefix(prefix))
	var generational gateway.GenerationalHistory = history
	t.Cleanup(func() { _ = client.Del(ctx, prefix+"conn-1").Err() })

	_, err := generational.AppendGenerational(ctx, "conn-1", "e-1", []byte("one"), 5)
	require.NoError(t, err)

	_, err = generational.AppendGenerational(ctx, "conn-1", "e-stale", []byte("stale"), 4)
	require.ErrorIs(t, err, gateway.ErrStaleGeneration)

	seq, err := generational.AppendGenerational(ctx, "conn-1", "e-2", []byte("two"), 5)
	require.NoError(t, err)
	require.EqualValues(t, 2, seq, "the stale rejection must not have consumed a sequence number")

	events, err := history.Since(ctx, "conn-1", "")
	require.NoError(t, err)
	require.Equal(t, []gateway.SSEEvent{
		{ID: "e-1", Payload: []byte("one")},
		{ID: "e-2", Payload: []byte("two")},
	}, events, "the rejected write must never appear in replay")
}

// TestRedisHistoryAdvanceGenerationFencesSubsequentStaleAppend covers the takeover entry point:
// bumping the generation with no accompanying event still fences the very next
// AppendGenerational call naming the superseded generation, closing the window where a stale
// owner could otherwise race a fresh one to append first.
func TestRedisHistoryAdvanceGenerationFencesSubsequentStaleAppend(t *testing.T) {
	client := testClient(t)
	ctx := context.Background()

	var counter atomic.Int64
	prefix := uniquePrefix(&counter)
	history := gatewayredis.New(client, gatewayredis.WithKeyPrefix(prefix))
	var generational gateway.GenerationalHistory = history
	t.Cleanup(func() { _ = client.Del(ctx, prefix+"conn-1").Err() })

	require.NoError(t, generational.AdvanceGeneration(ctx, "conn-1", 2))

	_, err := generational.AppendGenerational(ctx, "conn-1", "e-stale", []byte("stale"), 1)
	require.ErrorIs(t, err, gateway.ErrStaleGeneration)

	seq, err := generational.AppendGenerational(ctx, "conn-1", "e-1", []byte("one"), 2)
	require.NoError(t, err)
	require.EqualValues(t, 1, seq)

	// The marker AdvanceGeneration wrote must never surface in replay.
	events, err := history.Since(ctx, "conn-1", "")
	require.NoError(t, err)
	require.Equal(t, []gateway.SSEEvent{{ID: "e-1", Payload: []byte("one")}}, events)
}

// TestRedisHistoryAdvanceGenerationIsNoOpWhenNotGreater proves AdvanceGeneration leaves the
// recorded generation untouched, and returns no error, when the caller's generation does not
// exceed what is already recorded - both for an equal value and an explicit regression.
func TestRedisHistoryAdvanceGenerationIsNoOpWhenNotGreater(t *testing.T) {
	client := testClient(t)
	ctx := context.Background()

	var counter atomic.Int64
	prefix := uniquePrefix(&counter)
	history := gatewayredis.New(client, gatewayredis.WithKeyPrefix(prefix))
	var generational gateway.GenerationalHistory = history
	t.Cleanup(func() { _ = client.Del(ctx, prefix+"conn-1").Err() })

	require.NoError(t, generational.AdvanceGeneration(ctx, "conn-1", 5))
	require.NoError(t, generational.AdvanceGeneration(ctx, "conn-1", 5), "equal generation must be a no-op, not an error")
	require.NoError(t, generational.AdvanceGeneration(ctx, "conn-1", 3), "a regression must be a no-op, not an error")

	_, err := generational.AppendGenerational(ctx, "conn-1", "e-stale", []byte("stale"), 4)
	require.ErrorIs(t, err, gateway.ErrStaleGeneration, "generation 5 must still be enforced after the no-op calls")
}

// TestRedisHistoryAppendGenerationalConcurrentRejectsStaleAndKeepsSeqUnique drives many
// goroutines at increasing generations against one connection concurrently, proving under
// -race that every accepted sequence number is unique with no gaps in the accepted subset, and
// that a call naming a generation already superseded by an accepted one is always rejected -
// never silently interleaved into the buffer a reconnect would replay.
func TestRedisHistoryAppendGenerationalConcurrentRejectsStaleAndKeepsSeqUnique(t *testing.T) {
	client := testClient(t)
	ctx := context.Background()

	var counter atomic.Int64
	prefix := uniquePrefix(&counter)
	history := gatewayredis.New(client, gatewayredis.WithKeyPrefix(prefix), gatewayredis.WithPerConn(256))
	var generational gateway.GenerationalHistory = history
	t.Cleanup(func() { _ = client.Del(ctx, prefix+"conn-1").Err() })

	// Two "generations" of writers race to append. Every generation-2 write must eventually
	// fence out every generation-1 write that loses the race (or all of them, if generation 2
	// gets there first), and every accepted seq across both must be unique.
	const perGeneration = 20
	var wg sync.WaitGroup
	var acceptedSeqs sync.Map // seq -> struct{}
	var acceptedCount atomic.Int64

	run := func(generation uint64, id string) {
		defer wg.Done()
		seq, err := generational.AppendGenerational(ctx, "conn-1", id, []byte(id), generation)
		if err != nil {
			require.ErrorIs(t, err, gateway.ErrStaleGeneration)
			return
		}
		_, loaded := acceptedSeqs.LoadOrStore(seq, struct{}{})
		require.False(t, loaded, "sequence number %d was assigned to two accepted writes", seq)
		acceptedCount.Add(1)
	}

	for i := range perGeneration {
		wg.Add(2)
		go run(1, fmt.Sprintf("gen1-%d", i))
		go run(2, fmt.Sprintf("gen2-%d", i))
	}
	wg.Wait()

	require.Positive(t, acceptedCount.Load(), "at least one write must have been accepted")

	// The accepted sequence numbers must form a gap-free 1..N run.
	var maxSeq int64
	acceptedSeqs.Range(func(key, _ any) bool {
		seq := key.(uint64)
		if int64(seq) > maxSeq {
			maxSeq = int64(seq)
		}
		return true
	})
	require.EqualValues(t, acceptedCount.Load(), maxSeq, "accepted sequence numbers must be exactly 1..N with no gaps")

	// Replay must be free of reordering or omission: every retained event's id must belong to
	// an accepted write, in the exact order Since returns them, and no rejected write's id may
	// appear.
	events, err := history.Since(ctx, "conn-1", "")
	require.NoError(t, err)
	require.LessOrEqual(t, len(events), 256)
	for _, ev := range events {
		require.Equal(t, ev.ID, string(ev.Payload), "each retained event's payload must match its own id, ruling out cross-write corruption")
	}
}

// TestRedisHistorySharesOneBackend is the multi-node case the memory backend cannot cover: a
// client that appended through one node (its writer goroutine there) reconnects to a second
// node and is replayed from the shared buffer.
func TestRedisHistorySharesOneBackend(t *testing.T) {
	client := testClient(t)
	ctx := context.Background()

	var counter atomic.Int64
	prefix := uniquePrefix(&counter)
	nodeA := gatewayredis.New(client, gatewayredis.WithKeyPrefix(prefix))
	nodeB := gatewayredis.New(client, gatewayredis.WithKeyPrefix(prefix))
	t.Cleanup(func() { _ = client.Del(ctx, prefix+"conn-1").Err() })

	require.NoError(t, nodeA.Append(ctx, "conn-1", "e-1", []byte("one")))
	require.NoError(t, nodeA.Append(ctx, "conn-1", "e-2", []byte("two")))

	events, err := nodeB.Since(ctx, "conn-1", "e-1")
	require.NoError(t, err)
	require.Equal(t, []gateway.SSEEvent{{ID: "e-2", Payload: []byte("two")}}, events,
		"a reconnect to another node must replay from the shared buffer")
}

// TestRedisHistoryPrefixIsolation proves two deployments sharing one Redis database do not
// see each other's connections.
func TestRedisHistoryPrefixIsolation(t *testing.T) {
	client := testClient(t)
	ctx := context.Background()

	var counter atomic.Int64
	prefixA := uniquePrefix(&counter)
	prefixB := uniquePrefix(&counter)
	deploymentA := gatewayredis.New(client, gatewayredis.WithKeyPrefix(prefixA))
	deploymentB := gatewayredis.New(client, gatewayredis.WithKeyPrefix(prefixB))
	t.Cleanup(func() {
		_ = client.Del(ctx, prefixA+"conn-1", prefixB+"conn-1").Err()
	})

	require.NoError(t, deploymentA.Append(ctx, "conn-1", "e-1", []byte("one")))

	events, err := deploymentB.Since(ctx, "conn-1", "")
	require.NoError(t, err)
	require.Empty(t, events, "a different key prefix must not observe another deployment's events")
}
