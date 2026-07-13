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
	"fmt"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/tochemey/goakt/v4/actor"
	"github.com/tochemey/goakt/v4/log"

	gateway "github.com/StringKe/goakt-gateway"
)

// TestMemoryPresenceJoinLeave covers the plain membership lifecycle.
func TestMemoryPresenceJoinLeave(t *testing.T) {
	presence := gateway.NewMemoryPresence()
	ctx := context.Background()

	require.NoError(t, presence.Join(ctx, "user:1", "conn-a", time.Minute))
	require.NoError(t, presence.Join(ctx, "user:1", "conn-b", time.Minute))

	members, err := presence.Members(ctx, "user:1")
	require.NoError(t, err)
	require.ElementsMatch(t, []string{"conn-a", "conn-b"}, members)

	online, err := presence.Online(ctx, "user:1")
	require.NoError(t, err)
	require.True(t, online)

	require.NoError(t, presence.Leave(ctx, "user:1", "conn-a"))
	require.NoError(t, presence.Leave(ctx, "user:1", "conn-b"))
	// leaving twice must not resurrect an error
	require.NoError(t, presence.Leave(ctx, "user:1", "conn-b"))

	online, err = presence.Online(ctx, "user:1")
	require.NoError(t, err)
	require.False(t, online)
}

// TestMemoryPresenceTTLExpiry pins the lease semantics a dead node relies on: a member
// whose lease is never renewed stops counting as online once the TTL elapses, without
// anyone calling Leave.
func TestMemoryPresenceTTLExpiry(t *testing.T) {
	presence := gateway.NewMemoryPresence()
	ctx := context.Background()

	require.NoError(t, presence.Join(ctx, "user:1", "conn-a", 100*time.Millisecond))

	online, err := presence.Online(ctx, "user:1")
	require.NoError(t, err)
	require.True(t, online)

	time.Sleep(250 * time.Millisecond)

	members, err := presence.Members(ctx, "user:1")
	require.NoError(t, err)
	require.Empty(t, members, "a lapsed lease must not keep a connection online")

	online, err = presence.Online(ctx, "user:1")
	require.NoError(t, err)
	require.False(t, online)
}

// TestMemoryPresenceRefreshExtendsLease verifies that a renewed lease survives past the
// point where the original one would have lapsed.
func TestMemoryPresenceRefreshExtendsLease(t *testing.T) {
	presence := gateway.NewMemoryPresence()
	ctx := context.Background()

	require.NoError(t, presence.Join(ctx, "user:1", "conn-a", 150*time.Millisecond))
	time.Sleep(100 * time.Millisecond)
	require.NoError(t, presence.Refresh(ctx, "user:1", "conn-a", time.Minute))
	time.Sleep(150 * time.Millisecond)

	online, err := presence.Online(ctx, "user:1")
	require.NoError(t, err)
	require.True(t, online, "a refreshed lease must outlive the original ttl")
}

// TestRegistryPresenceLifecycle verifies that the Registry keeps the presence backend in
// step with its local table: joined on register, left on unregister.
func TestRegistryPresenceLifecycle(t *testing.T) {
	system := newTestSystem(t, actor.WithPubSub())
	presence := gateway.NewMemoryPresence()
	registry := gateway.NewRegistry(system, log.DiscardLogger,
		gateway.WithPresence(presence),
		gateway.WithPresenceTTL(time.Minute),
	)
	t.Cleanup(func() { _ = registry.Close(context.Background()) })
	ctx := context.Background()

	online, err := registry.IsOnline(ctx, "user:1")
	require.NoError(t, err)
	require.False(t, online)

	require.NoError(t, registry.Register(ctx, "phone", func([]byte) error { return nil }, gateway.WithConnGroup("user:1")))

	online, err = registry.IsOnline(ctx, "user:1")
	require.NoError(t, err)
	require.True(t, online)

	members, err := presence.Members(ctx, "user:1")
	require.NoError(t, err)
	require.Equal(t, []string{"phone"}, members)

	require.NoError(t, registry.Unregister(ctx, "phone"))

	online, err = registry.IsOnline(ctx, "user:1")
	require.NoError(t, err)
	require.False(t, online)
}

// TestRegistryIsOnlineWithoutPresence verifies the degraded, local-only answer a Registry
// without a Presence backend gives.
func TestRegistryIsOnlineWithoutPresence(t *testing.T) {
	system := newTestSystem(t, actor.WithPubSub())
	registry := gateway.NewRegistry(system, log.DiscardLogger)
	ctx := context.Background()

	require.NoError(t, registry.Register(ctx, "phone", func([]byte) error { return nil }, gateway.WithConnGroup("user:1")))

	online, err := registry.IsOnline(ctx, "user:1")
	require.NoError(t, err)
	require.True(t, online)

	online, err = registry.IsOnline(ctx, "user:2")
	require.NoError(t, err)
	require.False(t, online)
}

// TestRegistryCloseIsIdempotent verifies that Close stops the presence renewal goroutine
// and can be called repeatedly, and that a closed Registry takes no new connections.
func TestRegistryCloseIsIdempotent(t *testing.T) {
	system := newTestSystem(t, actor.WithPubSub())
	registry := gateway.NewRegistry(system, log.DiscardLogger,
		gateway.WithPresence(gateway.NewMemoryPresence()),
		gateway.WithPresenceTTL(300*time.Millisecond),
	)
	ctx := context.Background()

	require.NoError(t, registry.Close(ctx))
	require.NoError(t, registry.Close(ctx))

	err := registry.Register(ctx, "late", func([]byte) error { return nil })
	require.ErrorIs(t, err, gateway.ErrRegistryClosed)
}

// TestRegistryPresenceRenewal verifies that the background renewal keeps a live
// connection online past its lease, which is what stops a long-lived socket from falling
// out of presence and getting web-pushed at instead.
func TestRegistryPresenceRenewal(t *testing.T) {
	system := newTestSystem(t, actor.WithPubSub())
	presence := gateway.NewMemoryPresence()
	registry := gateway.NewRegistry(system, log.DiscardLogger,
		gateway.WithPresence(presence),
		gateway.WithPresenceTTL(300*time.Millisecond),
	)
	t.Cleanup(func() { _ = registry.Close(context.Background()) })
	ctx := context.Background()

	require.NoError(t, registry.Register(ctx, "phone", func([]byte) error { return nil }, gateway.WithConnGroup("user:1")))

	// well past the ttl: without renewal at ttl/3 the lease would have lapsed by now.
	time.Sleep(700 * time.Millisecond)

	online, err := presence.Online(ctx, "user:1")
	require.NoError(t, err)
	require.True(t, online)
}

// TestMemoryPresenceJoinWithMetaCopiesInput pins the store-side copy: mutating the map handed
// to JoinWithMeta after the call returns must not reach the recorded member state.
func TestMemoryPresenceJoinWithMetaCopiesInput(t *testing.T) {
	presence := gateway.NewMemoryPresence()
	ctx := context.Background()

	meta := map[string]string{"device": "ios", "app": "1.0"}
	require.NoError(t, presence.JoinWithMeta(ctx, "user:1", "conn-a", meta, time.Minute))

	meta["device"] = "android"
	meta["extra"] = "leak"
	delete(meta, "app")

	entries, err := presence.Entries(ctx, "user:1")
	require.NoError(t, err)
	require.Len(t, entries, 1)
	require.Equal(t, map[string]string{"device": "ios", "app": "1.0"}, entries[0].Meta,
		"stored metadata must not observe a mutation of the caller's map")
}

// TestMemoryPresenceEntriesReturnsCopy pins the return-side copy: mutating a map returned by
// Entries must not corrupt the member state that stays live behind it.
func TestMemoryPresenceEntriesReturnsCopy(t *testing.T) {
	presence := gateway.NewMemoryPresence()
	ctx := context.Background()

	require.NoError(t, presence.JoinWithMeta(ctx, "user:1", "conn-a",
		map[string]string{"device": "ios"}, time.Minute))

	first, err := presence.Entries(ctx, "user:1")
	require.NoError(t, err)
	require.Len(t, first, 1)
	first[0].Meta["device"] = "android"
	first[0].Meta["extra"] = "leak"

	second, err := presence.Entries(ctx, "user:1")
	require.NoError(t, err)
	require.Len(t, second, 1)
	require.Equal(t, map[string]string{"device": "ios"}, second[0].Meta,
		"a mutation of a previously returned map must not survive into internal state")
}

// TestMemoryPresenceRefreshGenRejectsStaleGeneration pins the core PresenceFencer contract: a
// RefreshGen carrying a generation behind what is on record must not extend the lease, must
// not lower the recorded generation, and must report ErrStaleOwner rather than a transient
// failure - this is what stops a node whose owner lease was taken over from using a delayed
// renewal to keep a Presence entry alive past the takeover.
func TestMemoryPresenceRefreshGenRejectsStaleGeneration(t *testing.T) {
	presence := gateway.NewMemoryPresence()
	ctx := context.Background()

	require.NoError(t, presence.RefreshGen(ctx, "user:1", "conn-a", 5, 100*time.Millisecond))

	err := presence.RefreshGen(ctx, "user:1", "conn-a", 3, time.Minute)
	require.ErrorIs(t, err, gateway.ErrStaleOwner)

	// The stale call must not have extended the lease past its original, short ttl.
	time.Sleep(200 * time.Millisecond)
	online, err := presence.Online(ctx, "user:1")
	require.NoError(t, err)
	require.False(t, online, "a stale RefreshGen must not extend the lease")
}

// TestMemoryPresenceRefreshGenAcceptsEqualOrHigherGeneration verifies the non-stale half of
// the same contract: a caller's own periodic renewal (equal generation) and a takeover's
// renewal (higher generation) must both succeed and extend the lease.
func TestMemoryPresenceRefreshGenAcceptsEqualOrHigherGeneration(t *testing.T) {
	presence := gateway.NewMemoryPresence()
	ctx := context.Background()

	require.NoError(t, presence.RefreshGen(ctx, "user:1", "conn-a", 5, 100*time.Millisecond))
	require.NoError(t, presence.RefreshGen(ctx, "user:1", "conn-a", 5, time.Minute), "equal generation must succeed")
	require.NoError(t, presence.RefreshGen(ctx, "user:1", "conn-a", 9, time.Minute), "higher generation must succeed")

	time.Sleep(200 * time.Millisecond)
	online, err := presence.Online(ctx, "user:1")
	require.NoError(t, err)
	require.True(t, online, "a non-stale RefreshGen must extend the lease")
}

// TestMemoryPresenceLeaveGenRejectsStaleGeneration verifies that a delayed Leave from a
// superseded generation cannot remove a membership: the takeover already (re)established the
// connection's presence at a higher generation, so the old owner's stale Leave must be
// rejected rather than undoing it.
func TestMemoryPresenceLeaveGenRejectsStaleGeneration(t *testing.T) {
	presence := gateway.NewMemoryPresence()
	ctx := context.Background()

	require.NoError(t, presence.RefreshGen(ctx, "user:1", "conn-a", 5, time.Minute))

	err := presence.LeaveGen(ctx, "user:1", "conn-a", 3)
	require.ErrorIs(t, err, gateway.ErrStaleOwner)

	members, err := presence.Members(ctx, "user:1")
	require.NoError(t, err)
	require.Equal(t, []string{"conn-a"}, members, "a stale LeaveGen must not remove the member")

	require.NoError(t, presence.LeaveGen(ctx, "user:1", "conn-a", 5), "equal generation must be allowed to leave")
	online, err := presence.Online(ctx, "user:1")
	require.NoError(t, err)
	require.False(t, online)
}

// TestMemoryPresenceRefreshGenAfterLeaveGenCannotResurrect is the split-brain scenario the
// owner lease exists to prevent: node A (generation 1) is delayed, node B takes over at
// generation 2 and explicitly leaves the group (e.g. the user closed the tab on B). A's
// straggling RefreshGen(1) arriving afterward must not resurrect the membership at the old
// generation - it must still see the tombstone LeaveGen(2) left behind and be rejected.
func TestMemoryPresenceRefreshGenAfterLeaveGenCannotResurrect(t *testing.T) {
	presence := gateway.NewMemoryPresence()
	ctx := context.Background()

	require.NoError(t, presence.RefreshGen(ctx, "user:1", "conn-a", 1, time.Minute))
	require.NoError(t, presence.LeaveGen(ctx, "user:1", "conn-a", 2))

	online, err := presence.Online(ctx, "user:1")
	require.NoError(t, err)
	require.False(t, online, "LeaveGen must have taken the member offline")

	err = presence.RefreshGen(ctx, "user:1", "conn-a", 1, time.Minute)
	require.ErrorIs(t, err, gateway.ErrStaleOwner, "a stale RefreshGen must not resurrect a departed group")

	online, err = presence.Online(ctx, "user:1")
	require.NoError(t, err)
	require.False(t, online, "the rejected resurrection attempt must not have made the member live")

	// A RefreshGen at or above the generation LeaveGen recorded is a legitimate rejoin.
	require.NoError(t, presence.RefreshGen(ctx, "user:1", "conn-a", 2, time.Minute))
	online, err = presence.Online(ctx, "user:1")
	require.NoError(t, err)
	require.True(t, online, "a rejoin at a non-stale generation must succeed")
}

// TestMemoryPresencePlainCallsPreserveGeneration verifies that a plain (non-Gen) Join/Refresh
// never resets the generation a RefreshGen call already established: a mix of fenced and
// unfenced callers for the same connID (e.g. an application that only fences Presence but not
// some other codepath) must not let the unfenced side quietly erase fencing state.
func TestMemoryPresencePlainCallsPreserveGeneration(t *testing.T) {
	presence := gateway.NewMemoryPresence()
	ctx := context.Background()

	require.NoError(t, presence.RefreshGen(ctx, "user:1", "conn-a", 7, time.Minute))
	require.NoError(t, presence.Refresh(ctx, "user:1", "conn-a", time.Minute))
	require.NoError(t, presence.Join(ctx, "user:1", "conn-a", time.Minute))

	err := presence.RefreshGen(ctx, "user:1", "conn-a", 4, time.Minute)
	require.ErrorIs(t, err, gateway.ErrStaleOwner, "a plain Join/Refresh must not have reset the recorded generation")
}

// TestMemoryPresenceLeaveGenNoopWhenAbsent pins LeaveGen's "must not fail when the member is
// already gone" contract, matching plain Leave.
func TestMemoryPresenceLeaveGenNoopWhenAbsent(t *testing.T) {
	presence := gateway.NewMemoryPresence()
	ctx := context.Background()

	require.NoError(t, presence.LeaveGen(ctx, "user:1", "conn-a", 1))
	require.NoError(t, presence.LeaveGen(ctx, "user:1", "conn-a", 1))
}

// TestMemoryPresenceRejoinAfterLeaveEmitsJoinEvent verifies that a rejoin after a Leave is
// reported as a fresh PresenceJoin even though a tombstone for the connID still sits in the
// map for generation-fencing purposes: watchers must observe it as "back online", not as
// nothing having happened because a record technically still existed.
func TestMemoryPresenceRejoinAfterLeaveEmitsJoinEvent(t *testing.T) {
	presence := gateway.NewMemoryPresence()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	events, watchCancel, err := presence.Watch(ctx, "user:1")
	require.NoError(t, err)
	defer watchCancel()

	require.NoError(t, presence.Join(ctx, "user:1", "conn-a", time.Minute))
	require.NoError(t, presence.Leave(ctx, "user:1", "conn-a"))
	require.NoError(t, presence.Join(ctx, "user:1", "conn-a", time.Minute))

	var kinds []gateway.PresenceEventKind
	for len(kinds) < 3 {
		select {
		case ev := <-events:
			kinds = append(kinds, ev.Kind)
		case <-time.After(time.Second):
			t.Fatalf("timed out waiting for events, got %v", kinds)
		}
	}
	require.Equal(t, []gateway.PresenceEventKind{gateway.PresenceJoin, gateway.PresenceLeave, gateway.PresenceJoin}, kinds)
}

// TestMemoryPresenceConcurrentGenOperations runs RefreshGen/LeaveGen with monotonically
// increasing generations concurrently across many connections so -race trips on any lock-free
// access the tombstone bookkeeping might have introduced.
func TestMemoryPresenceConcurrentGenOperations(t *testing.T) {
	presence := gateway.NewMemoryPresence()
	ctx := context.Background()

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			connID := fmt.Sprintf("conn-%d", n)
			for gen := uint64(1); gen <= 50; gen++ {
				_ = presence.RefreshGen(ctx, "user:1", connID, gen, time.Minute)
				_, _ = presence.Members(ctx, "user:1")
				_, _ = presence.Online(ctx, "user:1")
				if gen%5 == 0 {
					_ = presence.LeaveGen(ctx, "user:1", connID, gen)
				}
			}
		}(i)
	}
	wg.Wait()
}

// TestMemoryPresenceConcurrentJoinWatchEntries runs Join/Watch/Entries/Members concurrently so
// -race trips on any lock-free access to a shared metadata map. Each worker mutates its input
// map right after JoinWithMeta to expose a missing store-side copy under the race detector.
func TestMemoryPresenceConcurrentJoinWatchEntries(t *testing.T) {
	presence := gateway.NewMemoryPresence()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	events, watchCancel, err := presence.Watch(ctx, "user:1")
	require.NoError(t, err)

	drained := make(chan struct{})
	go func() {
		defer close(drained)
		for range events {
		}
	}()

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			connID := fmt.Sprintf("conn-%d", n)
			for j := 0; j < 200; j++ {
				meta := map[string]string{"n": strconv.Itoa(n)}
				_ = presence.JoinWithMeta(ctx, "user:1", connID, meta, time.Minute)
				meta["n"] = "mutated"

				es, _ := presence.Entries(ctx, "user:1")
				for _, e := range es {
					_ = e.Meta["n"]
				}
				_, _ = presence.Members(ctx, "user:1")
				_, _ = presence.Online(ctx, "user:1")
				if j%3 == 0 {
					_ = presence.Leave(ctx, "user:1", connID)
				}
			}
		}(i)
	}
	wg.Wait()

	watchCancel()
	<-drained
}
