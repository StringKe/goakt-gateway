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

// This file is a white-box (package gateway) test so it can reach the unexported ownerLease
// type directly, ahead of registry.go wiring it up to WithOwnerLease.
package gateway

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestOwnerLeaseAcquireFreshConnGrantsGenerationOne(t *testing.T) {
	l := newOwnerLease(NewMemoryCoordinator(), "node-a", time.Minute)
	ctx := context.Background()

	generation, err := l.acquire(ctx, "conn-1", false)
	require.NoError(t, err)
	require.EqualValues(t, 1, generation)
}

func TestOwnerLeaseAcquireWithoutTakeoverFailsWhileHeldAndUnexpired(t *testing.T) {
	l := newOwnerLease(NewMemoryCoordinator(), "node-a", time.Minute)
	ctx := context.Background()

	_, err := l.acquire(ctx, "conn-1", false)
	require.NoError(t, err)

	other := newOwnerLease(l.coord, "node-b", time.Minute)
	_, err = other.acquire(ctx, "conn-1", false)
	require.ErrorIs(t, err, ErrOwnerHeld)
}

func TestOwnerLeaseAcquireWithTakeoverPreemptsAndBumpsGeneration(t *testing.T) {
	l := newOwnerLease(NewMemoryCoordinator(), "node-a", time.Minute)
	ctx := context.Background()

	firstGeneration, err := l.acquire(ctx, "conn-1", false)
	require.NoError(t, err)

	other := newOwnerLease(l.coord, "node-b", time.Minute)
	secondGeneration, err := other.acquire(ctx, "conn-1", true)
	require.NoError(t, err)
	require.Greater(t, secondGeneration, firstGeneration)

	nodeID, generation, ok, err := other.ownerNode(ctx, "conn-1")
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, "node-b", nodeID)
	require.Equal(t, secondGeneration, generation)
}

func TestOwnerLeaseAcquireAfterExpirySucceedsWithoutTakeover(t *testing.T) {
	l := newOwnerLease(NewMemoryCoordinator(), "node-a", 30*time.Millisecond)
	ctx := context.Background()

	firstGeneration, err := l.acquire(ctx, "conn-1", false)
	require.NoError(t, err)

	other := newOwnerLease(l.coord, "node-b", time.Minute)
	require.Eventually(t, func() bool {
		_, err := other.acquire(ctx, "conn-1", false)
		return err == nil
	}, 3*time.Second, 10*time.Millisecond, "acquire must succeed once the prior holder's lease has expired, even without takeover")

	nodeID, generation, ok, err := other.ownerNode(ctx, "conn-1")
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, "node-b", nodeID)
	require.Greater(t, generation, firstGeneration)
}

func TestOwnerLeaseRefreshExtendsWithoutChangingGeneration(t *testing.T) {
	l := newOwnerLease(NewMemoryCoordinator(), "node-a", time.Minute)
	ctx := context.Background()

	generation, err := l.acquire(ctx, "conn-1", false)
	require.NoError(t, err)

	require.NoError(t, l.refresh(ctx, "conn-1", generation))

	_, currentGeneration, ok, err := l.ownerNode(ctx, "conn-1")
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, generation, currentGeneration)
}

func TestOwnerLeaseRefreshRejectsStaleGenerationAfterTakeover(t *testing.T) {
	l := newOwnerLease(NewMemoryCoordinator(), "node-a", time.Minute)
	ctx := context.Background()

	staleGeneration, err := l.acquire(ctx, "conn-1", false)
	require.NoError(t, err)

	other := newOwnerLease(l.coord, "node-b", time.Minute)
	_, err = other.acquire(ctx, "conn-1", true)
	require.NoError(t, err)

	// The old owner's background refresh, still carrying its now-superseded generation,
	// must be fenced out rather than reviving the old owner's claim.
	err = l.refresh(ctx, "conn-1", staleGeneration)
	require.ErrorIs(t, err, ErrStaleOwner)
}

func TestOwnerLeaseRefreshRejectsUnknownConnection(t *testing.T) {
	l := newOwnerLease(NewMemoryCoordinator(), "node-a", time.Minute)
	ctx := context.Background()

	err := l.refresh(ctx, "never-acquired", 1)
	require.ErrorIs(t, err, ErrStaleOwner)
}

func TestOwnerLeaseReleaseFreesTheConnectionForImmediateReacquireWithoutTakeover(t *testing.T) {
	l := newOwnerLease(NewMemoryCoordinator(), "node-a", time.Minute)
	ctx := context.Background()

	generation, err := l.acquire(ctx, "conn-1", false)
	require.NoError(t, err)
	require.NoError(t, l.release(ctx, "conn-1", generation))

	other := newOwnerLease(l.coord, "node-b", time.Minute)
	newGeneration, err := other.acquire(ctx, "conn-1", false)
	require.NoError(t, err, "release must free the connection for a plain (non-takeover) acquire")
	require.Greater(t, newGeneration, generation)
}

func TestOwnerLeaseReleaseWithStaleGenerationIsNoOpAndDoesNotClobberNewOwner(t *testing.T) {
	l := newOwnerLease(NewMemoryCoordinator(), "node-a", time.Minute)
	ctx := context.Background()

	staleGeneration, err := l.acquire(ctx, "conn-1", false)
	require.NoError(t, err)

	other := newOwnerLease(l.coord, "node-b", time.Minute)
	newGeneration, err := other.acquire(ctx, "conn-1", true)
	require.NoError(t, err)

	// The old owner's belated release call, carrying its superseded generation, must not
	// be able to tear down the new owner's lease.
	require.NoError(t, l.release(ctx, "conn-1", staleGeneration))

	nodeID, generation, ok, err := other.ownerNode(ctx, "conn-1")
	require.NoError(t, err)
	require.True(t, ok, "a stale release must not evict the current owner's lease")
	require.Equal(t, "node-b", nodeID)
	require.Equal(t, newGeneration, generation)
}

func TestOwnerLeaseReleaseOfUnknownConnectionIsNoOp(t *testing.T) {
	l := newOwnerLease(NewMemoryCoordinator(), "node-a", time.Minute)
	require.NoError(t, l.release(context.Background(), "never-acquired", 1))
}

func TestOwnerLeaseOwnerNodeReportsNotOwnedForAbsentExpiredOrUnknownConnection(t *testing.T) {
	l := newOwnerLease(NewMemoryCoordinator(), "node-a", 30*time.Millisecond)
	ctx := context.Background()

	_, _, ok, err := l.ownerNode(ctx, "never-acquired")
	require.NoError(t, err)
	require.False(t, ok)

	_, err = l.acquire(ctx, "conn-1", false)
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		_, _, ok, err := l.ownerNode(ctx, "conn-1")
		return err == nil && !ok
	}, 3*time.Second, 10*time.Millisecond, "ownerNode must report ok=false once the lease has expired")
}

// TestOwnerLeaseConcurrentAcquireOnlyOneWinnerPerGeneration is the split-brain regression
// this whole mechanism exists to close: many nodes racing acquire(takeover=false) for a
// brand-new connection id must not all believe they won. Exactly one call may succeed;
// every other must observe ErrOwnerHeld.
func TestOwnerLeaseConcurrentAcquireOnlyOneWinnerPerGeneration(t *testing.T) {
	coord := NewMemoryCoordinator()
	ctx := context.Background()

	const concurrency = 20
	var wins atomic.Int64
	var wg sync.WaitGroup
	for i := range concurrency {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			l := newOwnerLease(coord, "node-"+string(rune('a'+i)), time.Minute)
			if _, err := l.acquire(ctx, "conn-1", false); err == nil {
				wins.Add(1)
			}
		}(i)
	}
	wg.Wait()

	require.EqualValues(t, 1, wins.Load(), "exactly one concurrent acquire call must win a brand-new connection id")
}

// TestOwnerLeaseConcurrentTakeoverOnlyOneWinnerPerRound proves the same property when many
// nodes race a forced takeover against an already-held, unexpired lease: exactly one of the
// racing CompareAndSwap calls must land, whichever generation it produces.
func TestOwnerLeaseConcurrentTakeoverOnlyOneWinnerPerRound(t *testing.T) {
	coord := NewMemoryCoordinator()
	ctx := context.Background()

	holder := newOwnerLease(coord, "node-holder", time.Minute)
	_, err := holder.acquire(ctx, "conn-1", false)
	require.NoError(t, err)

	const concurrency = 20
	generations := make(chan uint64, concurrency)
	var wg sync.WaitGroup
	for i := range concurrency {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			l := newOwnerLease(coord, "node-"+string(rune('a'+i)), time.Minute)
			if generation, err := l.acquire(ctx, "conn-1", true); err == nil {
				generations <- generation
			}
		}(i)
	}
	wg.Wait()
	close(generations)

	seen := make(map[uint64]int)
	for generation := range generations {
		seen[generation]++
	}
	for generation, count := range seen {
		require.Equal(t, 1, count, "generation %d must have been granted to exactly one racing takeover caller", generation)
	}
}

// TestOwnerLeaseAbortTakeoverRestoresPriorOwner is the primitive-level half of the P0-6
// regression: abortTakeover must give back the exact ownership a takeover's acquire call
// preempted, with a fresh expiry, so the original owner's own subsequent refresh at its
// original generation succeeds rather than being permanently fenced out by a takeover that
// never actually completed.
func TestOwnerLeaseAbortTakeoverRestoresPriorOwner(t *testing.T) {
	coord := NewMemoryCoordinator()
	ctx := context.Background()

	original := newOwnerLease(coord, "node-a", time.Minute)
	_, err := original.acquire(ctx, "conn-1", false)
	require.NoError(t, err)

	newOwner := newOwnerLease(coord, "node-b", time.Minute)
	acq, err := newOwner.acquireDetailed(ctx, "conn-1", true)
	require.NoError(t, err)
	require.True(t, acq.hadPrior)

	require.NoError(t, newOwner.abortTakeover(ctx, "conn-1", acq))

	// The original owner must be able to refresh at its original generation again, exactly as
	// if the takeover attempt had never happened.
	require.NoError(t, original.refresh(ctx, "conn-1", 1))

	nodeID, generation, ok, err := original.ownerNode(ctx, "conn-1")
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, "node-a", nodeID)
	require.EqualValues(t, 1, generation)
}

// TestOwnerLeaseAbortTakeoverWithNoPriorOwnerTombstones proves abortTakeover degrades to a plain
// release (a tombstone, not a restore) when the takeover it is undoing preempted nothing - a
// brand new connection id, or one whose previous lease had already lapsed - since there is no
// prior owner to give back.
func TestOwnerLeaseAbortTakeoverWithNoPriorOwnerTombstones(t *testing.T) {
	coord := NewMemoryCoordinator()
	ctx := context.Background()

	l := newOwnerLease(coord, "node-a", time.Minute)
	acq, err := l.acquireDetailed(ctx, "conn-1", true)
	require.NoError(t, err)
	require.False(t, acq.hadPrior)

	require.NoError(t, l.abortTakeover(ctx, "conn-1", acq))

	_, _, ok, err := l.ownerNode(ctx, "conn-1")
	require.NoError(t, err)
	require.False(t, ok, "a tombstoned record must not read back as a live owner")

	// A fresh acquire must succeed exactly as if the aborted attempt's claim were released - the
	// tombstone still carries acq's generation forward for monotonicity (see
	// generationRetentionMultiplier), the same guarantee a plain release makes.
	other := newOwnerLease(coord, "node-b", time.Minute)
	generation, err := other.acquire(ctx, "conn-1", false)
	require.NoError(t, err)
	require.Greater(t, generation, acq.generation, "a fresh acquire after the tombstone must still move the generation forward")
}

// TestOwnerLeaseAbortTakeoverIsNoOpAfterSupersession proves abortTakeover never clobbers a
// record a later, real takeover has since written: it must only revert the exact acquisition it
// was given, identified by (nodeID, generation), not whatever currently occupies the key.
func TestOwnerLeaseAbortTakeoverIsNoOpAfterSupersession(t *testing.T) {
	coord := NewMemoryCoordinator()
	ctx := context.Background()

	original := newOwnerLease(coord, "node-a", time.Minute)
	_, err := original.acquire(ctx, "conn-1", false)
	require.NoError(t, err)

	failedTakeover := newOwnerLease(coord, "node-b", time.Minute)
	acq, err := failedTakeover.acquireDetailed(ctx, "conn-1", true)
	require.NoError(t, err)

	// A third node's real takeover lands before node B gets around to aborting its own.
	thirdOwner := newOwnerLease(coord, "node-c", time.Minute)
	thirdGeneration, err := thirdOwner.acquire(ctx, "conn-1", true)
	require.NoError(t, err)

	require.NoError(t, failedTakeover.abortTakeover(ctx, "conn-1", acq))

	nodeID, generation, ok, err := thirdOwner.ownerNode(ctx, "conn-1")
	require.NoError(t, err)
	require.True(t, ok, "a stale abortTakeover must not clobber a real subsequent takeover")
	require.Equal(t, "node-c", nodeID)
	require.Equal(t, thirdGeneration, generation)
}

func TestOwnerLeaseEncodeDecodeValueRoundTrips(t *testing.T) {
	value := encodeLeaseValue("node-a", 42, 1234567890123)

	nodeID, generation, expiresAtMs, ok := decodeLeaseValue(value)
	require.True(t, ok)
	require.Equal(t, "node-a", nodeID)
	require.EqualValues(t, 42, generation)
	require.EqualValues(t, 1234567890123, expiresAtMs)
}

func TestOwnerLeaseDecodeValueRejectsTooShortInput(t *testing.T) {
	_, _, _, ok := decodeLeaseValue([]byte("short"))
	require.False(t, ok)
}
