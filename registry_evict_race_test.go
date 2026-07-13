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

// This file is a white-box (package gateway) test so it can reach the unexported
// unregisterTeardownBarrier and evictLocalBarrier seams and drive the evict-vs-reused-id ABA
// race deterministically. It needs a cluster-mode actor system (a testkit node) because only
// there does a duplicate connActor Spawn fail with ErrActorAlreadyExists and thus drive the
// takeover evict path the bug lived on.
package gateway

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/tochemey/goakt/v4/actor"
	"github.com/tochemey/goakt/v4/log"
	"github.com/tochemey/goakt/v4/testkit"
)

// evictKindActor is a no-op actor registered as a cluster kind so a testkit node starts in
// cluster mode. It is never spawned or addressed directly.
type evictKindActor struct{}

var _ actor.Actor = (*evictKindActor)(nil)

func (evictKindActor) PreStart(*actor.Context) error { return nil }
func (evictKindActor) Receive(*actor.ReceiveContext) {}
func (evictKindActor) PostStop(*actor.Context) error { return nil }

// TestEvictLocalDoesNotClobberSameNodeReconnect pins the availability regression the evict
// identity guard closes. On a clustered node an old connection's socket dies and its
// Unregister removes it from the table but parks before shutting down its backing actor, so
// that actor still owns the cluster-unique connActor name. The same id then reconnects on the
// same node with WithReplaceExisting: its reserve sees no local entry, inserts the new one, and
// its Spawn hits ErrActorAlreadyExists on the still-held name, so it drives the takeover evict
// loop against the old, still-alive actor.
//
// Pre-fix, that actor's evictLocal keyed on the bare id: it read the freshly reconnected entry,
// force-closed its brand-new socket and marked it dead, so the legitimate reconnect was
// spuriously rejected with ErrConnectionClosed. The fix keys the evict on entry identity, so
// the old actor's evict is a no-op against the newcomer, its own PostStop releases the name,
// and the reconnect succeeds.
func TestEvictLocalDoesNotClobberSameNodeReconnect(t *testing.T) {
	ctx := context.Background()

	multi := testkit.NewMultiNodes(t, log.DiscardLogger, []actor.Actor{&evictKindActor{}}, nil)
	multi.Start()
	t.Cleanup(multi.Stop)

	node := multi.StartNode(ctx, "evict-race-node")
	registry := NewRegistry(node.ActorSystem(), log.DiscardLogger)
	t.Cleanup(func() { _ = registry.Close(ctx) })

	const id = "evict-race-conn"

	require.NoError(t, registry.Register(ctx, id, func([]byte) error { return nil }))
	// Let the cluster directory settle so the old actor's name is definitely registered when
	// the reconnect's Spawn checks for it.
	time.Sleep(2 * time.Second)

	// Park the old connection's teardown after it has left the table but before its actor is
	// shut down, so the actor keeps holding connActorName(id).
	teardownReached := make(chan struct{})
	teardownProceed := make(chan struct{})
	unregisterTeardownBarrier = func(gotID string) {
		if gotID != id {
			return
		}
		close(teardownReached)
		<-teardownProceed
	}
	t.Cleanup(func() { unregisterTeardownBarrier = nil })

	// Observe the takeover's evict landing on the old actor, so the test releases the parked
	// teardown only once the pre-fix, buggy evict would already have run against the new entry.
	evicted := make(chan struct{}, 8)
	evictLocalBarrier = func(gotID string) {
		if gotID != id {
			return
		}
		select {
		case evicted <- struct{}{}:
		default:
		}
	}
	t.Cleanup(func() { evictLocalBarrier = nil })

	// The old socket dies: its handler Unregisters, which parks in teardown.
	unregDone := make(chan struct{})
	go func() {
		defer close(unregDone)
		_ = registry.Unregister(ctx, id)
	}()
	<-teardownReached

	// The client reconnects on the same node while the old actor still holds the name.
	var newClosed atomic.Bool
	regErrCh := make(chan error, 1)
	go func() {
		regErrCh <- registry.Register(ctx, id, func([]byte) error { return nil },
			WithReplaceExisting(),
			WithConnCloseHook(func(string) { newClosed.Store(true) }))
	}()

	// Wait for the reconnect's takeover loop to drive an evict onto the old, still-alive actor.
	// At this point the pre-fix code has already force-closed and killed the new entry.
	select {
	case <-evicted:
	case <-time.After(8 * time.Second):
		t.Fatal("the reconnect's takeover loop never issued an evict against the old actor")
	}

	// Release the old teardown so its actor shuts down and frees the name, letting the
	// reconnect's Spawn finally succeed.
	close(teardownProceed)
	<-unregDone

	select {
	case err := <-regErrCh:
		require.NoError(t, err, "the same-node reconnect must not be rejected by the old connection's own evict")
	case <-time.After(15 * time.Second):
		t.Fatal("the reconnect never completed")
	}

	require.True(t, registry.Has(id), "the reconnected connection must own the id")
	require.False(t, newClosed.Load(), "the reconnected socket must not be force-closed by the old connection's evict")
	require.Equal(t, 1, registry.Len())
}
