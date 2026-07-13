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

// This file is a white-box (package gateway) test so it can reach the unexported ownerLeaseTTL
// and ownerLeaseStaleEvictReason seams, and reuses newRaceTestSystem from registry_race_test.go.
// It exercises WithOwnerLease's wiring into Registry.register/Unregister/RegisterHandle, ahead
// of registry.go integration; ownership_test.go covers the ownerLease primitive itself.
package gateway

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/tochemey/goakt/v4/actor"
	"github.com/tochemey/goakt/v4/log"
)

// TestOwnerLeaseConcurrentRegisterAcrossTwoNodesHasSingleOwner is the split-brain regression
// WithOwnerLease exists to close: two Registries backed by two entirely separate, non-clustered
// actor systems - so their actor directories give each other zero exclusion, exactly the "both
// nodes observe no owner and both Spawn" failure mode a PA/EC cluster directory can produce -
// race Register for the very same connection id. Only the linearizable fencing lease they
// share can arbitrate this, and it must let exactly one of them win.
func TestOwnerLeaseConcurrentRegisterAcrossTwoNodesHasSingleOwner(t *testing.T) {
	systemA := newRaceTestSystem(t)
	systemB := newRaceTestSystem(t)
	coord := NewMemoryCoordinator()

	registryA := NewRegistry(systemA, log.DiscardLogger, WithOwnerLease(coord))
	registryB := NewRegistry(systemB, log.DiscardLogger, WithOwnerLease(coord))
	t.Cleanup(func() { _ = registryA.Close(context.Background()) })
	t.Cleanup(func() { _ = registryB.Close(context.Background()) })

	const id = "split-brain-conn"

	start := make(chan struct{})
	var wg sync.WaitGroup
	var errA, errB error
	wg.Add(2)
	go func() {
		defer wg.Done()
		<-start
		errA = registryA.Register(context.Background(), id, func([]byte) error { return nil })
	}()
	go func() {
		defer wg.Done()
		<-start
		errB = registryB.Register(context.Background(), id, func([]byte) error { return nil })
	}()
	close(start)
	wg.Wait()

	winners := 0
	if errA == nil {
		winners++
		require.True(t, registryA.Has(id))
	} else {
		require.ErrorIs(t, errA, ErrOwnerHeld, "a losing Register must fail with ErrOwnerHeld, not silently succeed")
		require.False(t, registryA.Has(id), "a losing Register must not leave a local entry behind")
	}
	if errB == nil {
		winners++
		require.True(t, registryB.Has(id))
	} else {
		require.ErrorIs(t, errB, ErrOwnerHeld, "a losing Register must fail with ErrOwnerHeld, not silently succeed")
		require.False(t, registryB.Has(id), "a losing Register must not leave a local entry behind")
	}
	require.Equal(t, 1, winners, "exactly one of two nodes racing Register for the same id must win - the split-brain bug this mechanism closes let both win")
}

// TestOwnerLeaseCrossNodeTakeoverFencesOldOwnerAndEvictsItLocally proves the other half of the
// contract: a legitimate cross-node takeover (WithReplaceExisting landing on a different node,
// which never has a local entry to race against) must fence the old owner's generation, and the
// old owner's own background lease renewal - not any explicit message from the new owner, since
// these two Registries share no actor system - must notice and evict its now-stale connection.
func TestOwnerLeaseCrossNodeTakeoverFencesOldOwnerAndEvictsItLocally(t *testing.T) {
	previousTTL := ownerLeaseTTL
	ownerLeaseTTL = 60 * time.Millisecond
	t.Cleanup(func() { ownerLeaseTTL = previousTTL })

	systemA := newRaceTestSystem(t)
	systemB := newRaceTestSystem(t)
	coord := NewMemoryCoordinator()

	registryA := NewRegistry(systemA, log.DiscardLogger, WithOwnerLease(coord))
	registryB := NewRegistry(systemB, log.DiscardLogger, WithOwnerLease(coord))
	t.Cleanup(func() { _ = registryA.Close(context.Background()) })
	t.Cleanup(func() { _ = registryB.Close(context.Background()) })

	const id = "takeover-conn"

	var closeReason atomic.Value
	require.NoError(t, registryA.Register(context.Background(), id, func([]byte) error { return nil },
		WithConnCloseHook(func(reason string) { closeReason.Store(reason) })))
	require.True(t, registryA.Has(id))

	// Node B takes over the same id. Its local table has no entry for id at all - it is a
	// different Registry on a different actor system - so this succeeds purely on the strength
	// of the shared owner lease's takeover=true preemption, exactly the cross-node path
	// Register's acquireEntryLease documents.
	require.NoError(t, registryB.Register(context.Background(), id, func([]byte) error { return nil },
		WithReplaceExisting()))
	require.True(t, registryB.Has(id))

	// A's own renewOwnerLeases loop must observe ErrStaleOwner on its next refresh and evict the
	// connection locally, running the close hook with ownerLeaseStaleEvictReason - there is no
	// evict Tell in this scenario for it to react to instead.
	require.Eventually(t, func() bool {
		return !registryA.Has(id)
	}, 5*time.Second, 10*time.Millisecond, "the old owner must self-evict once its background lease refresh observes the takeover")

	require.Equal(t, ownerLeaseStaleEvictReason, closeReason.Load())
	require.True(t, registryB.Has(id), "the new owner must be unaffected by the old owner's self-eviction")
}

// TestConnHandleUnregisterHandleDoesNotClobberTakeover pins the handler entry-guard ABA
// contract RegisterHandle/ConnHandle exist for: a handler that holds a handle for a connection
// which a same-id takeover has since replaced must not be able to tear the new owner down by
// unregistering through its own (now stale) handle.
func TestConnHandleUnregisterHandleDoesNotClobberTakeover(t *testing.T) {
	system := newRaceTestSystem(t)
	registry := NewRegistry(system, log.DiscardLogger)
	t.Cleanup(func() { _ = registry.Close(context.Background()) })
	const id = "handle-aba-conn"

	oldHandle, err := registry.RegisterHandle(context.Background(), id, func([]byte) error { return nil })
	require.NoError(t, err)
	require.True(t, registry.Has(id))

	var newReceived atomic.Int64
	require.NoError(t, registry.Register(context.Background(), id, func([]byte) error {
		newReceived.Add(1)
		return nil
	}, WithReplaceExisting()))
	require.True(t, registry.Has(id))
	require.Equal(t, 1, registry.Len())

	// The stale handler tears down through its own handle, not the bare id.
	require.NoError(t, oldHandle.UnregisterHandle(context.Background()))

	require.True(t, registry.Has(id), "an entry-guarded unregister from a stale handle must not remove the new owner")
	require.Equal(t, 1, registry.Len())
	require.NoError(t, registry.SendToConnection(context.Background(), id, []byte("ping")))
	require.EqualValues(t, 1, newReceived.Load())
}

// TestWithOwnerLeaseUnsupportedCoordinatorFailsRegister proves NewRegistry's lack of an error
// return does not silently drop WithOwnerLease being misconfigured: a Coordinator that does not
// implement LinearizableFencingCoordinator must fail every subsequent Register with
// ErrOwnerLeaseUnsupported.
func TestWithOwnerLeaseUnsupportedCoordinatorFailsRegister(t *testing.T) {
	system := newRaceTestSystem(t)
	registry := NewRegistry(system, log.DiscardLogger, WithOwnerLease(nonCASCoordinator{}))
	t.Cleanup(func() { _ = registry.Close(context.Background()) })

	err := registry.Register(context.Background(), "unsupported-conn", func([]byte) error { return nil })
	require.ErrorIs(t, err, ErrOwnerLeaseUnsupported)
	require.Equal(t, 0, registry.Len())
}

// TestWithOwnerLeaseCASOnlyCoordinatorFailsRegister verifies that atomic CAS by itself is not
// accepted as strict ownership fencing. A provider must explicitly declare a linearizable order
// across all participating processes before WithOwnerLease can use it.
func TestWithOwnerLeaseCASOnlyCoordinatorFailsRegister(t *testing.T) {
	system := newRaceTestSystem(t)
	registry := NewRegistry(system, log.DiscardLogger, WithOwnerLease(newCASOnlyCoordinator()))
	t.Cleanup(func() { _ = registry.Close(context.Background()) })

	err := registry.Register(context.Background(), "cas-only-conn", func([]byte) error { return nil })
	require.ErrorIs(t, err, ErrOwnerLeaseUnsupported)
	require.Equal(t, 0, registry.Len())
}

// TestOwnerLeaseCoordinatorGetFailureRejectsEveryLocalDeliveryPath verifies strict ownership
// fails closed. sendTo is the delivery method connActor.Receive uses after resolving its local
// connection, so covering it verifies the actor path without constructing an actor context.
func TestOwnerLeaseCoordinatorGetFailureRejectsEveryLocalDeliveryPath(t *testing.T) {
	system := newRaceTestSystem(t, actor.WithPubSub())
	coord := &failingGetFencingCoordinator{MemoryCoordinator: NewMemoryCoordinator()}
	registry := NewRegistry(system, log.DiscardLogger, WithOwnerLease(coord))
	t.Cleanup(func() { _ = registry.Close(context.Background()) })

	const id = "owner-get-failure-conn"
	const group = "owner-get-failure-group"
	const topic = "owner-get-failure-topic"
	var delivered atomic.Int64
	require.NoError(t, registry.Register(context.Background(), id, func([]byte) error {
		delivered.Add(1)
		return nil
	}, WithConnGroup(group), WithConnTopics(topic)))

	coord.failGet.Store(true)

	err := registry.SendToConnection(context.Background(), id, []byte("direct"))
	require.ErrorIs(t, err, ErrStaleOwner)

	groupResult, err := registry.SendToGroup(context.Background(), group, []byte("group"))
	require.NoError(t, err)
	require.Equal(t, DeliveryResult{Dropped: 1}, groupResult)

	broadcastResult, err := registry.Broadcast(context.Background(), topic, []byte("broadcast"))
	require.NoError(t, err)
	require.Equal(t, DeliveryResult{Dropped: 1}, broadcastResult)

	err = registry.sendTo(context.Background(), id, []byte("actor"))
	require.ErrorIs(t, err, ErrStaleOwner)
	require.Zero(t, delivered.Load())
}

// TestOwnerLeaseStaleLocalEntryRejectsLocalDelivery is the P0-1 regression: a local entry whose
// owner-lease generation a cross-node takeover has already superseded, but which this node has
// not yet evicted, must not be locally deliverable through any path - SendToConnection's direct
// write and SendToGroup's local fan-out - even though it is still present in this node's table.
// Before deliverOne fenced every local write, only the cross-node connActor path checked
// staleness, leaving local delivery paths free to keep writing to a dying, dispossessed socket.
func TestOwnerLeaseStaleLocalEntryRejectsLocalDelivery(t *testing.T) {
	systemA := newRaceTestSystem(t, actor.WithPubSub())
	systemB := newRaceTestSystem(t, actor.WithPubSub())
	coord := NewMemoryCoordinator()

	registryA := NewRegistry(systemA, log.DiscardLogger, WithOwnerLease(coord))
	registryB := NewRegistry(systemB, log.DiscardLogger, WithOwnerLease(coord))
	t.Cleanup(func() { _ = registryA.Close(context.Background()) })
	t.Cleanup(func() { _ = registryB.Close(context.Background()) })

	const id = "stale-local-delivery-conn"
	const group = "stale-local-delivery-group"

	var delivered atomic.Int64
	require.NoError(t, registryA.Register(context.Background(), id, func([]byte) error {
		delivered.Add(1)
		return nil
	}, WithConnGroup(group)))
	require.True(t, registryA.Has(id))

	// Node B takes over. registryA's local entry is still present - its background lease
	// renewal loop that would eventually evict it has not ticked yet (ownerLeaseTTL defaults to
	// 30s) - exactly the window the finding identifies, and there is no evict Tell in this
	// scenario (separate, non-clustered actor systems) to short-circuit it either.
	require.NoError(t, registryB.Register(context.Background(), id, func([]byte) error { return nil },
		WithConnGroup(group), WithReplaceExisting()))
	require.True(t, registryA.Has(id), "the stale entry must still be locally present for this test to be meaningful")

	err := registryA.SendToConnection(context.Background(), id, []byte("stale"))
	require.ErrorIs(t, err, ErrStaleOwner, "a superseded local entry must reject SendToConnection, not deliver on the old owner's behalf")

	result, err := registryA.SendToGroup(context.Background(), group, []byte("stale-group"))
	require.NoError(t, err)
	require.Zero(t, result.Delivered, "a superseded local entry's group fan-out must not deliver")
	require.Equal(t, 1, result.Dropped, "a superseded local entry must be counted as dropped, not silently ignored")

	require.Zero(t, delivered.Load(), "the stale local entry's socket must never receive a delivery its generation was fenced out of")
}

// TestOwnerLeaseTakeoverFencesPresenceRefreshAndLeave is the P0-2/P0-7 regression: Registry must
// route Presence Refresh and Leave through PresenceFencer's generation-fenced RefreshGen/LeaveGen
// when the configured backend implements it, so a stale owner's delayed refresh cannot keep its
// own superseded membership alive, and its delayed teardown cannot delete a takeover's
// already-(re)established membership.
func TestOwnerLeaseTakeoverFencesPresenceRefreshAndLeave(t *testing.T) {
	systemA := newRaceTestSystem(t, actor.WithPubSub())
	systemB := newRaceTestSystem(t, actor.WithPubSub())
	coord := NewMemoryCoordinator()
	presence := NewMemoryPresence()

	registryA := NewRegistry(systemA, log.DiscardLogger, WithOwnerLease(coord), WithPresence(presence))
	registryB := NewRegistry(systemB, log.DiscardLogger, WithOwnerLease(coord), WithPresence(presence))
	t.Cleanup(func() { _ = registryA.Close(context.Background()) })
	t.Cleanup(func() { _ = registryB.Close(context.Background()) })

	const id = "presence-fence-conn"
	const group = "presence-fence-group"

	handleA, err := registryA.RegisterHandle(context.Background(), id, func([]byte) error { return nil }, WithConnGroup(group))
	require.NoError(t, err)

	handleB, err := registryB.RegisterHandle(context.Background(), id, func([]byte) error { return nil },
		WithConnGroup(group), WithReplaceExisting())
	require.NoError(t, err)
	require.True(t, registryB.Has(id))

	// B's background renewPresence ticker would eventually RefreshGen at its own, higher
	// generation; drive it directly rather than waiting out the real interval.
	require.NoError(t, registryB.refreshPresence(context.Background(), handleB.entry))

	online, err := presence.Online(context.Background(), group)
	require.NoError(t, err)
	require.True(t, online)

	// A's own background refresh, still carrying its now-superseded generation, must be
	// rejected rather than reviving its stale claim on the membership.
	err = registryA.refreshPresence(context.Background(), handleA.entry)
	require.ErrorIs(t, err, ErrStaleOwner, "a superseded owner's presence refresh must be fenced")

	// A's delayed teardown (its own stale-owner eviction, or a normal Unregister racing the
	// takeover) must not be able to undo B's already-(re)established membership.
	require.NoError(t, registryA.unregisterEntry(context.Background(), handleA.entry))

	online, err = presence.Online(context.Background(), group)
	require.NoError(t, err)
	require.True(t, online, "a stale owner's teardown must not remove the new owner's presence membership")

	members, err := presence.Members(context.Background(), group)
	require.NoError(t, err)
	require.Contains(t, members, id)
}

// TestOwnerLeaseFailedTakeoverRestoresOriginalOwnerGeneration is the P0-6 regression: a
// WithReplaceExisting takeover whose physical eviction never completes must restore the
// coordinator record to whichever owner it actually preempted, instead of leaving its own failed
// claim (or a bare tombstone) permanently fencing that owner's own subsequent lease refresh with
// ErrStaleOwner - which would kill a connection that was never actually taken over.
//
// The real-world trigger register() guards against is ErrTakeoverTimeout: a cluster actor-name
// collision whose evict Tell the occupying node never processes within takeoverEvictTimeout (a
// real 10s wait against an actual clustered actor system this test does not stand up). Register's
// abort branch, however, keys only on "a lease was granted but spawnConnActor then failed for any
// reason" (see the entry.dead || leaseErr != nil || spawnErr != nil branch in register()), so
// stopping the actor system between the lease grant and the spawn attempt exercises the exact
// same code path deterministically and fast: Spawn fails immediately with
// ErrActorSystemNotStarted, a non-ErrActorAlreadyExists error, which spawnConnActor already
// returns without entering its retry loop regardless of which error it is.
func TestOwnerLeaseFailedTakeoverRestoresOriginalOwnerGeneration(t *testing.T) {
	system := newRaceTestSystem(t)
	coord := NewMemoryCoordinator()

	const id = "failed-takeover-conn"

	// Node A already legitimately owns id at generation 1.
	original := newOwnerLease(coord, "node-a", time.Minute)
	generation, err := original.acquire(context.Background(), id, false)
	require.NoError(t, err)
	require.EqualValues(t, 1, generation)

	registryB := NewRegistry(system, log.DiscardLogger, WithOwnerLease(coord))
	t.Cleanup(func() { _ = registryB.Close(context.Background()) })

	// Stopping the actor system makes the physical takeover (the spawn following a successful
	// lease preemption) fail immediately and deterministically, standing in for the real
	// ErrTakeoverTimeout trigger without a 10-second wait or a live cluster.
	require.NoError(t, system.Stop(context.Background()))

	err = registryB.Register(context.Background(), id, func([]byte) error { return nil }, WithReplaceExisting())
	require.Error(t, err, "the takeover's spawn must fail once the actor system has stopped")
	require.False(t, registryB.Has(id), "a failed takeover must not leave a local entry behind")

	// Node A - never actually dislodged, since B's takeover never got past the lease
	// preemption - must still be able to refresh its own lease at its original generation
	// instead of being fenced out by B's failed attempt.
	require.NoError(t, original.refresh(context.Background(), id, 1),
		"a failed takeover must restore the original owner's lease instead of permanently fencing it out")

	nodeID, currentGeneration, ok, err := original.ownerNode(context.Background(), id)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, "node-a", nodeID)
	require.EqualValues(t, 1, currentGeneration)
}

// TestOwnerLeaseTakeoverAdvancesOutboxGenerationFloor is the P1/finding-4 registry-integration
// regression: Register must raise a configured Outbox's Ack fencing floor itself the moment a
// takeover's lease acquisition is confirmed, via OutboxGenerationAdvancer, rather than leaving
// the floor to be raised only by the new owner's own first Ack.
func TestOwnerLeaseTakeoverAdvancesOutboxGenerationFloor(t *testing.T) {
	systemA := newRaceTestSystem(t)
	systemB := newRaceTestSystem(t)
	coord := NewMemoryCoordinator()
	outbox := NewMemoryOutbox()

	registryA := NewRegistry(systemA, log.DiscardLogger, WithOwnerLease(coord), WithOutbox(outbox))
	registryB := NewRegistry(systemB, log.DiscardLogger, WithOwnerLease(coord), WithOutbox(outbox))
	t.Cleanup(func() { _ = registryA.Close(context.Background()) })
	t.Cleanup(func() { _ = registryB.Close(context.Background()) })

	const id = "outbox-advance-conn"

	require.NoError(t, registryA.Register(context.Background(), id, func([]byte) error { return nil }))
	msgID, _, err := outbox.Append(context.Background(), id, []byte("m1"))
	require.NoError(t, err)

	require.NoError(t, registryB.Register(context.Background(), id, func([]byte) error { return nil }, WithReplaceExisting()))
	require.True(t, registryB.Has(id))

	// A's in-flight ack, still carrying its pre-takeover generation 1, must be rejected: B's
	// takeover already raised the Outbox's fencing floor to its own (higher) generation, even
	// though B has not acked anything of its own yet.
	err = registryA.Ack(context.Background(), id, msgID)
	require.ErrorIs(t, err, ErrStaleOwner, "a stale owner's ack after a takeover must be fenced by the Outbox's advanced generation floor")

	msgs, err := outbox.Unacked(context.Background(), id)
	require.NoError(t, err)
	require.Len(t, msgs, 1, "the stale-rejected ack must not have removed the message")

	require.NoError(t, registryB.Ack(context.Background(), id, msgID))
	msgs, err = outbox.Unacked(context.Background(), id)
	require.NoError(t, err)
	require.Empty(t, msgs, "the new owner's own ack must still succeed normally")
}

// nonCASCoordinator implements Coordinator but deliberately not CASCoordinator, so
// TestWithOwnerLeaseUnsupportedCoordinatorFailsRegister can drive the ErrOwnerLeaseUnsupported
// path without depending on MemoryCoordinator (which does implement CASCoordinator) not
// implementing it.
type nonCASCoordinator struct{}

func (nonCASCoordinator) Get(context.Context, string) ([]byte, bool, error)        { return nil, false, nil }
func (nonCASCoordinator) Put(context.Context, string, []byte, time.Duration) error { return nil }
func (nonCASCoordinator) TryLock(context.Context, string, time.Duration) (func(context.Context) error, error) {
	return nil, ErrLockNotAcquired
}

var _ Coordinator = nonCASCoordinator{}

// casOnlyCoordinator has atomic CAS but deliberately makes no linearizable fencing claim.
// This models a replicated backend whose asynchronous failover can discard an acknowledged CAS.
type casOnlyCoordinator struct {
	inner *MemoryCoordinator
}

func newCASOnlyCoordinator() *casOnlyCoordinator {
	return &casOnlyCoordinator{inner: NewMemoryCoordinator()}
}

func (c *casOnlyCoordinator) Get(ctx context.Context, key string) ([]byte, bool, error) {
	return c.inner.Get(ctx, key)
}

func (c *casOnlyCoordinator) Put(ctx context.Context, key string, value []byte, ttl time.Duration) error {
	return c.inner.Put(ctx, key, value, ttl)
}

func (c *casOnlyCoordinator) TryLock(ctx context.Context, key string, ttl time.Duration) (func(context.Context) error, error) {
	return c.inner.TryLock(ctx, key, ttl)
}

func (c *casOnlyCoordinator) CompareAndSwap(ctx context.Context, key string, expected, newValue []byte, ttl time.Duration) (bool, error) {
	return c.inner.CompareAndSwap(ctx, key, expected, newValue, ttl)
}

var _ CASCoordinator = (*casOnlyCoordinator)(nil)

// failingGetFencingCoordinator is a linearizable test double that can make only ownership
// reads fail after registration, isolating delivery's fail-closed contract.
type failingGetFencingCoordinator struct {
	*MemoryCoordinator
	failGet atomic.Bool
}

func (c *failingGetFencingCoordinator) Get(ctx context.Context, key string) ([]byte, bool, error) {
	if c.failGet.Load() {
		return nil, false, errInjectedOwnerLeaseGet
	}
	return c.MemoryCoordinator.Get(ctx, key)
}

func (*failingGetFencingCoordinator) LinearizableFencing() {}

var (
	errInjectedOwnerLeaseGet                                = errors.New("injected owner lease get failure")
	_                        LinearizableFencingCoordinator = (*failingGetFencingCoordinator)(nil)
)

// failingAdvanceOutbox makes registration fail after the actor has been spawned but before
// traffic can be opened, exercising the rollback boundary around AdvanceGeneration.
type failingAdvanceOutbox struct {
	*MemoryOutbox
}

func (*failingAdvanceOutbox) AdvanceGeneration(context.Context, string, uint64) error {
	return errInjectedAdvanceGeneration
}

var (
	errInjectedAdvanceGeneration                          = errors.New("injected advance generation failure")
	_                            OutboxGenerationAdvancer = (*failingAdvanceOutbox)(nil)
)

// TestOwnerLeaseAdvanceGenerationFailureRollsBackRegistration verifies an outbox fencing
// failure leaves no locally reachable entry or actor-backed delivery path.
func TestOwnerLeaseAdvanceGenerationFailureRollsBackRegistration(t *testing.T) {
	system := newRaceTestSystem(t)
	registry := NewRegistry(system, log.DiscardLogger,
		WithOwnerLease(NewMemoryCoordinator()),
		WithOutbox(&failingAdvanceOutbox{MemoryOutbox: NewMemoryOutbox()}),
	)
	t.Cleanup(func() { _ = registry.Close(context.Background()) })

	var delivered atomic.Int64
	err := registry.Register(context.Background(), "advance-failure-conn", func([]byte) error {
		delivered.Add(1)
		return nil
	})
	require.ErrorIs(t, err, errInjectedAdvanceGeneration)
	require.False(t, registry.Has("advance-failure-conn"))
	require.Equal(t, 0, registry.Len())
	require.ErrorIs(t, registry.SendToConnection(context.Background(), "advance-failure-conn", []byte("must-not-deliver")), ErrConnectionNotFound)
	require.Zero(t, delivered.Load())
}
