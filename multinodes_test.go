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
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/tochemey/goakt/v4/actor"
	"github.com/tochemey/goakt/v4/log"
	"github.com/tochemey/goakt/v4/testkit"

	gateway "github.com/StringKe/goakt-gateway"
)

// clusterKindActor is a no-op Actor registered as a cluster kind purely to satisfy
// ClusterConfig's requirement that at least one actor kind (or grain) be registered; it
// is never spawned or addressed directly by these tests.
type clusterKindActor struct{}

var _ actor.Actor = (*clusterKindActor)(nil)

func (clusterKindActor) PreStart(*actor.Context) error { return nil }
func (clusterKindActor) Receive(*actor.ReceiveContext) {}
func (clusterKindActor) PostStop(*actor.Context) error { return nil }

// TestGatewayMultiNodesCertIssuance verifies that, when N cluster nodes race to serve
// the same never-before-seen domain on cold start with a Coordinator shared across them,
// the shared CertIssuer is called exactly once cluster-wide (the rest are arbitrated out
// by Coordinator.TryLock and instead read the winner's certificate back), and every node
// ends up serving the very same certificate.
func TestGatewayMultiNodesCertIssuance(t *testing.T) {
	ctx := context.Background()

	multi := testkit.NewMultiNodes(t, log.DiscardLogger, []actor.Actor{&clusterKindActor{}}, nil)
	multi.Start()
	t.Cleanup(multi.Stop)

	const nodeCount = 3
	nodes := make([]*testkit.TestNode, nodeCount)
	for i := range nodeCount {
		nodes[i] = multi.StartNode(ctx, "cert-node")
	}

	// One issuer instance shared by every node's Manager, simulating N replicas that
	// would otherwise all call out to the very same external CA. A single shared
	// Coordinator plays the role a Redis-backed one would play across real processes.
	issuer := &fakeIssuer{ttl: time.Hour}
	coordinator := gateway.NewMemoryCoordinator()

	managers := make([]*gateway.Manager, nodeCount)
	for i, node := range nodes {
		managers[i] = gateway.NewManager(node.ActorSystem(), log.DiscardLogger,
			gateway.WithCertIssuer(issuer),
			gateway.WithCoordinator(coordinator),
			gateway.WithAllowedDomains("cluster-cold-start.example.com"),
			gateway.WithRenewInterval(""),
			gateway.WithRenewBefore(time.Minute),
			gateway.WithIssuanceLockTTL(10*time.Second),
		)
	}

	var wg sync.WaitGroup
	certs := make([][]byte, nodeCount)
	errs := make([]error, nodeCount)
	for i, manager := range managers {
		wg.Add(1)
		go func(i int, manager *gateway.Manager) {
			defer wg.Done()
			cert, err := manager.EnsureCertificate(ctx, "cluster-cold-start.example.com")
			errs[i] = err
			if err == nil && len(cert.Certificate) > 0 {
				certs[i] = cert.Certificate[0]
			}
		}(i, manager)
	}
	wg.Wait()

	for _, err := range errs {
		require.NoError(t, err)
	}
	require.EqualValues(t, 1, issuer.calls.Load(), "exactly one node must call the shared issuer")

	for i := 1; i < nodeCount; i++ {
		require.NotEmpty(t, certs[i])
		require.Equal(t, certs[0], certs[i], "every node must serve the same certificate bytes")
	}
}

// TestGatewayMultiNodesSendToConnection verifies the cross-node delivery path:
// Registry.SendToConnection on a node that does not hold the connection resolves it
// through the cluster-aware actor directory and delivers the payload to the node that
// does.
func TestGatewayMultiNodesSendToConnection(t *testing.T) {
	ctx := context.Background()

	multi := testkit.NewMultiNodes(t, log.DiscardLogger, []actor.Actor{&clusterKindActor{}}, nil)
	multi.Start()
	t.Cleanup(multi.Stop)

	nodeA := multi.StartNode(ctx, "gateway-node-a")
	nodeB := multi.StartNode(ctx, "gateway-node-b")

	registryA := gateway.NewRegistry(nodeA.ActorSystem(), log.DiscardLogger)
	registryB := gateway.NewRegistry(nodeB.ActorSystem(), log.DiscardLogger)

	var mu sync.Mutex
	var received []byte
	send := func(payload []byte) error {
		mu.Lock()
		defer mu.Unlock()
		received = payload
		return nil
	}

	require.NoError(t, registryA.Register(ctx, "cross-node-conn", send))
	// give the cluster's actor directory time to propagate the new registration to
	// node B before it tries to resolve it.
	time.Sleep(2 * time.Second)

	require.False(t, registryB.Has("cross-node-conn"), "connection must not be locally registered on node B")

	err := registryB.SendToConnection(ctx, "cross-node-conn", []byte("cross-node-hello"))
	require.NoError(t, err)

	time.Sleep(500 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, []byte("cross-node-hello"), received)
}

// TestGatewayMultiNodesBroadcast proves that the pub/sub bridge reimplemented in
// bridge.go on GoAkt's public actor.Subscribe/actor.Unsubscribe API (see bridge.go's
// package doc) actually delivers cross-node: a topic member registered on node A must
// receive a Registry.Broadcast issued from node B, with no direct connection between the
// two nodes other than the cluster's topic actor.
func TestGatewayMultiNodesBroadcast(t *testing.T) {
	ctx := context.Background()

	multi := testkit.NewMultiNodes(t, log.DiscardLogger, []actor.Actor{&clusterKindActor{}}, nil)
	multi.Start()
	t.Cleanup(multi.Stop)

	nodeA := multi.StartNode(ctx, "gateway-bridge-node-a")
	nodeB := multi.StartNode(ctx, "gateway-bridge-node-b")

	registryA := gateway.NewRegistry(nodeA.ActorSystem(), log.DiscardLogger)
	registryB := gateway.NewRegistry(nodeB.ActorSystem(), log.DiscardLogger)

	var mu sync.Mutex
	var received [][]byte
	send := func(payload []byte) error {
		mu.Lock()
		defer mu.Unlock()
		received = append(received, payload)
		return nil
	}

	require.NoError(t, registryA.Register(ctx, "bridge-member", send, gateway.WithConnTopics("cross-node-room")))

	_, err := registryB.Broadcast(ctx, "cross-node-room", []byte("hello-from-node-b"))
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(received) == 1
	}, 5*time.Second, 100*time.Millisecond, "node A's topic bridge must deliver a broadcast published on node B")

	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, []byte("hello-from-node-b"), received[0])
}

// TestGatewayMultiNodesConfirmedDelivery verifies that with WithDeliveryConfirmation a
// cross-node SendToConnection uses Ask and returns only after the owning node acknowledges
// the write, rather than the default fire-and-forget Tell.
func TestGatewayMultiNodesConfirmedDelivery(t *testing.T) {
	ctx := context.Background()

	multi := testkit.NewMultiNodes(t, log.DiscardLogger, []actor.Actor{&clusterKindActor{}}, nil)
	multi.Start()
	t.Cleanup(multi.Stop)

	nodeA := multi.StartNode(ctx, "confirm-node-a")
	nodeB := multi.StartNode(ctx, "confirm-node-b")

	registryA := gateway.NewRegistry(nodeA.ActorSystem(), log.DiscardLogger)
	registryB := gateway.NewRegistry(nodeB.ActorSystem(), log.DiscardLogger,
		gateway.WithDeliveryConfirmation(),
		gateway.WithConfirmationTimeout(3*time.Second),
	)

	var mu sync.Mutex
	var received []byte
	send := func(payload []byte) error {
		mu.Lock()
		defer mu.Unlock()
		received = payload
		return nil
	}

	require.NoError(t, registryA.Register(ctx, "confirmed-conn", send))
	time.Sleep(2 * time.Second)
	require.False(t, registryB.Has("confirmed-conn"))

	require.NoError(t, registryB.SendToConnection(ctx, "confirmed-conn", []byte("confirmed-hello")))

	// The Ask returned success, so the payload is already delivered without further waiting.
	mu.Lock()
	require.Equal(t, []byte("confirmed-hello"), received)
	mu.Unlock()
}

// TestGatewayMultiNodesCrossNodeTakeover proves the deterministic reconnect takeover works
// across nodes, which the pre-fix reserve could not do: it only evicted a same-node holder,
// so a takeover landing on a different node hit the cluster-unique connActor name and failed
// its Spawn with ErrActorAlreadyExists. A connection is held on node A; the same id reconnects
// to node B with WithReplaceExisting. Node B must evict A's connection - closing its socket
// and releasing the name - and end up owning the connection, with A holding nothing.
func TestGatewayMultiNodesCrossNodeTakeover(t *testing.T) {
	ctx := context.Background()

	multi := testkit.NewMultiNodes(t, log.DiscardLogger, []actor.Actor{&clusterKindActor{}}, nil)
	multi.Start()
	t.Cleanup(multi.Stop)

	nodeA := multi.StartNode(ctx, "takeover-node-a")
	nodeB := multi.StartNode(ctx, "takeover-node-b")

	registryA := gateway.NewRegistry(nodeA.ActorSystem(), log.DiscardLogger)
	registryB := gateway.NewRegistry(nodeB.ActorSystem(), log.DiscardLogger)
	t.Cleanup(func() { _ = registryA.Close(ctx); _ = registryB.Close(ctx) })

	var mu sync.Mutex
	evicted := ""
	var oldReceived [][]byte
	oldSend := func(payload []byte) error {
		mu.Lock()
		defer mu.Unlock()
		oldReceived = append(oldReceived, payload)
		return nil
	}
	closeHook := func(reason string) {
		mu.Lock()
		defer mu.Unlock()
		evicted = reason
	}

	require.NoError(t, registryA.Register(ctx, "roamer", oldSend, gateway.WithConnCloseHook(closeHook)))
	// let the cluster directory propagate node A's ownership of the actor name to node B.
	time.Sleep(2 * time.Second)

	var newReceived [][]byte
	newSend := func(payload []byte) error {
		mu.Lock()
		defer mu.Unlock()
		newReceived = append(newReceived, payload)
		return nil
	}
	require.NoError(t, registryB.Register(ctx, "roamer", newSend, gateway.WithReplaceExisting()),
		"the cross-node takeover must succeed once node A's connection is evicted")

	require.True(t, registryB.Has("roamer"), "node B must now own the connection")

	// Node A's previous connection must have been force-closed by the takeover and released.
	mu.Lock()
	require.NotEmpty(t, evicted, "node A's old connection must have been force-closed by the takeover")
	mu.Unlock()
	require.Eventually(t, func() bool {
		return !registryA.Has("roamer")
	}, 5*time.Second, 100*time.Millisecond, "node A must release the connection after eviction")

	// A delivery addressed from node A now resolves to node B and reaches the new socket.
	require.Eventually(t, func() bool {
		return registryA.SendToConnection(ctx, "roamer", []byte("after-takeover")) == nil
	}, 5*time.Second, 200*time.Millisecond, "node A must be able to route to the new owner on node B")

	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(newReceived) == 1 && string(newReceived[0]) == "after-takeover"
	}, 5*time.Second, 100*time.Millisecond, "the new owner on node B must receive deliveries")

	mu.Lock()
	require.Empty(t, oldReceived, "the evicted connection must receive nothing after takeover")
	mu.Unlock()
}

// TestGatewayMultiNodesConfirmedGroupRemoteCount verifies that with a shared Presence
// backend and WithDeliveryConfirmation, SendToGroup issued from a node holding none of a
// group's members sets DeliveryResult.Remote to the number of remote members whose owning
// node acknowledged the write, rather than a single fan-out.
func TestGatewayMultiNodesConfirmedGroupRemoteCount(t *testing.T) {
	ctx := context.Background()

	multi := testkit.NewMultiNodes(t, log.DiscardLogger, []actor.Actor{&clusterKindActor{}}, nil)
	multi.Start()
	t.Cleanup(multi.Stop)

	nodeA := multi.StartNode(ctx, "confirm-group-a")
	nodeB := multi.StartNode(ctx, "confirm-group-b")

	// A single Presence instance shared across both registries stands in for a real
	// cluster-wide backend, so node B can enumerate the members node A holds.
	presence := gateway.NewMemoryPresence()
	registryA := gateway.NewRegistry(nodeA.ActorSystem(), log.DiscardLogger, gateway.WithPresence(presence))
	registryB := gateway.NewRegistry(nodeB.ActorSystem(), log.DiscardLogger,
		gateway.WithPresence(presence),
		gateway.WithDeliveryConfirmation(),
		gateway.WithConfirmationTimeout(3*time.Second),
	)
	t.Cleanup(func() { _ = registryA.Close(ctx); _ = registryB.Close(ctx) })

	send := func([]byte) error { return nil }
	require.NoError(t, registryA.Register(ctx, "gm-1", send, gateway.WithConnGroup("squad")))
	require.NoError(t, registryA.Register(ctx, "gm-2", send, gateway.WithConnGroup("squad")))
	time.Sleep(2 * time.Second)

	result, err := registryB.SendToGroup(ctx, "squad", []byte("rally"))
	require.NoError(t, err)
	require.Equal(t, 0, result.Delivered)
	require.Equal(t, 2, result.Remote)
}
