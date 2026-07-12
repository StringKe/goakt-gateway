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

	require.NoError(t, registryA.Register(ctx, "bridge-member", send, "cross-node-room"))
	// give the topic actor's cluster dissemination time to establish before publishing.
	time.Sleep(2 * time.Second)

	require.NoError(t, registryB.Broadcast(ctx, "cross-node-room", []byte("hello-from-node-b")))

	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(received) == 1
	}, 5*time.Second, 100*time.Millisecond, "node A's topic bridge must deliver a broadcast published on node B")

	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, []byte("hello-from-node-b"), received[0])
}
