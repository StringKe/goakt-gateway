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

// Command cluster is a runnable demonstration of the reason this package exists: a
// connection can be registered on one node and receive a message handed to a completely
// different node.
//
// It boots three GoAkt actor systems in a single OS process, joins them into one cluster
// with the static discovery provider, and gives each system its own gateway.Registry and
// its own HTTP listener - so from the outside it looks exactly like three independent
// gateway-echo replicas behind a load balancer, just without the extra terminals or
// containers that would take to actually run three of those. See README.md for the
// two-tier delivery model this exercises and how to tell local delivery from remote
// delivery while it runs.
package main

import (
	"context"
	"fmt"
	"html"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/tochemey/goakt/v4/actor"
	"github.com/tochemey/goakt/v4/discovery/static"
	golog "github.com/tochemey/goakt/v4/log"
	"github.com/tochemey/goakt/v4/remote"

	gateway "github.com/StringKe/goakt-gateway"
)

// nodeCount is fixed at three rather than made a flag: the point of the sample is the
// interaction between exactly the node the client connects to, the node it does not, and
// a third node used purely to prove the first two aren't secretly talking to each other
// directly.
const nodeCount = 3

// Port bases below are spread far apart and away from common dev-server ranges (3000,
// 8080, 9090, ...) purely to avoid clashing with whatever else might be running on the
// machine this is demoed on; nothing about the values themselves is meaningful.
const (
	baseDiscoveryPort = 24101
	basePeersPort     = 24201
	baseRemotingPort  = 24301
	baseHTTPPort      = 18081
)

// actorSystemName is deliberately identical across every node. GoAkt derives its
// memberlist gossip label from the actor system's name (internal/cluster/cluster.go's
// mconfig.Label = "prefix-"+strings.ToLower(name)), so peers whose systems are named
// differently reject each other's gossip streams outright and never form one cluster -
// the actor systems must share a name; gatewayNode.name below is what actually
// distinguishes nodes in this sample's logs and HTTP addresses.
const actorSystemName = "gateway-cluster-demo"

// clusterKindActor is a no-op Actor registered as a cluster kind purely to satisfy
// ClusterConfig's requirement that at least one actor kind be registered; the gateway
// package's own connActor is spawned dynamically per connection and needs no static
// registration, so this exists only to make ClusterConfig.WithKinds happy.
type clusterKindActor struct{}

var _ actor.Actor = (*clusterKindActor)(nil)

func (clusterKindActor) PreStart(*actor.Context) error { return nil }
func (clusterKindActor) Receive(*actor.ReceiveContext) {}
func (clusterKindActor) PostStop(*actor.Context) error { return nil }

// gatewayNode bundles one actor system, its Registry, and its own logger together. Each
// node in this sample is a self-contained "replica" - nothing here is shared between
// nodes except the cluster membership formed by the static discovery provider.
type gatewayNode struct {
	name     string
	system   actor.ActorSystem
	registry *gateway.Registry
	logger   *log.Logger
	httpAddr string
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// Every node's static discovery provider is handed the very same host list,
	// including its own: DiscoverPeers returns this list verbatim and the cluster
	// engine works out which entry is "self" from the bound address, so there is no
	// special-casing needed here beyond listing every discovery port up front.
	hosts := make([]string, nodeCount)
	for i := range nodeCount {
		hosts[i] = fmt.Sprintf("127.0.0.1:%d", baseDiscoveryPort+i)
	}

	nodes := make([]*gatewayNode, nodeCount)
	for i := range nodeCount {
		node, err := startNode(ctx, i, hosts)
		if err != nil {
			log.Fatalf("failed to start node %d: %v", i+1, err)
		}
		nodes[i] = node
		// Mirrors the pause the module's own multi-node tests take between starting
		// cluster members (see multinodes_test.go): the static provider's peer list
		// is known immediately, but SWIM-style membership gossip and the actor
		// directory's replication both need a moment to actually converge before the
		// next node - or a client - starts relying on cross-node lookups.
		time.Sleep(2 * time.Second)
	}

	servers := make([]*gateway.Server, nodeCount)
	for i, node := range nodes {
		server, err := gateway.NewServer(node.httpAddr, buildMux(node))
		if err != nil {
			log.Fatalf("failed to configure HTTP server for %s: %v", node.name, err)
		}
		servers[i] = server

		go func(node *gatewayNode, server *gateway.Server) {
			node.logger.Printf("listening on http://%s (ws: /ws?id=<conn-id>, send: /send?id=<conn-id>&msg=<text>)", node.httpAddr)
			if err := server.ListenAndServe(ctx); err != nil && err != http.ErrServerClosed {
				node.logger.Fatalf("http server stopped unexpectedly: %v", err)
			}
		}(node, server)
	}

	log.Printf("cluster of 3 nodes is up - connect a websocket client to node 1 (http://127.0.0.1:%d/ws?id=alice) and curl node 3's /send to see cross-node delivery; see README.md", baseHTTPPort)

	<-ctx.Done()
	log.Println("shutting down...")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for i, node := range nodes {
		if err := servers[i].Shutdown(shutdownCtx); err != nil {
			node.logger.Printf("http shutdown error: %v", err)
		}
		if err := node.registry.Close(shutdownCtx); err != nil {
			node.logger.Printf("registry close error: %v", err)
		}
		if err := node.system.Stop(shutdownCtx); err != nil {
			node.logger.Printf("actor system stop error: %v", err)
		}
	}
}

// startNode boots one actor system as a cluster member and wires a Registry to it. index
// is this node's 0-based position, used only to derive its distinct set of ports from the
// bases above.
func startNode(ctx context.Context, index int, hosts []string) (*gatewayNode, error) {
	name := fmt.Sprintf("cluster-node-%d", index+1)
	discoveryPort := baseDiscoveryPort + index
	peersPort := basePeersPort + index
	remotingPort := baseRemotingPort + index
	httpPort := baseHTTPPort + index

	provider := static.NewDiscovery(&static.Config{Hosts: hosts})

	clusterConfig := actor.NewClusterConfig().
		WithDiscovery(provider).
		WithPartitionCount(7).
		WithReplicaCount(1).
		WithPeersPort(peersPort).
		WithMinimumPeersQuorum(1).
		WithDiscoveryPort(discoveryPort).
		WithClusterStateSyncInterval(300 * time.Millisecond).
		WithKinds(&clusterKindActor{})

	system, err := actor.NewActorSystem(actorSystemName,
		// The actor system's own logs are almost entirely SWIM gossip and cluster
		// housekeeping chatter; discarding them keeps this sample's output limited to
		// the one thing it's meant to demonstrate - the delivery path a message took.
		actor.WithLogger(golog.DiscardLogger),
		actor.WithRemote(remote.NewConfig("127.0.0.1", remotingPort)),
		actor.WithCluster(clusterConfig),
	)
	if err != nil {
		return nil, fmt.Errorf("new actor system: %w", err)
	}
	if err := system.Start(ctx); err != nil {
		return nil, fmt.Errorf("start actor system: %w", err)
	}

	return &gatewayNode{
		name:     name,
		system:   system,
		registry: gateway.NewRegistry(system, golog.DiscardLogger),
		logger:   log.New(os.Stdout, fmt.Sprintf("[%s] ", name), log.LstdFlags),
		httpAddr: fmt.Sprintf("127.0.0.1:%d", httpPort),
	}, nil
}

// buildMux wires node's /ws and /send endpoints. Every node runs the identical pair of
// handlers - which node ends up holding a given connection and which node a /send request
// lands on are purely accidents of which port a client happened to talk to, exactly as it
// would be behind a real load balancer.
func buildMux(node *gatewayNode) *http.ServeMux {
	mux := http.NewServeMux()

	mux.Handle("/ws", gateway.NewWSHandler(node.registry,
		gateway.WithWSAuth(func(r *http.Request) (*gateway.ConnInfo, error) {
			id := r.URL.Query().Get("id")
			if id == "" {
				return nil, fmt.Errorf("id query parameter is required")
			}
			return &gateway.ConnInfo{ID: id}, nil
		}),
		gateway.WithWSOnConnect(func(_ context.Context, info *gateway.ConnInfo, _ *http.Request) {
			node.logger.Printf("connection %q registered - this is now the only node holding its socket", info.ID)
		}),
		gateway.WithWSOnDisconnect(func(info *gateway.ConnInfo) {
			node.logger.Printf("connection %q disconnected", info.ID)
		}),
		gateway.WithWSOnMessage(func(ctx context.Context, info *gateway.ConnInfo, payload []byte) {
			// Echoed straight back through this same node's Registry: the connection
			// is always local to whichever node accepted its upgrade, so this
			// particular send always takes the local fast path, unlike /send below.
			if err := node.registry.SendToConnection(ctx, info.ID, payload); err != nil {
				node.logger.Printf("echo to %q failed: %v", info.ID, err)
			}
		}),
	))

	mux.HandleFunc("/send", func(w http.ResponseWriter, r *http.Request) {
		id := r.URL.Query().Get("id")
		msg := r.URL.Query().Get("msg")
		if id == "" || msg == "" {
			http.Error(w, "id and msg query parameters are required", http.StatusBadRequest)
			return
		}

		// Has reports only local membership, so checking it before the send is what
		// lets this handler tell the client (and the log) which of the two delivery
		// tiers is about to run: a direct write to a socket this process owns, or a
		// cluster-wide actor lookup that hands the payload to whichever node does.
		local := node.registry.Has(id)
		if local {
			node.logger.Printf("connection %q is held locally on %s - delivering via direct socket write", id, node.name)
		} else {
			node.logger.Printf("connection %q is NOT held locally on %s - resolving it through the cluster actor directory and routing remotely", id, node.name)
		}

		if err := node.registry.SendToConnection(r.Context(), id, []byte(msg)); err != nil {
			http.Error(w, err.Error(), http.StatusNotFound)
			return
		}

		if local {
			node.logger.Printf("delivered %q to %q directly - no cluster hop needed", msg, id)
			fmt.Fprintf(w, "%s: delivered %q to %q locally (this node holds the connection)\n", node.name, html.EscapeString(msg), html.EscapeString(id))
			return
		}

		node.logger.Printf("routed %q for %q into the cluster; whichever node actually holds the socket wrote it to the wire", msg, id)
		fmt.Fprintf(w, "%s: routed %q to %q through the cluster (a different node holds the connection)\n", node.name, html.EscapeString(msg), html.EscapeString(id))
	})

	return mux
}
