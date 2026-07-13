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

// Command sse-resume is a minimal, runnable demonstration of Last-Event-ID replay.
//
// It runs in one of two modes selected by the REDIS_ADDR environment variable:
//
//   - Unset (default): a single node on :8080 backed by gateway.MemorySSEHistory. A
//     client that reconnects under the same connection id with a Last-Event-ID header
//     (which every browser EventSource sends on its own after a dropped connection) is
//     replayed the events it missed, then resumes the live stream with no gap. This is
//     the replay contract between SSEHandler and SSEHistory, which needs no cluster.
//
//   - Set to a Redis or Valkey address: two nodes are started in one process, node A on
//     :8080 and node B on :8081, each with its own actor system and Registry but both
//     wired to one shared ssehistory/redis.History. A client can stream from node A,
//     drop, and reconnect its EventSource to node B, and node B - which never saw that
//     connection live - still replays cleanly from the shared history instead of
//     emitting a gateway-gap. gateway.MemorySSEHistory cannot do this: its buffer lives
//     only in the process that recorded it, so a reconnect to any other node has no
//     record to replay. See README.md for the two-terminal curl walkthrough.
//
// Both modes drive a single ticker that broadcasts one numbered event per second to
// every node, so the application sequence numbers a client sees stay gapless across a
// reconnect regardless of which node it lands on.
package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	goredis "github.com/redis/go-redis/v9"
	goredislogging "github.com/redis/go-redis/v9/logging"
	"github.com/tochemey/goakt/v4/actor"
	golog "github.com/tochemey/goakt/v4/log"

	gateway "github.com/StringKe/goakt-gateway"
	redishistory "github.com/StringKe/goakt-gateway/ssehistory/redis"
)

// tickerTopic is the pubsub topic every /events connection is auto-joined to, so a
// single Registry.Broadcast reaches all of them without the server having to track
// connection ids itself.
const tickerTopic = "ticker"

// historyPerConn bounds how many seconds of ticks a reconnecting client can recover.
// Kept small on purpose: waiting more than this many seconds before reconnecting (or
// throttling the network for that long in devtools) is the easiest way to trigger and
// observe the gateway-gap event described in README.md.
const historyPerConn = 20

// node is one simulated gateway process: its own actor system and Registry, sharing a
// SSEHistory with every other node so a connection recorded on one can be replayed by
// another.
type node struct {
	name     string
	addr     string
	system   actor.ActorSystem
	registry *gateway.Registry
	server   *gateway.Server
}

func main() {
	ctx := context.Background()

	// go-redis logs background connection-pool errors (e.g. a Redis/Valkey it cannot
	// reach) to stderr on its own; silence it so this example's own log is the only
	// narrator of the backend choice.
	goredislogging.Disable()

	history, multiNode, historyDesc, cleanup := buildHistory(ctx)
	defer cleanup()
	log.Printf("history: %s", historyDesc)

	// One node in memory mode, two nodes in shared-history mode: two nodes are what make
	// "reconnect to a different node still replays" observable, and it is only correct
	// with a shared SSEHistory, so it is gated on REDIS_ADDR being set.
	specs := []struct{ name, addr string }{{name: "A", addr: "127.0.0.1:8080"}}
	if multiNode {
		specs = append(specs, struct{ name, addr string }{name: "B", addr: "127.0.0.1:8081"})
	}

	nodes := make([]*node, 0, len(specs))
	var ownerLease gateway.Coordinator
	if multiNode {
		ownerLease = gateway.NewMemoryCoordinator()
	}
	for _, spec := range specs {
		n, err := startNode(ctx, spec.name, spec.addr, history, ownerLease)
		if err != nil {
			log.Fatalf("start node %s: %v", spec.name, err)
		}
		nodes = append(nodes, n)
	}
	defer func() {
		for _, n := range nodes {
			_ = n.registry.Close(ctx)
			_ = n.system.Stop(ctx)
		}
	}()

	registries := make([]*gateway.Registry, 0, len(nodes))
	for _, n := range nodes {
		registries = append(registries, n.registry)
	}
	runTicker(ctx, registries)

	for _, n := range nodes {
		log.Printf("node %s listening on http://%s (browser demo: /, curl: /events?id=demo)", n.name, n.addr)
	}
	if multiNode {
		log.Printf("shared history: stream from node A, drop, reconnect the same id to node B - node B replays from the shared backend")
	}

	// A signal-driven graceful shutdown is the only way to actually exercise
	// SSEHandler.Drain (which WithDrainOnShutdown wires into Server.Shutdown): without it,
	// killing the process with SIGKILL would never send the going-away frame this sample
	// is meant to be run and stopped with Ctrl-C.
	shutdownCtx, stop := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
	defer stop()

	errCh := make(chan error, len(nodes))
	for _, n := range nodes {
		go func(n *node) { errCh <- n.server.ListenAndServe(ctx) }(n)
	}

	select {
	case err := <-errCh:
		if err != nil && err != http.ErrServerClosed {
			log.Fatal(err)
		}
	case <-shutdownCtx.Done():
		log.Println("shutting down")
		timeout, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		for _, n := range nodes {
			if err := n.server.Shutdown(timeout); err != nil {
				log.Printf("node %s shutdown: %v", n.name, err)
			}
		}
	}
}

// buildHistory picks the SSEHistory backend from REDIS_ADDR. An unset address yields an
// in-process gateway.MemorySSEHistory and single-node mode; a set address yields a shared
// ssehistory/redis.History (pointed at a Redis or Valkey server) and two-node mode, which
// is what demonstrates cross-node replay. The returned cleanup closes the client, if any.
func buildHistory(ctx context.Context) (history gateway.SSEHistory, multiNode bool, desc string, cleanup func()) {
	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		return gateway.NewMemorySSEHistory(historyPerConn), false,
			"in-process MemorySSEHistory (REDIS_ADDR unset: replay only works reconnecting to the same node)",
			func() {}
	}

	client := goredis.NewClient(&goredis.Options{Addr: addr})
	pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := client.Ping(pingCtx).Err(); err != nil {
		_ = client.Close()
		log.Fatalf("REDIS_ADDR=%q set but unreachable: %v", addr, err)
	}
	return redishistory.New(client, redishistory.WithPerConn(historyPerConn)), true,
		fmt.Sprintf("shared ssehistory/redis at %s (replay works reconnecting to any node)", addr),
		func() { _ = client.Close() }
}

// startNode builds one simulated gateway process listening on addr with its SSE handler
// wired to the shared history. Each node's actor system has a distinct name because these
// nodes are not GoAkt-clustered (there is no WithCluster): every node runs its own pubsub
// and the ticker broadcasts to each independently. What ties them together is the shared
// SSEHistory, not a gossip membership.
func startNode(ctx context.Context, name, addr string, history gateway.SSEHistory, ownerLease gateway.Coordinator) (*node, error) {
	// WithPubSub is required: /events auto-joins every connection to tickerTopic, and
	// Registry.Join/Broadcast both need the underlying actor system's pubsub bridge.
	system, err := actor.NewActorSystem("gateway-sse-resume-"+name,
		actor.WithLogger(golog.DiscardLogger),
		actor.WithPubSub(),
	)
	if err != nil {
		return nil, err
	}
	if err := system.Start(ctx); err != nil {
		return nil, err
	}

	registryOptions := []gateway.RegistryOption{}
	if ownerLease != nil {
		registryOptions = append(registryOptions, gateway.WithOwnerLease(ownerLease))
	}
	registry := gateway.NewRegistry(system, golog.DiscardLogger, registryOptions...)

	nodeName := name
	sseHandler := gateway.NewSSEHandler(registry,
		gateway.WithSSEIDFunc(func(r *http.Request) string { return r.URL.Query().Get("id") }),
		gateway.WithSSETopics(func(*http.Request) []string { return []string{tickerTopic} }),
		gateway.WithSSEHistory(history),
		// A short retry keeps the demo snappy: kill the connection (Ctrl-C the curl, or
		// throttle the browser tab offline) and the client comes back in ~2s.
		gateway.WithSSERetry(2*time.Second),
		gateway.WithSSEOnConnect(func(_ context.Context, info *gateway.ConnInfo, _ *http.Request) {
			log.Printf("node %s: connection %q joined", nodeName, info.ID)
		}),
		gateway.WithSSEOnDisconnect(func(info *gateway.ConnInfo) {
			log.Printf("node %s: connection %q left", nodeName, info.ID)
		}),
	)

	mux := http.NewServeMux()
	mux.Handle("/events", sseHandler)
	mux.HandleFunc("/", serveDemoPage)

	server, err := gateway.NewServer(addr, mux, gateway.WithDrainOnShutdown(sseHandler))
	if err != nil {
		_ = registry.Close(ctx)
		_ = system.Stop(ctx)
		return nil, err
	}

	return &node{name: name, addr: addr, system: system, registry: registry, server: server}, nil
}

// runTicker broadcasts one numbered event per second to tickerTopic on every node. Every
// node emits the same sequence number at the same tick, so a client that reconnects from
// one node to another sees the application sequence continue without a jump. The sequence
// number is application data, separate from (and unrelated to) the per-connection SSE event
// id SSEHandler assigns each delivery: it is what a client displays to prove the stream had
// no gap after a reconnect, not what Last-Event-ID resumption is keyed on.
func runTicker(ctx context.Context, registries []*gateway.Registry) {
	var seq atomic.Uint64
	go func() {
		ticker := time.NewTicker(1 * time.Second)
		defer ticker.Stop()
		for range ticker.C {
			n := seq.Add(1)
			payload := fmt.Sprintf(`{"seq":%d,"time":%q}`, n, time.Now().Format(time.RFC3339))
			for _, registry := range registries {
				if _, err := registry.Broadcast(ctx, tickerTopic, []byte(payload)); err != nil {
					log.Printf("broadcast tick %d failed: %v", n, err)
				}
			}
		}
	}()
}

// serveDemoPage serves the browser EventSource demo. It is a static page with no
// server-side templating or user input reflected into it, so a plain byte constant is
// safe to write directly - there is nothing here for html/template to escape.
func serveDemoPage(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = w.Write([]byte(demoPageHTML))
}

// demoPageHTML is a vanilla-JS EventSource client. The connection id is generated once
// with crypto.randomUUID() and kept in localStorage so that reloading the page - not
// just an EventSource-internal reconnect - resumes the same connection id and therefore
// the same replay history, exactly like the curl walkthrough in README.md.
const demoPageHTML = `<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>gateway sse-resume demo</title>
<style>
  body { font: 14px/1.4 -apple-system, sans-serif; max-width: 640px; margin: 2rem auto; padding: 0 1rem; }
  #status { font-weight: bold; }
  #status.open { color: #1a7f37; }
  #status.closed { color: #b42318; }
  #log { list-style: none; padding: 0; margin-top: 1rem; max-height: 60vh; overflow-y: auto; }
  #log li { padding: 2px 6px; font-family: ui-monospace, monospace; }
  #log li.gap { background: #ffe9e9; color: #b42318; font-weight: bold; }
  #log li.event { background: #f6f8fa; }
  code { background: #f0f0f0; padding: 1px 4px; }
</style>
</head>
<body>
<h1>gateway sse-resume demo</h1>
<p>Connection id: <code id="connId"></code> (persisted in localStorage; same id survives a page reload)</p>
<p>Status: <span id="status">connecting</span></p>
<p>Open devtools -&gt; Network -&gt; throttle to Offline for a few seconds, then restore it,
to watch the browser's built-in EventSource reconnect (using the retry: delay the server
sent) and catch up without a gap. Wait longer than the server's history window and you
will see a red "gap" row instead.</p>
<ul id="log"></ul>
<script>
  const idKey = "gateway-sse-resume-id";
  let connId = localStorage.getItem(idKey);
  if (!connId) {
    connId = crypto.randomUUID();
    localStorage.setItem(idKey, connId);
  }
  document.getElementById("connId").textContent = connId;

  const statusEl = document.getElementById("status");
  const logEl = document.getElementById("log");
  let lastSeq = 0;

  function appendLine(text, className) {
    const li = document.createElement("li");
    li.textContent = text;
    li.className = className;
    logEl.appendChild(li);
    logEl.scrollTop = logEl.scrollHeight;
  }

  const source = new EventSource("/events?id=" + encodeURIComponent(connId));

  source.onopen = () => {
    statusEl.textContent = "open";
    statusEl.className = "open";
  };
  source.onerror = () => {
    statusEl.textContent = "reconnecting...";
    statusEl.className = "closed";
  };
  source.onmessage = (e) => {
    const data = JSON.parse(e.data);
    if (lastSeq !== 0 && data.seq !== lastSeq + 1) {
      appendLine("GAP: seq jumped from " + lastSeq + " to " + data.seq, "gap");
    }
    lastSeq = data.seq;
    appendLine("seq " + data.seq + " at " + data.time + " (event id " + e.lastEventId + ")", "event");
  };
  // Named event: the server could not replay everything back to this client's
  // Last-Event-ID because the history window had already moved past it.
  source.addEventListener("gateway-gap", (e) => {
    appendLine("SERVER GAP: history no longer has events after " + e.data, "gap");
  });
</script>
</body>
</html>
`
