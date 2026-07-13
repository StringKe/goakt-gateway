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

// Command delivery-confirm is a runnable, single-process demonstration of
// gateway.WithDeliveryConfirmation: the opt-in that turns cross-node delivery from a
// fire-and-forget actor.Tell into an actor.Ask that waits for the connection-owning node
// to acknowledge the socket write.
//
// It boots two real GoAkt cluster members in one OS process - node A, which accepts the
// browser's WebSocket connection, and node B, which never holds any socket and only ever
// resolves connections through the cluster actor directory. Node B carries two Registry
// instances wired to the very same actor system: one built with the package default
// (fire-and-forget), one built with WithDeliveryConfirmation. Both send to the exact same
// remote connection on node A, so the only variable between a plain and a confirmed call is
// the option itself.
//
// The two Registries share one gateway.MemoryPresence instance across both nodes purely as
// an in-process shortcut for what a real deployment would run as presence/redis; sharing a
// Go value across two actor systems living in the same process only works because nothing
// about Presence is tied to a particular actor system, and it is what lets node B's
// SendToGroup demonstrate the DeliveryResult.Remote semantics WithDeliveryConfirmation
// documents (confirmed acknowledgements instead of fan-outs issued) without a real Redis.
//
// See README.md for what the two /api endpoints measure and how to read the numbers.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os/signal"
	"syscall"
	"time"

	"github.com/tochemey/goakt/v4/actor"
	"github.com/tochemey/goakt/v4/discovery/static"
	golog "github.com/tochemey/goakt/v4/log"
	"github.com/tochemey/goakt/v4/remote"

	gateway "github.com/StringKe/goakt-gateway"
)

// Ports are spread far from the other examples' defaults (8080-8090) and from the cluster
// sample's 24101/24201/24301/18081 series purely so this can run alongside either without a
// bind conflict; nothing about the exact numbers is meaningful.
const (
	nodeADiscoveryPort = 24401
	nodeBDiscoveryPort = 24402
	nodeAPeersPort     = 24501
	nodeBPeersPort     = 24502
	nodeARemotingPort  = 24601
	nodeBRemotingPort  = 24602
	nodeAHTTPAddr      = "127.0.0.1:18087"
	nodeBHTTPAddr      = "127.0.0.1:18088"
)

// actorSystemName must be identical on both nodes: GoAkt derives its memberlist gossip
// label from it, so two systems named differently never see each other's gossip and never
// form one cluster (see examples/cluster/main.go for the same note in more detail).
const actorSystemName = "gateway-delivery-confirm-demo"

// confirmationTimeout bounds how long a confirmed send waits for node A's acknowledgement
// before failing with gateway.ErrConfirmationTimeout. It is set well above anything a
// healthy loopback round trip needs, and only ever matters if the target connection is not
// actually reachable.
const confirmationTimeout = 3 * time.Second

// benchDefaultCalls is how many round trips /api/compare runs per mode when the caller does
// not specify one. A single call's latency on a loopback socket is too close to Go's
// scheduling noise floor to read meaningfully; averaging over a few hundred calls is not.
const benchDefaultCalls = 200

// benchMaxCalls caps what a caller can ask /api/compare to run, so a stray large n cannot
// turn a demo click into a multi-second HTTP request.
const benchMaxCalls = 5000

// userGroup derives the identity group a connection's SendToGroup demo uses from its
// connection id. This sample gives every connection its own group (a group of one) purely
// so /api/broadcast has something cluster-addressable to fan out to; a real application's
// groups hold every device of one identity, not one connection each.
func userGroup(connID string) string {
	return "user:" + connID
}

// clusterKindActor is a no-op Actor registered purely to satisfy ClusterConfig.WithKinds'
// requirement that at least one actor kind exist; the gateway package's own connActor is
// spawned dynamically per connection and needs no static registration.
type clusterKindActor struct{}

var _ actor.Actor = (*clusterKindActor)(nil)

func (clusterKindActor) PreStart(*actor.Context) error { return nil }
func (clusterKindActor) Receive(*actor.ReceiveContext) {}
func (clusterKindActor) PostStop(*actor.Context) error { return nil }

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	hosts := []string{
		fmt.Sprintf("127.0.0.1:%d", nodeADiscoveryPort),
		fmt.Sprintf("127.0.0.1:%d", nodeBDiscoveryPort),
	}

	systemA, err := startClusterMember(ctx, "node-a", nodeADiscoveryPort, nodeAPeersPort, nodeARemotingPort, hosts)
	if err != nil {
		log.Fatalf("start node A: %v", err)
	}
	// Mirrors the pause examples/cluster and the module's own multi-node tests take between
	// starting cluster members: the static discovery provider's peer list is known
	// immediately, but SWIM gossip and the actor directory both need a moment to converge.
	time.Sleep(2 * time.Second)

	systemB, err := startClusterMember(ctx, "node-b", nodeBDiscoveryPort, nodeBPeersPort, nodeBRemotingPort, hosts)
	if err != nil {
		log.Fatalf("start node B: %v", err)
	}
	time.Sleep(2 * time.Second)

	// One Presence instance, shared by value across every Registry on both nodes. See the
	// package doc comment above for why this in-process sharing stands in for presence/redis.
	presence := gateway.NewMemoryPresence()

	registryA := gateway.NewRegistry(systemA, golog.DiscardLogger, gateway.WithPresence(presence))
	registryBPlain := gateway.NewRegistry(systemB, golog.DiscardLogger, gateway.WithPresence(presence))
	registryBConfirm := gateway.NewRegistry(systemB, golog.DiscardLogger,
		gateway.WithPresence(presence),
		gateway.WithDeliveryConfirmation(),
		gateway.WithConfirmationTimeout(confirmationTimeout),
	)

	muxA := http.NewServeMux()
	muxA.HandleFunc("/", serveNodeAPage)
	muxA.Handle("/ws", gateway.NewWSHandler(registryA,
		gateway.WithWSAuth(func(r *http.Request) (*gateway.ConnInfo, error) {
			id := r.URL.Query().Get("id")
			if id == "" {
				return nil, fmt.Errorf("id query parameter is required")
			}
			return &gateway.ConnInfo{ID: id, Group: userGroup(id)}, nil
		}),
		gateway.WithWSOnConnect(func(_ context.Context, info *gateway.ConnInfo, _ *http.Request) {
			log.Printf("[node-a] connection %q registered - now addressable from node B via the cluster actor directory", info.ID)
		}),
		gateway.WithWSOnDisconnect(func(info *gateway.ConnInfo) {
			log.Printf("[node-a] connection %q disconnected", info.ID)
		}),
	))
	serverA, err := gateway.NewServer(nodeAHTTPAddr, muxA)
	if err != nil {
		log.Fatalf("configure node A HTTP server: %v", err)
	}

	sender := &senderNode{plain: registryBPlain, confirm: registryBConfirm}
	muxB := http.NewServeMux()
	muxB.HandleFunc("/", serveNodeBPage)
	muxB.HandleFunc("/api/send", sender.handleSend)
	muxB.HandleFunc("/api/broadcast", sender.handleBroadcast)
	muxB.HandleFunc("/api/compare", sender.handleCompare)
	serverB, err := gateway.NewServer(nodeBHTTPAddr, muxB)
	if err != nil {
		log.Fatalf("configure node B HTTP server: %v", err)
	}

	go func() {
		log.Printf("[node-a] listening on http://%s (open in a browser to hold a connection)", nodeAHTTPAddr)
		if err := serverA.ListenAndServe(ctx); err != nil && err != http.ErrServerClosed {
			log.Fatalf("node A http server stopped unexpectedly: %v", err)
		}
	}()
	go func() {
		log.Printf("[node-b] listening on http://%s (never holds a socket - every send resolves the connection through the cluster)", nodeBHTTPAddr)
		if err := serverB.ListenAndServe(ctx); err != nil && err != http.ErrServerClosed {
			log.Fatalf("node B http server stopped unexpectedly: %v", err)
		}
	}()

	log.Printf("open http://%s in a browser, note the connection id, then drive sends from http://%s", nodeAHTTPAddr, nodeBHTTPAddr)

	<-ctx.Done()
	log.Println("shutting down...")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := serverA.Shutdown(shutdownCtx); err != nil {
		log.Printf("node A http shutdown error: %v", err)
	}
	if err := serverB.Shutdown(shutdownCtx); err != nil {
		log.Printf("node B http shutdown error: %v", err)
	}
	if err := registryA.Close(shutdownCtx); err != nil {
		log.Printf("node A registry close error: %v", err)
	}
	if err := registryBPlain.Close(shutdownCtx); err != nil {
		log.Printf("node B plain registry close error: %v", err)
	}
	if err := registryBConfirm.Close(shutdownCtx); err != nil {
		log.Printf("node B confirm registry close error: %v", err)
	}
	if err := systemA.Stop(shutdownCtx); err != nil {
		log.Printf("node A actor system stop error: %v", err)
	}
	if err := systemB.Stop(shutdownCtx); err != nil {
		log.Printf("node B actor system stop error: %v", err)
	}
}

// startClusterMember boots one actor system as a cluster member reachable at the given
// ports. WithPubSub is required alongside WithCluster: Registry.Register's group bridge and
// SendToGroup's cross-node fan-out both ride on the actor system's topic actor, which only
// exists when pub/sub is enabled - WithCluster alone does not turn it on (it is an
// independent option; see actor.WithPubSub's doc comment).
func startClusterMember(ctx context.Context, name string, discoveryPort, peersPort, remotingPort int, hosts []string) (actor.ActorSystem, error) {
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
		// housekeeping chatter; discarding them keeps this sample's output limited to the
		// one thing it demonstrates - the measured cost of confirmed delivery.
		actor.WithLogger(golog.DiscardLogger),
		actor.WithRemote(remote.NewConfig("127.0.0.1", remotingPort)),
		actor.WithCluster(clusterConfig),
		actor.WithPubSub(),
	)
	if err != nil {
		return nil, fmt.Errorf("new actor system %q: %w", name, err)
	}
	if err := system.Start(ctx); err != nil {
		return nil, fmt.Errorf("start actor system %q: %w", name, err)
	}
	return system, nil
}

// senderNode bundles node B's two Registries: plain is the package default
// (fire-and-forget cross-node delivery), confirm has WithDeliveryConfirmation set. Both
// wrap the very same actor system, so a request handler picks between them purely by
// which one it calls - there is no other difference in how a send reaches node A.
type senderNode struct {
	plain   *gateway.Registry
	confirm *gateway.Registry
}

// registryFor selects plain or confirm by the request's confirm flag.
func (s *senderNode) registryFor(confirm bool) *gateway.Registry {
	if confirm {
		return s.confirm
	}
	return s.plain
}

// sendRequest is the JSON body /api/send and /api/compare accept.
type sendRequest struct {
	ID      string `json:"id"`
	Msg     string `json:"msg"`
	Confirm bool   `json:"confirm"`
	// Calls repeats SendToConnection this many times back to back and reports the average
	// per-call latency, since a single loopback call's latency is too close to Go's
	// scheduling noise floor to read meaningfully. Defaults to 1 (a single, immediate send).
	Calls int `json:"calls"`
}

// sendResult is what /api/send and each side of /api/compare report: how the mode behaved
// across Calls back-to-back sends to the same connection.
type sendResult struct {
	Mode string `json:"mode"`
	// Calls is how many SendToConnection round trips this result summarizes.
	Calls int `json:"calls"`
	// Succeeded is how many of those calls returned a nil error.
	Succeeded int `json:"succeeded"`
	// TotalMicros is the wall-clock time every call together took.
	TotalMicros int64 `json:"totalMicros"`
	// AvgMicros is TotalMicros / Calls: the number this demo exists to make visible.
	AvgMicros int64 `json:"avgMicros"`
	// LastError is the most recent non-nil error's message, if any call failed. A confirmed
	// send surfaces a real delivery failure here (e.g. gateway.ErrConfirmationTimeout if the
	// id does not resolve to a live connection); a plain send never does; see README.md.
	LastError string `json:"lastError,omitempty"`
}

// runSends issues n back-to-back SendToConnection calls against registry and summarizes
// their timing. n below 1 is treated as 1: a comparison with zero calls reports nothing.
func runSends(ctx context.Context, registry *gateway.Registry, id, mode string, payload []byte, n int) sendResult {
	if n < 1 {
		n = 1
	}
	result := sendResult{Mode: mode, Calls: n}

	start := time.Now()
	for i := 0; i < n; i++ {
		if err := registry.SendToConnection(ctx, id, payload); err != nil {
			result.LastError = err.Error()
			continue
		}
		result.Succeeded++
	}
	elapsed := time.Since(start)

	result.TotalMicros = elapsed.Microseconds()
	result.AvgMicros = elapsed.Microseconds() / int64(n)
	return result
}

// handleSend runs one sendRequest against the mode it names and returns a single
// sendResult. It is the "just show me what happens" endpoint; handleCompare is the one
// that makes the latency difference legible.
func (s *senderNode) handleSend(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST required", http.StatusMethodNotAllowed)
		return
	}
	var req sendRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.ID == "" || req.Msg == "" {
		http.Error(w, "id and msg fields are required", http.StatusBadRequest)
		return
	}
	calls := req.Calls
	if calls < 1 {
		calls = 1
	}
	if calls > benchMaxCalls {
		calls = benchMaxCalls
	}

	mode := "plain (fire-and-forget)"
	if req.Confirm {
		mode = "confirmed (WithDeliveryConfirmation)"
	}
	result := runSends(r.Context(), s.registryFor(req.Confirm), req.ID, mode, []byte(req.Msg), calls)

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(result)
}

// compareResponse is what /api/compare reports: the same id and message sent Calls times
// through each Registry, back to back, so both numbers were measured under identical
// conditions in the same run.
type compareResponse struct {
	Plain   sendResult `json:"plain"`
	Confirm sendResult `json:"confirm"`
}

// handleCompare is the endpoint the demo page's "Compare" button drives: it runs the same
// burst of sends through the plain Registry, then through the confirmed one, and returns
// both summaries together so the extra round-trip cost WithDeliveryConfirmation adds is a
// single subtraction away (Confirm.AvgMicros - Plain.AvgMicros) rather than two separate
// requests a caller has to line up themselves.
func (s *senderNode) handleCompare(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST required", http.StatusMethodNotAllowed)
		return
	}
	var req sendRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.ID == "" || req.Msg == "" {
		http.Error(w, "id and msg fields are required", http.StatusBadRequest)
		return
	}
	calls := req.Calls
	if calls < 1 {
		calls = benchDefaultCalls
	}
	if calls > benchMaxCalls {
		calls = benchMaxCalls
	}

	ctx := r.Context()
	resp := compareResponse{
		Plain:   runSends(ctx, s.plain, req.ID, "plain (fire-and-forget)", []byte(req.Msg), calls),
		Confirm: runSends(ctx, s.confirm, req.ID, "confirmed (WithDeliveryConfirmation)", []byte(req.Msg), calls),
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}

// broadcastRequest is the JSON body /api/broadcast accepts.
type broadcastRequest struct {
	ID      string `json:"id"`
	Msg     string `json:"msg"`
	Confirm bool   `json:"confirm"`
}

// broadcastResult reports a single SendToGroup call, including the exact DeliveryResult
// counters WithDeliveryConfirmation's doc comment describes: Remote counts fan-outs issued
// without confirmation, and confirmed remote writes with it.
type broadcastResult struct {
	Mode          string `json:"mode"`
	Group         string `json:"group"`
	Delivered     int    `json:"delivered"`
	Dropped       int    `json:"dropped"`
	Remote        int    `json:"remote"`
	RemoteMeans   string `json:"remoteMeans"`
	None          bool   `json:"none"`
	ElapsedMicros int64  `json:"elapsedMicros"`
}

// handleBroadcast runs Registry.SendToGroup for the connection's group (a group of one in
// this demo - see userGroup) through the mode the request names, so DeliveryResult.Remote
// can be read directly: with the shared MemoryPresence in place, node B's confirmed
// Registry actually calls confirmRemoteGroup, which is what makes Remote count
// acknowledged writes instead of a single "fan-out issued" guess.
func (s *senderNode) handleBroadcast(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST required", http.StatusMethodNotAllowed)
		return
	}
	var req broadcastRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.ID == "" || req.Msg == "" {
		http.Error(w, "id and msg fields are required", http.StatusBadRequest)
		return
	}

	group := userGroup(req.ID)
	mode := "plain (fan-out issued)"
	remoteMeans := "fan-outs issued; whether node A actually wrote the socket is unobserved"
	if req.Confirm {
		mode = "confirmed (acknowledged writes)"
		remoteMeans = "remote writes node A acknowledged; a member that failed or timed out is not counted"
	}

	start := time.Now()
	result, err := s.registryFor(req.Confirm).SendToGroup(r.Context(), group, []byte(req.Msg))
	elapsed := time.Since(start)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(broadcastResult{
		Mode:          mode,
		Group:         group,
		Delivered:     result.Delivered,
		Dropped:       result.Dropped,
		Remote:        result.Remote,
		RemoteMeans:   remoteMeans,
		None:          result.None(),
		ElapsedMicros: elapsed.Microseconds(),
	})
}

// serveNodeAPage serves the page a browser opens to hold the WebSocket connection every
// send in this demo targets.
func serveNodeAPage(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = w.Write([]byte(nodeAPageHTML))
}

// serveNodeBPage serves the page a browser uses to drive sends from node B.
func serveNodeBPage(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = w.Write([]byte(nodeBPageHTML))
}

// nodeAPageHTML is the page held open on node A. The connection id is generated once with
// crypto.randomUUID() and kept in localStorage, the same pattern examples/sse-resume uses,
// so reloading the tab resumes the same id instead of minting a new connection to hunt for
// on node B's page.
const nodeAPageHTML = `<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>gateway delivery-confirm demo - node A</title>
<style>
  body { font: 14px/1.4 -apple-system, sans-serif; max-width: 640px; margin: 2rem auto; padding: 0 1rem; }
  #status { font-weight: bold; }
  #status.open { color: #1a7f37; }
  #status.closed { color: #b42318; }
  #log { list-style: none; padding: 0; margin-top: 1rem; max-height: 60vh; overflow-y: auto; }
  #log li { padding: 2px 6px; font-family: ui-monospace, monospace; background: #f6f8fa; }
  code { background: #f0f0f0; padding: 1px 4px; }
</style>
</head>
<body>
<h1>node A - connection holder</h1>
<p>Connection id: <code id="connId"></code> (persisted in localStorage; copy it into node B's page)</p>
<p>Status: <span id="status">connecting</span></p>
<p>This tab's WebSocket connection lives only on node A. Every send you trigger from
<a id="nodeBLink" href="#">node B</a> reaches this socket through the cluster actor
directory, never directly.</p>
<ul id="log"></ul>
<script>
  const idKey = "gateway-delivery-confirm-id";
  let connId = localStorage.getItem(idKey);
  if (!connId) {
    connId = crypto.randomUUID();
    localStorage.setItem(idKey, connId);
  }
  document.getElementById("connId").textContent = connId;
  document.getElementById("nodeBLink").href = "http://" + location.hostname + ":18088/?id=" + encodeURIComponent(connId);

  const statusEl = document.getElementById("status");
  const logEl = document.getElementById("log");

  function appendLine(text) {
    const li = document.createElement("li");
    li.textContent = text;
    logEl.appendChild(li);
    logEl.scrollTop = logEl.scrollHeight;
    while (logEl.children.length > 200) {
      logEl.removeChild(logEl.firstChild);
    }
  }

  const ws = new WebSocket("ws://" + location.host + "/ws?id=" + encodeURIComponent(connId));
  ws.onopen = () => { statusEl.textContent = "open"; statusEl.className = "open"; };
  ws.onclose = () => { statusEl.textContent = "closed"; statusEl.className = "closed"; };
  ws.onerror = () => { statusEl.textContent = "error"; statusEl.className = "closed"; };
  ws.onmessage = (e) => appendLine(new Date().toLocaleTimeString() + "  " + e.data);
</script>
</body>
</html>
`

// nodeBPageHTML is the page used to drive sends from node B, which never holds a socket
// itself. It reads the target connection id from the ?id= query parameter (which node A's
// page's link already fills in) or lets it be typed in by hand.
const nodeBPageHTML = `<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>gateway delivery-confirm demo - node B</title>
<style>
  body { font: 14px/1.4 -apple-system, sans-serif; max-width: 760px; margin: 2rem auto; padding: 0 1rem; }
  fieldset { margin-bottom: 1.5rem; }
  input { width: 16rem; }
  input[type=number] { width: 6rem; }
  pre { background: #111; color: #0f0; padding: 0.75rem; overflow-x: auto; font-size: 0.85rem; }
  table { border-collapse: collapse; margin-top: 0.5rem; }
  td, th { border: 1px solid #ccc; padding: 4px 10px; text-align: right; }
  th:first-child, td:first-child { text-align: left; }
  .hint { color: #666; font-size: 0.85rem; }
</style>
</head>
<body>
<h1>node B - sender (holds no sockets)</h1>
<p class="hint">Every call below resolves the connection id through the cluster actor
directory and reaches node A's socket only through its connActor - node B never has a
direct reference to it.</p>

<fieldset>
  <legend>Target</legend>
  <label>connection id (from node A's page) <input id="connId"></label>
</fieldset>

<fieldset>
  <legend>1. Single send - SendToConnection, one call each mode</legend>
  <label>message <input id="msg1" value="hello from node B"></label>
  <button id="sendPlain">Send (plain)</button>
  <button id="sendConfirm">Send (confirmed)</button>
  <pre id="sendResult">(nothing sent yet)</pre>
</fieldset>

<fieldset>
  <legend>2. Compare latency - the same burst of SendToConnection calls through both modes</legend>
  <label>message <input id="msg2" value="benchmark payload"></label>
  <label>calls each <input id="calls" type="number" value="200" min="1" max="5000"></label>
  <button id="compare">Compare</button>
  <table id="compareTable" style="display:none">
    <thead><tr><th>mode</th><th>calls</th><th>succeeded</th><th>avg &micro;s/call</th><th>total ms</th></tr></thead>
    <tbody></tbody>
  </table>
  <pre id="compareError" style="display:none"></pre>
</fieldset>

<fieldset>
  <legend>3. SendToGroup - DeliveryResult.Remote under each mode</legend>
  <label>message <input id="msg3" value="group message"></label>
  <button id="broadcastPlain">Broadcast (plain)</button>
  <button id="broadcastConfirm">Broadcast (confirmed)</button>
  <pre id="broadcastResult">(nothing sent yet)</pre>
</fieldset>

<script>
const params = new URLSearchParams(location.search);
if (params.get("id")) document.getElementById("connId").value = params.get("id");

async function postJSON(url, body) {
  const res = await fetch(url, { method: "POST", body: JSON.stringify(body) });
  const text = await res.text();
  if (!res.ok) throw new Error(text);
  return JSON.parse(text);
}

document.getElementById("sendPlain").onclick = () => doSend(false);
document.getElementById("sendConfirm").onclick = () => doSend(true);
async function doSend(confirm) {
  const id = document.getElementById("connId").value;
  const msg = document.getElementById("msg1").value;
  const out = document.getElementById("sendResult");
  try {
    const result = await postJSON("/api/send", { id, msg, confirm, calls: 1 });
    out.textContent = JSON.stringify(result, null, 2);
  } catch (e) {
    out.textContent = "error: " + e.message;
  }
}

document.getElementById("compare").onclick = async () => {
  const id = document.getElementById("connId").value;
  const msg = document.getElementById("msg2").value;
  const calls = parseInt(document.getElementById("calls").value, 10) || 200;
  const table = document.getElementById("compareTable");
  const tbody = table.querySelector("tbody");
  const errEl = document.getElementById("compareError");
  errEl.style.display = "none";
  try {
    const result = await postJSON("/api/compare", { id, msg, calls });
    tbody.innerHTML = "";
    for (const row of [result.plain, result.confirm]) {
      const tr = document.createElement("tr");
      tr.innerHTML = "<td>" + row.mode + "</td><td>" + row.calls + "</td><td>" + row.succeeded +
        "</td><td>" + row.avgMicros + "</td><td>" + (row.totalMicros / 1000).toFixed(2) + "</td>";
      tbody.appendChild(tr);
    }
    table.style.display = "";
  } catch (e) {
    table.style.display = "none";
    errEl.style.display = "";
    errEl.textContent = "error: " + e.message;
  }
};

document.getElementById("broadcastPlain").onclick = () => doBroadcast(false);
document.getElementById("broadcastConfirm").onclick = () => doBroadcast(true);
async function doBroadcast(confirm) {
  const id = document.getElementById("connId").value;
  const msg = document.getElementById("msg3").value;
  const out = document.getElementById("broadcastResult");
  try {
    const result = await postJSON("/api/broadcast", { id, msg, confirm });
    out.textContent = JSON.stringify(result, null, 2);
  } catch (e) {
    out.textContent = "error: " + e.message;
  }
}
</script>
</body>
</html>
`
