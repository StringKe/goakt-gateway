# cluster

A runnable demonstration of the reason `gateway.Registry` exists at all: a WebSocket
connection accepted on one node can receive a message handed to a completely different
node. `examples/echo` only ever exercises the local fast path (the connection and the
`/send` call are always in the same process); this sample is what that README's "What
this sample does not show" section points to.

## What this demonstrates

- Three GoAkt actor systems, in one cluster, each with its own `gateway.Registry` and its
  own HTTP listener - as close as one process can get to three independent
  `gateway-echo`-style replicas behind a load balancer.
- `Registry.SendToConnection` calling into a connection that is **not local**: the call
  resolves the connection cluster-wide through GoAkt's actor directory
  (`ActorSystem.ActorOf`) and hands the payload to the node that actually holds the
  socket, which writes it to the wire. No direct link between the sending node and the
  receiving node is configured anywhere - the cluster's actor directory is the only thing
  that makes this work.
- The same call taking the **local** path when it happens to land on the node that does
  hold the connection, so you can watch both tiers of the two-tier delivery model
  side by side.

## What this does not demonstrate

- `Registry.Broadcast` / `SendToGroup` (topic and group fan-out) - covered by
  `TestGatewayMultiNodesBroadcast` in the module's own test suite, not by a sample.
- Node failure or takeover - all three nodes run for the lifetime of the process; killing
  one mid-demo is not something this main.go handles specially.
- A real multi-process deployment. Running three *separate* processes (one per node, one
  port each) instead of three actor systems in one `main()` works identically from
  `gateway`'s point of view - the only thing that has to change is how each process
  finds the cluster's discovery backend (see "Turning this into three real processes"
  below).

## Run it

```
go run ./examples/cluster
```

This starts three cluster nodes in one process, each with its own HTTP listener:

| Node | HTTP address              |
|------|----------------------------|
| 1    | `http://127.0.0.1:18081`  |
| 2    | `http://127.0.0.1:18082`  |
| 3    | `http://127.0.0.1:18083`  |

Every node exposes the identical pair of endpoints:

- `GET /ws?id=<connection-id>` - upgrades to a WebSocket connection and echoes back
  whatever the client sends (always via that same node's Registry - the echo is always
  local).
- `GET /send?id=<connection-id>&msg=<text>` - delivers `msg` to the connection registered
  under `id`, via `Registry.SendToConnection`, on *this* node.

Give the cluster a few seconds after startup: each node joins over a static discovery
provider and the actor directory needs a moment to converge before cross-node lookups
succeed (the log line `cluster of 3 nodes is up` prints once all three HTTP listeners are
serving, but that's a few seconds before the directory is fully converged - if the first
`/send` after startup 404s with `connection not found`, retry it a second later).

## Reproduce it in three terminals

**Terminal 1** - start the cluster and leave it running:

```
go run ./examples/cluster
```

**Terminal 2** - connect a WebSocket client to **node 1**. Any client works; using your
browser's devtools console needs no extra tools:

```js
const ws = new WebSocket("ws://127.0.0.1:18081/ws?id=alice");
ws.onmessage = (e) => console.log("received:", e.data);
```

Or with `websocat`/`wscat` from a shell:

```
websocat ws://127.0.0.1:18081/ws?id=alice
```

**Terminal 3** - push a message from **node 3**, which does not hold this connection:

```
curl "http://127.0.0.1:18083/send?id=alice&msg=hello-from-node3"
```

## What success looks like

Terminal 1's log shows the full round trip in three lines:

```
[cluster-node-1] ... connection "alice" registered - this is now the only node holding its socket
[cluster-node-3] ... connection "alice" is NOT held locally on cluster-node-3 - resolving it through the cluster actor directory and routing remotely
[cluster-node-3] ... routed "hello-from-node3" for "alice" into the cluster; whichever node actually holds the socket wrote it to the wire
```

Terminal 2 (the WebSocket client) then prints `received: hello-from-node3` - the payload
made a full hop from node 3's HTTP handler, through the cluster's actor directory, to
node 1's socket, without node 3 ever holding a reference to that connection.

Terminal 3's `curl` response also states which path was taken:

```
cluster-node-3: routed "hello-from-node3" to "alice" through the cluster (a different node holds the connection)
```

For contrast, run the same `curl` against **node 1** instead:

```
curl "http://127.0.0.1:18081/send?id=alice&msg=hello-from-node1"
```

which answers `delivered ... locally (this node holds the connection)` - the local fast
path from `examples/echo`, no cluster hop at all.

## How the demo tells local from remote

The `/send` handler checks `Registry.Has(id)` (a purely local, no-network call) *before*
calling `SendToConnection`, and logs the verdict:

- `Has(id) == true` -> this node's Registry has the connection in its local table; the
  send that follows is a direct write to a socket this process owns.
- `Has(id) == false` -> the send that follows falls through to `ActorSystem.ActorOf`,
  which asks the cluster's actor directory where the connection's backing actor lives and
  routes the payload there over GoAkt's remoting.

Neither `SendToConnection` nor `DeliveryResult` reports which tier was used after the
fact (a successful remote `Tell` only proves the payload reached the owning node's actor
mailbox, not that it reached the socket) - `Has` beforehand is the only honest way to
label the two cases, and it's exactly what a real application would check to decide
whether a delivery attempt is "cheap" (local) or "a network hop" (remote).

## A pitfall worth knowing if you build on this sample

All three actor systems here are created with the **same name**
(`actor.NewActorSystem("gateway-cluster-demo", ...)`) on purpose. GoAkt derives its
memberlist gossip label from the actor system's name
(`internal/cluster/cluster.go`: `mconfig.Label = "prefix-" + strings.ToLower(name)`), and
peers whose systems are named differently silently reject each other's gossip streams -
each node ends up "clustered" with only itself, and every cross-node lookup 404s with no
error explaining why. If you copy this pattern into three separate binaries, give every
node's `actor.NewActorSystem` call the identical name; use `gatewayNode.name` (or
hostname, pod name, etc.) only for logging and addressing, never for the actor system
name itself.

## Turning this into three real processes

Splitting `main.go`'s single process into three real processes needs only:

1. A shared discovery backend reachable from every process - swap `discovery/static`
   (which needs every peer's address known up front, fine for one process picking its own
   ports) for `discovery/nats`, `discovery/kubernetes`, `discovery/consul`, `discovery/
   dnssd`, or `discovery/mdns` depending on where the processes actually run.
2. Distinct `--discovery-port` / `--peers-port` / `--remoting-port` / `--http-port` flags
   per process instead of the `baseXxxPort + index` arithmetic here (which only exists
   because all three nodes share one machine's port space).
3. The same actor system name on every process (see the pitfall above).

Nothing in `gateway.Registry`, `gateway.NewWSHandler`, or `Registry.SendToConnection`
changes at all - the cluster topology is entirely a GoAkt actor-system concern this
package builds on top of.
