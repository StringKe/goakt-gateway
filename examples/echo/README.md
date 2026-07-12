# echo

A minimal, runnable demonstration of the `gateway` package: a WebSocket echo endpoint
plus an ordinary HTTP handler that delivers a message to a registered WebSocket
connection - the "HTTP handler talks to a websocket connection" pattern the whole
package exists for.

## Run it

```
go run ./examples/echo
```

This starts an HTTP server on `http://127.0.0.1:8080` with two endpoints:

- `GET /ws?id=<connection-id>` - upgrades to a WebSocket connection and echoes back
  whatever the client sends.
- `GET /send?id=<connection-id>&msg=<text>` - an ordinary HTTP handler that delivers
  `msg` to the WebSocket connection registered under `id`, via
  `Registry.SendToConnection`.

## Try it

In one terminal, connect a WebSocket client (any client works; here's one using
`websocat`, or use your browser's devtools console):

```
websocat ws://127.0.0.1:8080/ws?id=alice
```

Typing into that connection echoes back immediately (the `/ws` handler's own
`OnMessage` callback). In another terminal, push a message to the same connection from
plain HTTP instead:

```
curl "http://127.0.0.1:8080/send?id=alice&msg=hello-from-http"
```

The text appears in the WebSocket client. That single `curl` call is
`Registry.SendToConnection` taking its local fast path: since the connection is held by
this same process, the payload is written directly to the socket - no actor mailbox, no
cluster lookup.

Browser console alternative (no extra tools required):

```js
const ws = new WebSocket("ws://127.0.0.1:8080/ws?id=alice");
ws.onmessage = (e) => console.log("received:", e.data);
ws.onopen = () => ws.send("ping");
```

## What this sample does not show: cross-instance delivery

This sample runs a single, non-clustered actor system, so `Registry.SendToConnection`
always takes the local, direct-write path shown above. The other half of the two-tier
delivery model - resolving a connection held by *another* node through the cluster-aware
actor directory and delivering to it remotely - needs a real multi-node cluster to
demonstrate honestly, which is more infrastructure than a sample should carry.

That path is exercised end-to-end by the module's own test suite instead:

```
go test -race -run TestGatewayMultiNodesSendToConnection .
go test -race -run TestGatewayMultiNodesBroadcast .
```

To turn this sample into a real multi-node one, replace the single `actor.NewActorSystem`
call with `actor.WithCluster(...)` (see any GoAkt `discovery` provider), run two copies
of this binary with different `--addr`/ports pointed at the same discovery backend, and
register a connection against one instance's `/ws` while calling `/send` against the
other.
