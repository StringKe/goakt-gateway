# echo

A minimal, runnable demonstration of the `gateway` package: a WebSocket echo endpoint
plus an ordinary HTTP handler that delivers a message to a registered WebSocket
connection - the "HTTP handler talks to a websocket connection" pattern the whole
package exists for.

## Run it

```
go run ./examples/echo
```

This starts an HTTP server on `http://127.0.0.1:8080` with three endpoints:

- `GET /ws?id=<connection-id>` - upgrades to a WebSocket connection and echoes back
  whatever the client sends.
- `GET /send?id=<connection-id>&msg=<text>` - an ordinary HTTP handler that delivers
  `msg` to the WebSocket connection registered under `id`, via
  `Registry.SendToConnection`.
- `GET /healthz` - returns HTTP 204 for Kubernetes liveness and readiness probes.

The process accepts `SIGINT` and `SIGTERM`. Shutdown uses a 30 second deadline and
drains the WebSocket handler through `Server.Shutdown` before the process exits.

## Default behavior worth knowing before you connect

`NewWSHandler` ships opinionated defaults; this sample changes none of them, so what you
see below is the library's out-of-the-box behavior, not something main.go configured:

- **Text frames only.** `WithWSOnMessage` fires for `websocket.MessageText` frames
  (`WithWSMessageType` default). A client sending binary frames is rejected by the
  underlying `coder/websocket` library before your handler ever sees it.
- **Origin check: same-Host only.** A browser always sends an `Origin` header, and the
  default policy (no `WithWSOriginPatterns`) accepts it only when it matches the
  request's own `Host`. Non-browser clients (`websocat`, `curl`, `wscat`, this sample's
  browser console snippet run from `file://` or a different port) send no `Origin`
  header at all and are exempt from the check - see `authorizeOrigin` in `ws.go`. Cross-
  origin browser pages need `WithWSOriginPatterns` or `WithWSInsecureSkipOriginCheck`,
  neither of which this sample sets.
- **Ping/pong keepalive.** Every 30s (`WithWSPingInterval` default) the handler pings the
  socket and expects a pong within 10s, so a half-open connection (client vanished
  without a close frame - the common case on mobile networks) is dropped instead of
  leaking forever. You won't observe this on a short-lived local test; it matters once
  connections live for minutes.
- **Takeover on reconnect.** Registering the same `id` twice does not fail: the new
  connection replaces the old one (`WithWSReplaceExisting`, on by default) and the old
  socket is closed. Reconnect with `?id=alice` while a previous `alice` connection is
  still open to see the first one close.

## Try it

In one terminal, connect a WebSocket client. Any client works - `websocat` is used here
because it is a single static binary; `wscat` (`npx wscat -c ...`) is an equivalent
Node-based alternative:

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

Browser console alternative (no extra tools required - open any page served from
`http://127.0.0.1:8080`, or any other origin since this sample sets no origin
restriction and non-browser-style requests are exempt; a page's own `fetch`/`WebSocket`
calls do carry `Origin`, so if you instead open the console on a page served from a
*different* origin than `127.0.0.1:8080`, the upgrade is rejected with HTTP 403 unless
you widen `WithWSOriginPatterns`):

```js
const ws = new WebSocket("ws://127.0.0.1:8080/ws?id=alice");
ws.onmessage = (e) => console.log("received:", e.data);
ws.onopen = () => ws.send("ping");
```

**Success looks like:** the terminal running `go run ./examples/echo` logs
`connection "alice" joined`; typing in the WebSocket client echoes the same text back;
the `curl /send` command's message appears in the WebSocket client without you typing
anything; closing the client (or reconnecting with the same `id`) logs
`connection "alice" left`.

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
