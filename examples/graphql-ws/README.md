# graphql-ws example

Demonstrates that `gateway.Registry` does not require `gateway.WSHandler`. This example
hand-rolls the [graphql-transport-ws](https://github.com/enisdenjo/graphql-ws/blob/master/PROTOCOL.md)
subprotocol directly on top of `github.com/coder/websocket` and drives the Registry with
its own connection id scheme, its own subscribe/unsubscribe bookkeeping, and its own
authentication timing - none of which go through `gateway.WSHandler`.

## Why this example exists

`gateway.WSHandler` authenticates a connection during the HTTP upgrade: `WSAuthFunc`
only ever sees the `*http.Request`. That is the right shape for token-in-header or
token-in-query-param auth, and it is what most WebSocket gateways look like.

It is not what mip-aio's real WebSocket endpoint looks like. There, the socket is
hosted by gqlgen, and authentication happens on the *first application-level frame*:
the client sends `connection_init` with `payload.Authorization` only after the upgrade
has already completed. `WSAuthFunc` cannot express "accept the upgrade, then wait for a
frame, then decide". Nothing in `gateway.WSHandler`'s option set can either - subscribing
to a topic, in graphql-ws, also happens on a later `subscribe` frame, per-subscription,
not at registration time.

So this example proves the thing that actually matters for a consumer like mip-aio:
`Registry.Register`, `Unregister`, `Join`, `Leave`, `SendToConnection`, and `Broadcast`
are ordinary exported methods on `*gateway.Registry`. `WSHandler` is one caller of
them, built for the common case; you are free to be another, built for whatever your
transport actually does.

### What it demonstrates

- Negotiating a custom subprotocol (`Sec-WebSocket-Protocol: graphql-transport-ws`) with
  `coder/websocket` directly.
- Deferring authentication until after the upgrade, on the first frame
  (`connection_init.payload.Authorization`), and closing with the graphql-ws-specified
  code `4401` on failure - not an HTTP 403, because the upgrade already succeeded.
- Registering a connection with the Registry with no topics, then joining/leaving
  topics one at a time as `subscribe`/`complete` frames arrive, with per-topic
  reference counting so two subscriptions to the same topic only cost the Registry one
  `Join`.
- Driving `Registry.Broadcast` from a plain `net/http` handler (`/publish`) and having
  the payload arrive at every subscribed connection's `send` function - the same
  cross-node fan-out `WSHandler`-based connections get, with no `WSHandler` involved on
  either side.
- Routing a delivered payload back to the right graphql-ws subscription id(s). The
  Registry hands a connection's `send` function raw bytes with no indication of which
  topic they arrived on (one socket can be joined to several), so the topic travels
  inside the payload itself (`publishEnvelope{Topic, Data}`) and the connection looks up
  its own subscriptions by topic before framing a `next` message.

### What it does not demonstrate

- A spec-complete graphql-ws server. There is no query execution, no `error` frame, no
  rejection of a duplicate subscription id (`4409` in the spec), no
  `connection_init` retry limiting (`4429`). Only the message types named in the task -
  `connection_init` / `connection_ack` / `subscribe` / `next` / `complete` / `ping` /
  `pong` - are implemented.
- Real authentication. `demoToken` is a hardcoded string
  (`const demoToken = "Bearer demo-token"` in `main.go`); swap `awaitInit`'s comparison
  for a real token/JWT check.
- TLS. This example runs plain HTTP so `go run` needs no certificates; see
  `examples/echo` and the root `cert_manager.go`/`server.go` for the cluster-shared TLS
  story.

## When to use WSHandler vs. writing your own handler

Use `gateway.WSHandler` when:

- Authentication can be decided from the upgrade `*http.Request` alone (a header, a
  cookie, a query-string token) - i.e. `WSAuthFunc` can do the whole job.
- Topics are known before or at registration time (query params, the auth result) and
  do not change per-message during the connection's life.
- You want ping/pong keepalive, an inbound rate limiter, a read-size limit, origin
  checking, and takeover-on-reconnect handled for you.

Write your own handler (as this example does) when:

- Authentication or topic membership is decided by an application-level frame sent
  *after* the upgrade - graphql-ws's `connection_init`, or any protocol where the first
  message on the wire carries the credentials.
- You are hosting the WebSocket through another framework (gqlgen, a generated
  transport, a legacy protocol) that already owns the upgrade and the read/write loop,
  and the Registry is only there to give that framework cluster-wide delivery.
- You need message framing WSHandler does not have an option for (this example's
  `id`/`type`/`payload` envelope, or anything else that is not "one payload in, one
  payload out").

In both cases the Registry does the identical job: track which connection ids are
registered where in the cluster, and deliver payloads to them by id, by group, or by
topic. What differs is only who calls `Register`/`Join`/`Broadcast` and when.

## Running it

```sh
go run ./examples/graphql-ws
# gateway-graphql-ws listening on http://127.0.0.1:8090 (ws: /graphql, publish: /publish?topic=room:1&data=hello)
```

`-addr` overrides the listen address (default `127.0.0.1:8090`).

## Verifying it

The server exposes two endpoints:

- `ws://127.0.0.1:8090/graphql` - the graphql-transport-ws socket. Requires the
  `graphql-transport-ws` subprotocol; a client that does not offer it is closed with
  `websocket.StatusProtocolError`.
- `GET /publish?topic=<topic>&data=<text>` - broadcasts `data` (as a JSON string) to
  every connection subscribed to `topic`, wherever it is registered in the cluster.

### With a browser console or Node

```js
const ws = new WebSocket("ws://127.0.0.1:8090/graphql", "graphql-transport-ws");
ws.onmessage = (e) => console.log("recv", e.data);
ws.onclose = (e) => console.log("closed", e.code, e.reason);
ws.onopen = () => {
  ws.send(JSON.stringify({
    type: "connection_init",
    payload: { Authorization: "Bearer demo-token" },
  }));
};
```

Once you see `{"type":"connection_ack"}`, subscribe and trigger a broadcast from
another terminal:

```js
ws.send(JSON.stringify({ id: "sub-1", type: "subscribe", payload: { topic: "room:1" } }));
```

```sh
curl "http://127.0.0.1:8090/publish?topic=room:1&data=hello"
```

### What success looks like

- Connecting with a wrong (or missing) `Authorization` in `connection_init` gets the
  socket closed with code `4401` and reason `Unauthorized`, per graphql-ws's own
  convention for this case (see `PROTOCOL.md`,
  <https://github.com/enisdenjo/graphql-ws/blob/master/PROTOCOL.md#invalid-message>,
  which reserves `4401` for `Unauthorized`).
- Connecting with `Authorization: Bearer demo-token` gets `{"type":"connection_ack"}`.
- After `subscribe` with `{"topic":"room:1"}`, the `curl /publish?topic=room:1&...`
  above produces exactly one frame on the socket:
  `{"id":"sub-1","type":"next","payload":"hello"}` - the subscription id ties the
  delivery back to the `subscribe` that asked for it.
- `/publish` on a topic nobody has subscribed to still returns `200 OK` with
  `delivered=0 dropped=0 remote=0`; that is `gateway.DeliveryResult.None()` reporting
  correctly, not an error.
- Sending `complete` for a subscription id and then publishing to its topic again
  produces no further `next` frames on that socket.
