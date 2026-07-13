# gateway-persistence

Runnable demo of gateway's opt-in **at-least-once delivery**: a `gateway.Outbox` plus
`Registry.Ack`, turning the library's default fire-and-forget socket write into "persist,
deliver, wait for the client's ack, redeliver on reconnect if it never came".

## Why this matters

The default `Registry.SendToConnection` is at-most-once: it writes to whatever socket is
live right now and forgets about it. If the socket is gone, or the process crashes between
the write and the client actually reading it, the message is lost. That is the right default
for a gateway library - most traffic (presence pings, ephemeral cursor positions, chat
typing indicators) is not worth paying storage and latency for - but some traffic is: a
support ticket update, an order status change, a message a human is expected to read.

`WithOutbox` upgrades exactly that traffic, without changing anything for the rest:

- `Registry.SendToConnection` persists the payload to the `Outbox` **before** it is written
  to a socket, whether or not a socket for that connection id currently exists anywhere.
  Sending to an offline user still queues the message; it only returns
  `gateway.ErrConnectionNotFound` instead of writing anywhere. This demo's `/send` handler
  treats that as a normal, expected outcome, not a failure.
- `Registry.Register` - which every reconnect goes through - automatically redelivers every
  message the `Outbox` still holds for that connection id, in `Seq` order, before the caller
  of `Register` (the WebSocket handshake, here) gets control back. A client that reconnects
  catches up on everything it missed with zero extra application code.
- `Registry.Ack` is how redelivery stops. Without it, a message sits in the `Outbox` forever
  (or until a configured TTL expires it) and is redelivered on every future reconnect.

The one piece of plumbing this demo has to supply itself: `SendToConnection` deliberately
keeps its signature `(ctx, id, payload) error` - it does not hand back the id the `Outbox`
assigned the message internally, because doing so would mean either returning a second value
every caller has to thread through, or reaching into the payload bytes to inject one, both of
which compromise the "send is a raw `[]byte` in, `error` out" primitive the whole library is
built on. So the wire message this demo sends carries its own application-level id (a UUID,
distinct from the `Outbox`'s internal id), and when a client acks that id, the handler
recovers the `Outbox`'s internal id the way the library's own tests do: read the connection's
unacknowledged tail back with `Outbox.Unacked` and match on it. See the `ackHandler` doc
comment in `main.go` for the exact reasoning.

### At-least-once, not exactly-once

A crash or dropped connection between "client received message" and "server received the
ack" redelivers a message the client already saw. This is correct at-least-once behavior, not
a bug: the server has no way to know the client processed a message until the ack arrives, so
it must assume it did not. A real client discards a redelivered id it has already rendered
instead of showing it twice - the demo page's message log intentionally lets you see this by
disabling auto-ack and reconnecting, so a duplicate becomes visible instead of theoretical.

## Files

- `main.go` - the server: a `gateway.Registry` with `WithOutbox`, a `WSHandler` that acks
  inbound frames and (via `Register`, called during the handshake) redelivers on reconnect,
  and three plain HTTP endpoints (`/send`, `/unacked`, `/`) plus the demo page.
- `README.md` - this file.

## How to run

```sh
go run ./examples/persistence
```

Open <http://127.0.0.1:8080> in a browser.

By default the demo uses `gateway.NewMemoryOutbox()`, an in-process `Outbox` - fine for this
single-process demo, wrong for a real cluster (a reconnect that lands on a different node
would not see the tail the first node persisted). To exercise the cluster-safe backend
instead, point `REDIS_ADDR` at a Redis or Valkey instance before running:

```sh
REDIS_ADDR=127.0.0.1:6379 go run ./examples/persistence
```

The server logs which backend it picked on startup.

## Walking through it

1. **Send while offline.** With nobody connected as `alice`, use section 2 of the page
   (target user `alice`, any text) and click **Send**. The result reports
   `queued (user is offline, will redeliver on reconnect)` - the message is already in the
   Outbox even though no socket exists yet. Confirm it with section 4 (**Refresh** for user
   `alice`): you will see one entry.

2. **Reconnect and get it redelivered.** In section 1, uncheck nothing yet - just click
   **Connect** as `alice`. The queued message arrives immediately in section 3, with an
   **Ack** button next to it. This is `Registry.Register`'s automatic redelivery, not
   anything `/send` or the page's JS did explicitly.

3. **See redelivery repeat until acked.** Uncheck "auto-ack" in section 2. Click **Send**
   again. The message appears in section 3 but is not acked. Click **Disconnect**, then
   **Connect** again: the same message (same id, visible in the log) arrives a second time.
   This is the "redelivered because the server cannot know you already saw it" behavior the
   README's at-least-once section describes.

4. **Stop the redelivery.** Click **Ack** on a message. Refresh section 4: it is gone from
   the Outbox. Disconnect and reconnect once more - it does not come back.

## Success criteria

- `go run ./examples/persistence` starts and logs
  `gateway-persistence listening on http://127.0.0.1:8080` without a Redis connection error
  (when `REDIS_ADDR` is unset).
- A `/send` to an offline user returns HTTP 202 with `"status":"queued ..."`, and
  `/unacked?user=<name>` immediately shows the message.
- Connecting that user's WebSocket delivers the queued message without a second `/send` call.
- Disconnecting before acking and reconnecting redelivers the same message id again.
- Sending an ack for that id (via the page's **Ack** button, or by having the browser send
  `{"type":"ack","id":"<id>"}` on the socket) makes it disappear from `/unacked`, and it is
  not redelivered on the next reconnect.

## Library bugs found

None. `go build ./examples/persistence/...` and `go vet ./examples/persistence/...` are both
clean, and the flow above was exercised end to end (offline queue, live delivery, reconnect
redelivery with a stable id across reconnects, ack draining the Outbox) with a throwaway
WebSocket client against the running server.
