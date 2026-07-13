# chat

A runnable demonstration of topic broadcast in the `gateway` package: a room-based
WebSocket chat where every message a client sends is fanned out to every other client in
the same room via `Registry.Broadcast`, with `WithExclude` keeping the sender from getting
an echo of its own line back.

## What this sample shows

- `WithWSAuth` resolving identity once, at the handshake: the `room` and `user` query
  parameters become `ConnInfo.Topics` and `ConnInfo.Group` respectively, so the rest of the
  handler never touches `*http.Request` again.
- `Registry.Broadcast(ctx, topic, payload, gateway.WithExclude(senderID))` - the "send to
  everyone in this room except me" pattern.
- `DeliveryResult` as the thing a caller actually inspects after a fan-out, not just an
  `error`: the sample logs `delivered`/`dropped`/`remote` for every chat broadcast.
- The **topic vs. group** distinction (see below), because a chat room is the textbook case
  where conflating the two breaks the feature.
- The P0-2 text-frame fix: the bundled HTML page asserts `typeof event.data === 'string'`
  on every inbound WebSocket message and would visibly report an error if the server ever
  sent a binary frame instead.

## What this sample does not show

- Cross-instance delivery. Like `examples/echo`, this runs a single, non-clustered actor
  system, so every broadcast in this room is delivered to every member in the same
  process - `DeliveryResult.Remote` will read `0` for the whole session. See
  `examples/echo/README.md` for how to turn a sample like this into a real multi-node one
  (swap `actor.NewActorSystem` for a clustered one and run two copies against the same
  discovery backend); the broadcast code here does not change at all to get that, because
  `Registry.Broadcast` already fans out to the topic bridge regardless of cluster size.
- Presence/offline handling. `DeliveryResult.None()` is the signal a real chat app would use
  to fall back to a push notification when a room has nobody connected anywhere in the
  cluster, but that needs a `Presence` backend (see `presence/redis`), which is out of scope
  for a single-process sample.
- Message history / reconnect replay. That is what SSE's `SSEHistory` (`Last-Event-ID`)
  solves; this sample is plain WebSocket with no replay on reconnect.

## Topic vs. group

The sample deliberately keeps these separate to make the distinction concrete:

- **Room = topic.** `room=lobby` becomes the pub/sub topic `room:lobby`. Every connection
  that joined that topic - regardless of who it belongs to - receives a `Broadcast` to it.
  This is "who is listening to this channel right now".
- **User = group.** `user=alice` becomes `ConnInfo.Group = "user:alice"`. The connection ID
  itself is left empty in `authConn`, so the handler mints a fresh UUID per socket: opening
  two browser tabs as `alice` produces two different connection IDs sharing one group. A
  group is "who is this, across however many devices/tabs they currently have open" - it is
  what `Registry.SendToGroup` and `Registry.IsOnline` operate on, and this sample does not
  call either, precisely because a chat room broadcast is a topic operation, not a group
  one.

Mixing these up is the usual bug: broadcasting to a *group* would only reach one user's own
devices, never the room; sending to a *topic* named after a single user would work by
accident until a second device joined and either doubled delivery or silently split it.

## Run it

```
go run ./examples/chat
```

This starts an HTTP server on `http://127.0.0.1:8082` with two endpoints:

- `GET /` - the chat UI (a single embedded HTML page, no build step, no external assets).
- `GET /ws?room=<room>&user=<user>` - the WebSocket upgrade. Both query parameters are
  required; a missing one is rejected during the handshake.

## Try it

Open `http://127.0.0.1:8082/` in two browser tabs (or two different browsers). In both,
type the same room name (the form defaults to `lobby`) but a different user name, e.g.
`alice` in one tab and `bob` in the other, and click **Join** in each.

What you should see:

- Each tab logs its own join as a system line (`"alice" joined the room`), then the other
  tab's join once it connects.
- Typing a message in alice's tab and pressing Enter shows up in bob's tab as
  `alice: <message>` - and does **not** reappear in alice's own log. That is
  `WithExclude(info.ID)` at work.
- Closing a tab (or clicking **Leave**) produces a `"<user> left the room"` line in the
  other tab.
- The server's stdout logs one line per broadcast, e.g.
  `chat: room="lobby" user="alice" delivered=1 dropped=0 remote=0` - with two tabs open,
  `delivered` is `1` (bob's connection only; alice's own connection was excluded) and
  `remote` stays `0` because this sample is single-node.

To see the P0-2 regression check fire, you would need a server that regressed back to
sending binary frames; against this build there is nothing to see except its absence, which
is the point - the sample's `onmessage` handler checks `typeof event.data === 'string'`
before doing anything else and would print `ERROR: received a non-text frame (...)` into
the log pane instead of silently rendering a `[object Blob]`.

## Verification

The flow above was exercised end-to-end with a `coder/websocket` client (two connections,
same room, different users) during development: join notices arrive on both sides, a chat
message from alice is delivered to bob only (`delivered=1 dropped=0 remote=0`, confirming
the exclude), and alice's socket receives nothing further for that message. That check was
a temporary test file, not part of this sample, and was removed after confirming the
behavior - `go run ./examples/chat` plus two browser tabs is the intended way to see it.
