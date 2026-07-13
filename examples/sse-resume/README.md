# sse-resume

A minimal, runnable demonstration of `gateway`'s SSE Last-Event-ID replay: a background
ticker broadcasts one numbered event per second, `SSEHandler` records every event it
delivers to a connection, and a client that reconnects under the same id with a
`Last-Event-ID` header is replayed the events its previous registration recorded after
that id, before the live stream resumes.

Read "What replay does and does not cover" below before the walkthroughs: replay recovers
events that reached the connection **while it was still registered** (the half-open TCP
window that would otherwise strand a user), not events produced while nothing held the id.

## What this shows

- `SSEHandler` assigns every delivered event an id of the form `<connID>-<seq>` and, with
  a `SSEHistory` configured, records it under the connection id as it goes out.
- A client that reconnects under the **same connection id** with a `Last-Event-ID` header
  gets replayed the events the previous registration recorded after that id, before the
  live stream resumes. Browsers send `Last-Event-ID` automatically for `EventSource`;
  `curl` needs it passed by hand, which is what the walkthrough below does.
- If the requested `Last-Event-ID` has aged out of the history, the server sends a named
  `gateway-gap` event (payload: the id it could not find) before replaying whatever it
  still has, so the client can tell "caught up" apart from "silently missing events" -
  see `sse.go`'s `SSEGapEventName`.

## What replay does and does not cover

`SSEHistory` only ever contains events the server actually delivered to a connection. An
event is recorded at the moment `SSEHandler` writes it to the stream, so history grows
only while the connection is **registered**.

- **Covered: the half-open window.** Behind a half-open TCP connection (a phone that
  changed networks, a laptop that slept) the socket looks alive to the server for as long
  as it takes a write to fail or a ping to time out. The connection stays registered and
  keeps recording every tick. When the client reconnects under the same id it takes over
  and is replayed exactly those recorded ticks - gaplessly. This is the case that would
  otherwise strand a user, and it is what the curl walkthrough below reproduces
  deterministically with an overlapping second connection.
- **Not covered: events produced while nothing held the id.** A disconnect the server has
  already noticed unregisters the connection, which leaves the `"ticker"` topic; ticks
  produced after that are delivered to no one and never enter history. Reconnecting will
  resume from the live stream and the application `seq` will jump across the gap. See
  "What this does not show" below - this is the same limitation, stated from the other
  side.

A clean `Ctrl-C` on the curl process, or a DevTools "Offline" toggle that closes the
socket, is the second kind: the server notices at once, unregisters, and the ticks during
the gap are lost. That is expected, not a bug - do not expect a gapless resume across one.

## What this does not show

- **Cross-instance delivery.** In the default (no `REDIS_ADDR`) mode this sample runs a
  single, non-clustered actor system, so every event is a purely local
  `Registry.Broadcast` and `SSEHistory` is `MemorySSEHistory`, which is per-process: replay
  only works if the client reconnects to the *same* node. Setting `REDIS_ADDR` switches the
  sample to a shared `ssehistory/redis` backend and two nodes, which is exactly the
  cross-node replay case memory mode cannot cover - see "Cross-node replay with Redis /
  Valkey" below.
- **Arbitrary offline replay.** The history only ever contains events the server put on
  the wire for a connection that was still registered. Events produced while a
  connection is fully unregistered (not just disconnected-but-not-yet-noticed) never
  enter the history. That is what `DeliveryResult.None()` is for - see the
  `notification`-style offline fallback pattern described in the root README, not this
  sample.

## Run it

```
go run ./examples/sse-resume
```

This starts an HTTP server on `http://127.0.0.1:8080` with:

- `GET /events?id=<connection-id>` - the SSE stream. A fixed connection id (e.g. `demo`)
  is what makes replay possible across reconnects; omit `id` and you get a random one
  every time, which defeats the point.
- `GET /` - a browser demo page (vanilla JS `EventSource`, no build step, no
  dependencies).

Every connection is auto-joined to a single `"ticker"` topic server-side
(`WithSSETopics`), and a goroutine calls `Registry.Broadcast` on that topic once a
second - so opening `/events` is all a client needs to do to start receiving ticks; there
is no separate subscribe step.

## Try it with curl

To watch a gapless replay you have to reproduce the half-open window: the previous
registration must still be live and recording when the new one arrives. Two overlapping
`curl` processes do that deterministically.

In terminal 1, connect and leave it running. Note the event ids as they arrive (`id:
demo-1`, `demo-2`, ...):

```
curl -N "http://127.0.0.1:8080/events?id=demo"
```

A few ticks later, in terminal 2 - **without** stopping terminal 1 - reconnect under the
same id with `Last-Event-ID` set to an id from a few ticks back (say `demo-2`):

```
curl -N -H "Last-Event-ID: demo-2" "http://127.0.0.1:8080/events?id=demo"
```

Terminal 2 takes the id over (terminal 1's stream is terminated) and first replays every
tick terminal 1 recorded after `demo-2` - `demo-3`, `demo-4`, ... - in order, before the
live stream resumes. The `seq` field inside each event's JSON payload is continuous across
the handover: the events terminal 2 "missed" were kept because terminal 1 was still
registered and recording them. That is exactly what happens for a real client whose dead
socket the server has not noticed yet.

Now contrast the clean-disconnect case the sample cannot paper over: connect, watch a few
ticks, `Ctrl-C`, wait several seconds, then reconnect with the last id you saw. The server
noticed the `Ctrl-C` at once and unregistered, so the ticks during the wait were delivered
to no one and never recorded; the reconnect resumes live and the `seq` jumps. This is the
expected behavior described under "What replay does and does not cover".

To see the gap branch, reconnect with an id the server never issued (or one that has aged
out of the history):

```
curl -N -H "Last-Event-ID: demo-999" "http://127.0.0.1:8080/events?id=demo"
```

The very first thing on the wire is `event: gateway-gap` / `data: demo-999`, then the
stream continues with whatever history remains (or goes straight live if nothing does).

## Cross-node replay with Redis / Valkey

Everything above uses `MemorySSEHistory`, whose buffer lives only in the process that
recorded it - so a client that reconnects to a *different* node has no history to replay.
Set `REDIS_ADDR` and the sample instead runs **two nodes in one process** (node A on
`:8080`, node B on `:8081`), each with its own actor system and `Registry`, both wired to
one shared `ssehistory/redis.History`. The shared backend is what lets a connection
recorded on one node be replayed by the other. The sample also shares an owner lease
coordinator between its two registries. Shared replay requires both `WithOwnerLease` and a
`GenerationalHistory`; the handler rejects a shared history without that fencing boundary.

`docker-compose.yml` in the repo root starts a Redis and a Valkey; either works, because
`ssehistory/redis` uses only commands both implement:

```
docker compose up -d                                  # redis on :6399, valkey on :6400
REDIS_ADDR=127.0.0.1:6399 go run ./examples/sse-resume # or :6400 for Valkey
```

The log then shows both nodes listening. Reproduce a reconnect that lands on the *other*
node. In terminal 1, stream from node A and note the last id (e.g. `demo-3`):

```
curl -N "http://127.0.0.1:8080/events?id=demo"
```

`Ctrl-C` it, then in terminal 2 reconnect to node B with that `Last-Event-ID`:

```
curl -N -H "Last-Event-ID: demo-3" "http://127.0.0.1:8081/events?id=demo"
```

Node B has never seen connection `demo` live, yet it finds `demo`'s recorded buffer in the
shared backend: it does **not** emit a `gateway-gap`, and it continues the event-id
sequence past `demo-3` (`demo-4`, ...) rather than restarting at `demo-1`. Run the same two
commands in memory mode (no `REDIS_ADDR`, one node) against a second node and node B would
have no record at all. That difference - "any node can replay" versus "only the recording
node can replay" - is the entire reason the Redis / Valkey `SSEHistory` exists.

As in memory mode, only events recorded while the connection was registered are replayed;
ticks produced during the fully-disconnected gap between the two curls were delivered to no
one on either node and are not recovered, so the application `seq` still jumps across that
window even though the SSE event ids stay contiguous.

## Try it in a browser

Open `http://127.0.0.1:8080/` in a browser. The connection id is generated once with
`crypto.randomUUID()` and kept in `localStorage`, so it survives a page reload the same
way `demo` survives across the `curl` calls above. The page lists every tick it receives
and marks any `seq` jump in red.

Two things to try:

- **Reload the page** while the stream is live. The reload drops the socket, but for a
  brief window the server has not noticed and is still recording; `EventSource` reconnects
  under the same id (from `localStorage`) with `Last-Event-ID` and catches up on the ticks
  recorded in that window with no red row.
- **Throttle Network to Offline** in devtools for a few seconds, then restore it. Whether
  you see a red `seq` jump depends on whether the server had already noticed the drop and
  unregistered before you came back: a clean, promptly-noticed disconnect loses the
  offline ticks (a red row), matching the curl `Ctrl-C` case above. Stay offline longer
  than the history window and a reconnect that does replay will show the red "SERVER GAP"
  row first.

## The `retry:` field

`WithSSERetry(2 * time.Second)` (set in `main.go`) writes `retry: 2000` as the first line
of every stream. This is a standard SSE field: it tells the browser's `EventSource` how
long to wait before automatically reconnecting after the connection drops, which is the
knob that makes the browser experiments above resolve in about two seconds instead of the
browser's own three-second default. `curl` does not implement automatic reconnection at
all, which is why the curl walkthrough reconnects by hand.

## History capacity (what happens when a client is offline too long)

`main.go` configures `NewMemorySSEHistory(20)` - each connection keeps only its last 20
delivered events, i.e. about 20 seconds of ticks at one tick/second. This is
deliberately small so the gap behavior is easy to trigger in a quick manual test; a real
deployment would size it for its actual expected reconnect-gap distribution (and, if it
runs more than one node, would set `REDIS_ADDR` to use the shared `ssehistory/redis`
backend so replay survives a reconnect to any node - see "Cross-node replay with Redis /
Valkey" above).

Reconnect after the window has passed and the server cannot find your `Last-Event-ID`
anymore: you get the `gateway-gap` event described above, followed by whatever is still
in the (now-overwritten) buffer, then the live stream. Nothing panics or errors out -
losing history past its retention window is an expected, handled case, not a failure
mode.

## Verifying it worked

- `go vet ./examples/sse-resume/...` passes.
- The overlapping-curl walkthrough shows the second connection replaying exactly the ticks
  the first recorded after its `Last-Event-ID`, with the `seq` field inside the JSON
  payload continuing without a gap across the handover (a fresh curl process always starts
  its printed `id:` at `demo-<n>` wherever the server's per-connection counter is; the
  `seq` inside the payload - the thing this sample is about - is what must not skip).
- The clean-`Ctrl-C`-then-reconnect walkthrough shows the opposite, expected outcome: the
  `seq` jumps, because the ticks produced while nothing held the id were never recorded.
- The gap walkthrough shows an `event: gateway-gap` line appear before replay when the
  requested `Last-Event-ID` is unresolvable.
- The browser page catches up with no red row after a quick page reload, and shows a red
  `seq` jump when a disconnect was noticed before it reconnected, or a "SERVER GAP" row
  when it comes back after more than the 20-second history window.
