# presence-watch example

## What this demonstrates

"A friend came online" is a different question from "is my friend online right now": the
first needs a live event the instant a friend connects or disconnects, the second is
answered fine by a single point-in-time check. Polling `Registry.GroupMembers` or
`Registry.IsOnline` on a timer can answer the second question, but it either misses
transient sessions between polls or burns a request per tick to catch them. This example
is the minimal, runnable version of the first question:

- **`Registry.WatchPresence`** - a long-lived subscription to one identity group's
  membership changes. It returns a `<-chan gateway.PresenceEvent` of `PresenceJoin` /
  `PresenceLeave` events, a `cancel` function, and an error (`gateway.ErrPresenceWatchUnsupported`
  when the configured `Presence` backend does not implement `gateway.PresenceWatcher`).
  The example exposes this subscription over a plain SSE endpoint (`/watch`) so a browser
  tab can watch it live, but the API itself is just a Go channel - no HTTP involved.
- **`Registry.GroupMembers`** - the point-in-time counterpart: every online member of a
  group right now, each with the metadata (`ConnInfo.Meta`) it registered with. Useful for
  painting the initial state a watch then keeps current, or for a one-off "who's online"
  check that doesn't justify holding a subscription open.
- **Metadata through presence.** A connection's `ConnInfo.Meta` (here, which `device` it
  connected from) flows through `Registry.Register` into the `Presence` backend via the
  optional `gateway.PresenceMetaJoiner` extension, so `GroupMembers` can report it back
  even for a backend (`presence/redis`) that stores membership and metadata cluster-wide.

`REDIS_ADDR` selects the `Presence` backend, exactly as in `examples/notification`: unset
uses the in-process `gateway.MemoryPresence` (this node's connections only), set uses
`presence/redis.Presence` (shared state, real cluster-wide membership). Both implement
`gateway.PresenceWatcher` and `gateway.PresenceDirectory`, so `/watch` and `/members` work
identically against either - only the scope of "who counts" changes.

## Why this is genuinely cross-node, unlike some of this library's other examples

`examples/notification`'s README explains a real limitation: `Registry.SendToGroup`'s
cluster fan-out rides the GoAkt actor system's cluster transport, and a single-process
demo (no cluster discovery configured) can never actually prove a message crosses a
process boundary - only that the local fan-out path and the `Presence`-backed online
verdict behave correctly.

`WatchPresence` and `GroupMembers` do not have that limitation when `presence/redis` is
configured. Both talk to Redis directly (`Watch` subscribes over Redis Pub/Sub, `Entries`
reads the group's sorted set and metadata hash) rather than through the actor system's
cluster transport. Run two instances of this binary against the same `REDIS_ADDR`,
connect a socket to instance A, and instance B's `/watch` stream and `/members` snapshot
both see it - no GoAkt cluster wiring required. This is demonstrated below.

## Why `actor.WithPubSub()` is still required

Even though this example never calls `Registry.SendToGroup`, the actor system is still
created with `actor.WithPubSub()`. `Registry.Register` sets up a group's local fan-out
bridge for *any* grouped connection (see `finalizeRegistration` in `registry.go`), and
that bridge always rides the actor system's topic actor - regardless of which delivery
APIs the application goes on to use. Leaving `WithPubSub()` out makes every grouped
`Register` call fail with `gateway: pub/sub is not enabled on this actor system`.

## How to run

Single node, in-process presence (only sees this process's own connections):

```bash
go run ./examples/presence-watch
```

Cluster-shared presence over Redis or Valkey (the repo-root `docker-compose.yml` starts
one of each):

```bash
REDIS_ADDR=127.0.0.1:6379 go run ./examples/presence-watch
```

The server listens on `http://127.0.0.1:8080` and exposes:

- `GET /` - a browser page: watch a user's presence in one panel, connect/disconnect as a
  user in another, and check a point-in-time member snapshot in a third.
- `GET /ws?user=<id>&device=<name>` - the WebSocket upgrade endpoint. Each upgrade is one
  connection with `ConnInfo{ID: "<id>-<uuid>", Group: "user:<id>", Meta: {"device": name}}`
  (`device` is optional).
- `GET /watch?user=<id>` - Server-Sent Events stream of `Registry.WatchPresence(ctx,
  "user:<id>")`, one `data:` line per join/leave: `{"group":"user:alice","connID":"...","kind":"join"}`.
- `GET /members?user=<id>` - `Registry.GroupMembers(ctx, "user:<id>")` as a JSON array of
  `{"ConnID":"...","Meta":{"device":"..."}}`.

## How to verify / what success looks like

Start the server, then in one terminal open the watch stream:

```bash
curl -sN "http://127.0.0.1:8080/watch?user=alice"
```

In another terminal, connect and disconnect a socket for `alice` (any WebSocket client
works; a one-line `github.com/coder/websocket` program is enough). The `curl` above
prints, in order, as the socket opens and then closes:

```
data: {"group":"user:alice","connID":"alice-<uuid>","kind":"join"}

data: {"group":"user:alice","connID":"alice-<uuid>","kind":"leave"}
```

While the socket is still open, a snapshot confirms the same state without watching:

```bash
curl -s "http://127.0.0.1:8080/members?user=alice"
# [{"ConnID":"alice-<uuid>","Meta":{"device":"phone"}}]
```

After it disconnects:

```bash
curl -s "http://127.0.0.1:8080/members?user=alice"
# []
```

The same sequence against `presence/redis` (`REDIS_ADDR` set) additionally proves the
cross-node claim above: run a second instance of this binary on a different port
(`gateway.NewServer` address is hardcoded to `127.0.0.1:8080` in `main.go` - edit it
locally to try this, e.g. `127.0.0.1:8081`) pointed at the same Redis, `curl -sN` its
`/watch?user=alice`, connect a socket to the *first* instance, and watch the join/leave
events arrive on the second instance's stream.

Driving the browser page at `http://localhost:8080/` end to end: open "1. Watch a user's
presence" with `alice`, then in "2. Connect/disconnect as a user" connect as `alice` with
device `phone` - the watch panel's log immediately shows `[presence]
{"group":"user:alice",...,"kind":"join"}`; clicking Disconnect shows the matching
`"kind":"leave"` line. "3. Snapshot current members" against `alice` while connected shows
the device metadata; after disconnecting it shows `[]`.

This was verified during development with `go vet ./examples/presence-watch/...` (clean)
and a live run against the in-process `MemoryPresence` backend: `go run
./examples/presence-watch`, a `github.com/coder/websocket` client connecting and
disconnecting as `alice`, `curl -sN /watch?user=alice` running throughout (captured both
the join and leave SSE lines above verbatim), and `curl /members?user=alice` checked both
mid-connection (returned the entry with `device":"phone"` metadata) and after disconnect
(returned `[]`).
