# notification example

## What this demonstrates

This is a minimal, runnable version of the pattern every "push a notification to a
user" consumer of `gateway` needs (modeled on `mip-aio`'s
`backend/module/capability/notification/` + `backend/module/runtime/connrouter/`):

- **Multi-device fan-out.** A user can have several live connections at once - one per
  browser tab, one per device. Every connection for `user:<id>` shares the same
  `ConnInfo.Group`, so a single `Registry.SendToGroup` call reaches all of them without
  the caller enumerating connection ids.
- **`DeliveryResult.None()` as the offline signal.** `SendToGroup` returns a
  `DeliveryResult` with three counters (`Delivered`, `Dropped`, `Remote`). When
  `result.None()` is true, nothing anywhere took the message, and that is precisely the
  moment to fall back to an offline channel (real Web Push, email, ...). This example
  stands in a `fakeWebPush` log line for that channel. The old `Broadcast` API always
  returned `nil` and gave the caller no way to make this decision at all - this is the
  concrete capability the new return value adds.
- **Cluster-level presence.** `Registry.IsOnline` answers "is this identity connected
  anywhere", not "does this process hold a socket for it". The example wires either
  `gateway.MemoryPresence` (single node) or `presence/redis.Presence` (shared state)
  behind the same `gateway.Presence` interface, selected by the `REDIS_ADDR` environment
  variable.

## What this does NOT demonstrate

This example runs a single `actor.ActorSystem` (no cluster discovery configured). That
means:

- `Registry.SendToGroup`'s local fan-out path - a direct write to a socket this one
  process holds - is the only path that ever actually delivers a message here, connected
  or not.
- Configuring the Redis presence backend changes *what `IsOnline` and `SendToGroup`'s
  `Remote` counter compute their answer from*, but it does not, by itself, wire two
  processes' actor systems together over the network. If you point two instances of this
  binary at the same Redis and connect a socket to instance A, instance B's `IsOnline`
  will correctly report the user online (Redis says so) and its `SendToGroup` will report
  a nonzero `Remote` count (it published a cluster fan-out) - but that publish never
  reaches instance A's socket, because there is no real inter-process transport between
  them. Wiring that up needs a clustered GoAkt actor system (discovery + membership),
  which is a separate, much larger piece of infrastructure this notification example does
  not stand up.

In short: this example proves the *API contract* (group fan-out, `None()` as the offline
signal, `Presence` as the cluster-wide online source of truth). Proving *actual*
cross-node delivery requires a real GoAkt cluster on top, which is out of scope here.

## Why "online" has to be a cluster-level question

If a user's browser is connected to node B and an event that should notify them is
produced on node A, node A's local connection table is empty - it never held that
socket. A local-only `len(group) > 0` check on node A would (wrongly) say the user is
offline and trigger a needless Web Push while a live socket is sitting right there on
node B. `Registry.IsOnline` avoids this by delegating to `Presence` (when one is
configured): every node asks the same shared source of truth, so the answer does not
depend on which node answers it.

## How to run

Single node, in-process presence (default, no external dependencies):

```bash
cd /Users/xiaobai/Code/SelfCode/goakt-gateway
go run ./examples/notification
```

Single node, Redis-backed presence (requires a Redis or Valkey reachable at `REDIS_ADDR`;
still a single process - see the caveat above):

```bash
REDIS_ADDR=127.0.0.1:6379 go run ./examples/notification
```

`presence/redis` is one of five interchangeable Redis / Valkey backends this library ships
(`coordinator/redis`, `presence/redis`, `store/redis`, `ssehistory/redis`, `persistence/redis`). A real
deployment can point all five at one Redis or Valkey instance and keep their keys apart
with distinct `WithKeyPrefix` namespaces - presence here uses the package default; the
other three are demonstrated in the `tls-cloudflare` and `sse-resume` examples. Either a
Redis or a Valkey server works unchanged; the repo-root `docker-compose.yml` starts one of
each.

The server listens on `http://127.0.0.1:8080` and exposes:

- `GET /` - a tiny browser page: connect as a user, send a notification, check online
  status. Open it in two tabs to simulate two devices of the same user, or with different
  `user` values to simulate two different users.
- `GET /ws?user=<id>` - the WebSocket upgrade endpoint. Each upgrade is one connection
  with `ConnInfo{ID: "<id>-<uuid>", Group: "user:<id>"}`.
- `GET /notify?user=<id>&msg=<text>` - fans `msg` out to every connection of `user` via
  `Registry.SendToGroup`, returns the `DeliveryResult` as JSON plus whether the offline
  fallback fired.
- `GET /online?user=<id>` - `Registry.IsOnline` for that user's group, as JSON.

## How to verify / what success looks like

With the server running and nobody connected as `alice`:

```bash
curl -s "http://127.0.0.1:8080/online?user=alice"
# {"online":false}
curl -s "http://127.0.0.1:8080/notify?user=alice&msg=hi"
# {"delivered":0,"dropped":0,"remote":0,"offlineFallback":true}
```

The server log prints `[fakeWebPush] user "alice" is offline cluster-wide, would push:
"hi"` - the offline path fired because `DeliveryResult.None()` was true.

Open `http://127.0.0.1:8080/` in a browser, enter `alice` in the "Connect as a user"
field and click Connect, then repeat the same two curl calls:

```bash
curl -s "http://127.0.0.1:8080/online?user=alice"
# {"online":true}
curl -s "http://127.0.0.1:8080/notify?user=alice&msg=hi"
# {"delivered":1,"dropped":0,"remote":0,"offlineFallback":false}
```

The browser tab's log panel shows `[message] hi` - the socket received the push, and no
fallback log line was printed on the server.

Open a second browser tab, connect as `alice` again (a second device), and send another
`/notify?user=alice&msg=...`: both tabs' log panels receive the message, and
`delivered` in the JSON response is `2`.

This was verified during development with `go vet ./examples/notification/...` (clean)
and a live run: `go run ./examples/notification`, then the two curl sequences above
against the disconnected and connected states, and a `github.com/coder/websocket` test
client standing in for the browser to confirm the WebSocket delivery path.
