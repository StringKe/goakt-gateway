# reauth-kick example

## Why this matters

A WebSocket handshake proves who the caller was at the moment it happened. It says
nothing about who they still are a minute, an hour, or a day into a long-lived
connection. In a real system, authorization changes underneath live sockets constantly:

- a role is downgraded (an admin is demoted to a regular user),
- a session is revoked (a password reset, a "log out everywhere" button),
- a subscription lapses,
- a short-lived token expires and the client never got a chance to refresh it,
- support or trust-and-safety decides a specific session needs to end right now,
  independent of whether the user is still authorized at all.

None of these are things a `websocket.Accept` call at connect time can ever see. A
gateway that only checks authorization at handshake time is a gateway where "revoke
access" quietly does nothing to anyone already connected.

This library gives you two independent, composable mechanisms for this:

1. **`WithWSReauth(interval, f)`** - the *passive* path. The handler periodically
   re-runs your auth function against the retained upgrade request. The moment it
   returns an error, the connection is closed with WebSocket status `1008` (policy
   violation) and a fixed reason (`"reauthentication failed"`), and unregistered. You
   don't have to know which sockets exist or track them yourself - every connection
   finds out about its own revoked access on its own schedule, within `interval`.
2. **`Registry.Disconnect` / `Registry.DisconnectGroup`** - the *active* path. An admin
   action (a support tool, a "kick this device" button, a ban) force-closes one
   connection by id, or every connection sharing an identity group, immediately, with a
   caller-supplied reason - independent of whatever the reauth check looks at. This is
   the tool for "get this session off my server right now" when the reason has nothing
   to do with whether the credentials are still valid.

The two are deliberately decoupled: revoking a permission does not directly close a
socket (it waits for the next reauth tick), and kicking a socket does not touch any
permission state (the user can reconnect immediately, still authorized). This example
wires both to one shared in-memory permission store so you can watch the difference
between them: a revoke takes up to `reauthInterval` (3s in this demo, deliberately
short so the effect is visible quickly - a production deployment would use something
like 30s-5m, since a check runs against every open connection on every tick) to take
effect, while a kick is immediate.

## What this demonstrates

- `gateway.WithWSAuth` and `gateway.WithWSReauth` sharing one `WSAuthFunc`, so the
  reauth check enforces exactly the rule the handshake did, not an approximation of it.
- `Registry.Disconnect(ctx, id, reason)` closing one specific connection by id.
- `Registry.DisconnectGroup(ctx, group, reason)` closing every connection of an identity
  group (every tab/device of one user) in a single call.
- The close code and reason a browser actually observes in `WebSocket.onclose` for each
  path: `1008` / `"reauthentication failed"` for a reauth failure, `1008` / your own
  reason string for an admin-initiated `Disconnect`/`DisconnectGroup`.
- `Registry.SendToConnection` used right after connect to push each socket its own
  connection id, so the demo page can target `Disconnect` at one specific device.

## What this does NOT demonstrate

- A real permission/session store. `permissionStore` here is an in-memory
  `map[string]bool` with a mutex - good enough to flip a switch for the demo, not a
  design for anything persistent or shared across processes.
- Cross-node delivery. This runs one `actor.ActorSystem` with no cluster discovery, so
  every connection is local to the one process; `Disconnect`/`DisconnectGroup` still
  work the same way, they just never need their remote (`connActor`) path here. See
  `examples/cluster` for a real multi-node setup.
- Header/cookie-based reauth specifically. This demo's `WSAuthFunc` reads a `user` query
  parameter, which the retained upgrade request still carries at reauth time. A
  production auth function would more commonly re-check an `Authorization` header or a
  cookie - both of which are equally available on the retained request. What it cannot
  re-check is anything derived from the request body, since the body is not readable
  after the connection has been hijacked into a WebSocket.

## Running it

```
go run ./examples/reauth-kick
```

Then open <http://127.0.0.1:8080/> in a browser.

## Walking through it

1. **Connect.** Enter a user id (e.g. `alice`) and click *Connect*. The page opens a
   WebSocket to `/ws?user=alice`. On connect, the server pushes a `welcome` message
   carrying the connection's id, which the page shows and pre-fills into the kick field.
2. **Revoke (passive).** Enter the same user id under *2a* and click *Revoke*. Nothing
   happens to the socket immediately - the server only flips a flag. Watch the log:
   within 3 seconds (the demo's `reauthInterval`), the connection's reauth tick re-runs
   the same auth check, finds it now fails, and the socket closes with
   `code=1008 reason="reauthentication failed"`. Click *Grant back* and *Connect* again
   to reconnect (a granted user is allowed on the next handshake; the closed connection
   itself never comes back).
3. **Kick one connection (active).** Connect again, then paste (or use the pre-filled)
   connection id under *2b* and click *Kick this connection*. The close happens
   immediately - `code=1008` with whatever reason you sent - and unlike the revoke path,
   the user's access is untouched: reconnecting works right away.
4. **Kick every device (active).** Open the page in two tabs and connect both as the
   same user (e.g. `carol`). Under *2c*, enter `carol` and click *Kick all devices*: both
   sockets close immediately with your reason, in one server call, without touching any
   permission state.

## Success criteria

- Revoking a connected user's access closes their WebSocket(s) within `reauthInterval`
  (3s) with `event.code === 1008` and `event.reason === "reauthentication failed"`,
  without any explicit kick call.
- `POST /admin/kick?id=<id>&reason=<reason>` closes exactly the targeted connection
  immediately (sub-second) with `event.code === 1008` and `event.reason` equal to the
  reason you passed, while leaving the user's permission state (and any other of their
  connections) untouched.
- `POST /admin/kick-group?user=<user>&reason=<reason>` closes every open connection of
  that user's group immediately, with the same reason on each, and reports the count of
  connections it acted on in its response body.
- A user granted back after a revoke can reconnect and receive a fresh `welcome`
  message; a kicked connection can reconnect immediately without waiting on anything.

These were all verified directly against a running instance of this example with a
Go WebSocket test client (`coder/websocket`) during development: the revoke path closed
at `elapsed=3.002s` with `code=1008 reason="reauthentication failed"`; a single-id kick
closed in microseconds with the exact reason passed to `/admin/kick`; and a group kick
of two simultaneous connections for the same user closed both in microseconds with the
reason passed to `/admin/kick-group`, and the endpoint reported
`disconnected 2 connection(s)`.

## Endpoints

| Method | Path                | Query params      | What it does                                            |
|--------|---------------------|--------------------|----------------------------------------------------------|
| GET    | `/`                 | -                  | Demo page                                                 |
| GET    | `/ws`               | `user`             | WebSocket upgrade, authenticated as `user`                |
| POST   | `/admin/revoke`     | `user`             | Marks `user` unauthorized; open sockets close on next reauth tick |
| POST   | `/admin/grant`      | `user`             | Restores `user`'s authorization for future handshakes     |
| POST   | `/admin/kick`       | `id`, `reason`     | `Registry.Disconnect`: closes one connection immediately  |
| POST   | `/admin/kick-group` | `user`, `reason`   | `Registry.DisconnectGroup`: closes every connection of `user` immediately |
