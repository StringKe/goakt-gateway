# Changelog

All notable changes to this project are documented here.

This project is pre-1.0. Breaking changes are expected and are not softened with
compatibility shims: there is one right way to call each API, and the old way is removed
rather than deprecated.

## v0.2.0

The next release is `v0.2.0`. It contains the first substantial reshaping of the connection
API since the initial release. Every
breaking change below exists to close a real gap: connection identity was not addressable
as a *person* (only as a socket), fan-out could not tell a caller whether anything actually
happened, and the WebSocket layer was built on an unmaintained library.

### Breaking changes

#### 1. `Registry.Register` takes options instead of a topic list

Registration now carries identity (`Group`), subscriptions (`Topics`), and application
metadata (`Meta`), so a variadic `topics ...string` tail no longer fits.

```go
// before
err := registry.Register(ctx, id, send, "room:42", "room:7")

// after
err := registry.Register(ctx, id, send,
    gateway.WithConnTopics("room:42", "room:7"),
    gateway.WithConnGroup("user:123"),               // new: the identity behind the socket
    gateway.WithConnMeta(map[string]string{"tenant": "acme"}),
    gateway.WithReplaceExisting(),                   // new: take over an existing id instead of failing
)
```

A registration with no options is unchanged in spirit:

```go
// before
err := registry.Register(ctx, id, send)
// after: identical
err := registry.Register(ctx, id, send)
```

**Also:** `Register` and `Join` now return an error when a topic or group bridge cannot be
established, instead of logging a warning and returning success. A connection that silently
never receives a broadcast is worse than a failed registration. The concrete consequence:
**registering with a group or with topics requires an actor system started with
`actor.WithPubSub()`**, otherwise you get `ErrPubSubUnavailable`. A failed `Register` rolls
back completely; there are no half-registered connections.

```go
system, err := actor.NewActorSystem("gateway", actor.WithPubSub())
```

#### 2. `Registry.Broadcast` returns a `DeliveryResult`

A bare `error` could not answer the only question a caller has after a fan-out: did this
reach anyone, and do I need an offline fallback?

```go
// before
err := registry.Broadcast(ctx, "room:42", payload)

// after
res, err := registry.Broadcast(ctx, "room:42", payload)
if err != nil {
    return err
}
// res.Delivered / res.Dropped / res.Remote; res.Total(); res.None()
```

If you only care about the error, `_, err := registry.Broadcast(...)` is a faithful
translation of the old behavior.

The new `Registry.SendToGroup` has the same shape and is the intended way to reach an
identity's every device:

```go
res, err := registry.SendToGroup(ctx, "user:123", payload)
if err != nil {
    return err
}
if res.None() {
    return webpush.Send(ctx, "user:123", payload) // nobody online: fall back
}
```

`res.None()` is only cluster-accurate when a `Presence` backend is configured (see below).
Without one it means "nobody on this node", which is not the same claim.

#### 3. WebSocket now runs on `github.com/coder/websocket`

`golang.org/x/net/websocket` is dormant, has no ping/pong, no read limits, no close codes,
and no context support. The handler is rebuilt on `github.com/coder/websocket`.

Visible consequences:

- `WithWSMessageType` now takes a `websocket.MessageType` from
  `github.com/coder/websocket` (`websocket.MessageText`, the default, or
  `websocket.MessageBinary`).
- Origin checking is now enforced by default: only same-`Host` requests are accepted. Widen
  it with `WithWSOriginPatterns("app.example.com")`, or turn it off with
  `WithWSInsecureSkipOriginCheck()` - which allows any website to open an authenticated
  socket to you using the visitor's cookies, so read its godoc before reaching for it.
- `Drain()` now sends a real `1001 GoingAway` close frame before dropping the socket, so
  clients can distinguish a rolling deploy from a network failure.
- New knobs that did not exist before: `WithWSPingInterval` (default 30s),
  `WithWSPongTimeout` (10s), `WithWSWriteTimeout` (10s), `WithWSReadLimit` (1 MiB),
  `WithWSInboundRate`, `WithWSSubprotocols`, `WithWSBackpressurePolicy`.

If you wrote your own WebSocket client against this server, nothing changes on the wire; if
you wrote tests using `golang.org/x/net/websocket` as a *client*, they will need the same
migration this repository's own tests took.

#### 4. Auth and lifecycle callbacks receive a `*ConnInfo`

The old design parsed the caller's token up to three times (once in `AuthFunc`, once in
`IDFunc`, once in `TopicsFunc`) and then handed callbacks a bare `id string` that had lost
every other fact about the caller. Authentication now resolves identity exactly once, and
everything downstream receives it.

```go
// before
gateway.NewWSHandler(registry,
    gateway.WithWSAuthFunc(func(r *http.Request) error { return verify(r) }),
    gateway.WithWSIDFunc(func(r *http.Request) string { return parseToken(r).DeviceID }),
    gateway.WithWSTopics(func(r *http.Request) []string { return roomsOf(parseToken(r)) }),
    gateway.WithWSOnMessage(func(ctx context.Context, id string, payload []byte) { ... }),
    gateway.WithWSOnConnect(func(ctx context.Context, id string, r *http.Request) { ... }),
    gateway.WithWSOnDisconnect(func(id string) { ... }),
)

// after
gateway.NewWSHandler(registry,
    gateway.WithWSAuth(func(r *http.Request) (*gateway.ConnInfo, error) {
        claims, err := verify(r)
        if err != nil {
            return nil, err // rejects the handshake with 403
        }
        return &gateway.ConnInfo{
            ID:     claims.DeviceID,
            Group:  "user:" + claims.UserID,
            Topics: roomsOf(claims),
            Meta:   map[string]string{"tenant": claims.Tenant},
        }, nil
    }),
    gateway.WithWSOnMessage(func(ctx context.Context, info *gateway.ConnInfo, payload []byte) { ... }),
    gateway.WithWSOnConnect(func(ctx context.Context, info *gateway.ConnInfo, r *http.Request) { ... }),
    gateway.WithWSOnDisconnect(func(info *gateway.ConnInfo) { ... }),
)
```

- `WithWSAuthFunc` is removed; use `WithWSAuth`. Same for `WithSSEAuthFunc` ->
  `WithSSEAuth`.
- `WithWSIDFunc` and `WithWSTopics` (and their SSE twins) still exist, but only as
  *fallbacks* for fields that auth left empty. An empty `ConnInfo.ID` is assigned a
  generated UUID.
- SSE's `OnConnect`/`OnDisconnect` changed identically.

#### 5. Reconnect takeover is the default

A reconnecting client whose connection id is already registered now takes over: the old
registration is replaced and its socket closed. Previously the second connection failed with
`ErrConnectionExists`.

This is a behavior change with a blunt justification: with half-open TCP, the "old"
connection is frequently a socket whose peer is already gone and whose death will not be
noticed for minutes. Failing the reconnect locks a user out of their own account for that
entire window. `Observer.ConnectionReplaced` reports every takeover, so a reconnect loop is
visible rather than silent.

`Registry.Register` remains strict by default (`ErrConnectionExists`) and only takes over
when given `WithReplaceExisting()`; it is the WS/SSE handlers that pass it.

In a cluster the takeover reaches across nodes: when the existing id is held on another
node, `Register` evicts that remote owner (closing its socket and freeing the
cluster-unique actor name) before claiming the id. Eviction is bounded - if the remote
owner cannot be displaced within the takeover budget, `Register` returns the new
`ErrTakeoverTimeout` rather than blocking indefinitely.

#### 6. Certificate domain admission is deny-by-default when a `CertIssuer` is configured

Previously, a `Manager` configured with `WithCertIssuer` but neither `WithAllowedDomains`
nor `WithDomainPolicy` admitted every domain - any SNI value a peer sent could trigger a
real issuance against the CA. It now refuses every domain with `ErrDomainNotAllowed`
instead: admission must be declared explicitly.

```go
// before: any SNI reaching an issuer-only Manager was issued for
manager := gateway.NewManager(system, logger, gateway.WithCertIssuer(issuer))

// after: every domain is refused until you declare which ones are servable
manager := gateway.NewManager(system, logger,
    gateway.WithCertIssuer(issuer),
    gateway.WithAllowedDomains("chat.example.com", "*.tenants.example.com"), // or WithDomainPolicy
)
```

This only affects a `Manager` with a `CertIssuer` configured. Without one, `Manager` can
only ever serve a certificate a `CertStore` already has or a `WithFallbackCertificate`
provides, so admitting every domain remains the default and nothing changes.

#### 7. `Outbox.Append` and `Outbox.Ack` signatures changed

This only breaks a custom `Outbox` implementation (`NewMemoryOutbox` and `persistence/redis`
are updated); `Registry.Ack`'s own public signature is unchanged.

```go
// before
Append(ctx context.Context, connID, msgID string, payload []byte) error
Ack(ctx context.Context, connID, msgID string) error

// after
Append(ctx context.Context, connID string, payload []byte) (msgID string, seq uint64, err error)
Ack(ctx context.Context, connID, msgID string, generation uint64) error
```

`Append` now mints `msgID` itself instead of taking a caller-supplied one, and returns it
alongside the assigned `Seq` - used by `WithOutboxEnvelope` to carry both back to the
client. `Ack` gained a `generation` parameter so a stale node's `Ack` (see `WithOwnerLease`
below) can be fenced instead of accepted; pass `0` if you do not use `WithOwnerLease`.

### Added

- **`Presence`** - cluster-wide "which connections does this identity hold?", with TTL'd
  leases refreshed in the background by `Registry`. `NewMemoryPresence()` for a single
  process; `presence/redis.NewPresence(client)` for a real cluster. Wire it with
  `WithPresence` / `WithPresenceTTL`. Without it, `IsOnline` and `LocalConnectionsOf` only
  ever see the local node.
- **`Registry.Close(ctx)`** - stops the presence refresh goroutine and tears down bridges.
  Call it. It is idempotent.
- **`Observer`** - optional hooks for registration, takeover, backpressure drops, delivery
  failures, and broadcast fan-out. Wire it with `WithObserver`. All methods must be cheap
  and non-blocking; a nil `Observer` is the default.
- **`BackpressurePolicy`** - `BackpressureDrop` (default, previous behavior) or
  `BackpressureClose`. Set per handler with `WithWSBackpressurePolicy` /
  `WithSSEBackpressurePolicy`.
- **SSE resume** - `WithSSEHistory` plus `NewMemorySSEHistory(perConn)` replay events after
  a client's `Last-Event-ID`. When the requested id has aged out, the client is told so with
  an explicit `gateway-gap` event (`SSEGapEventName`) instead of silently resuming from a
  hole. Also `WithSSERetry` and `WithSSEEventName`.
- **`Registry.IsOnline`, `Registry.LocalConnectionsOf`, `Registry.SendToGroup`**.
- **Certificate domain admission** - `WithDomainPolicy` (dynamic, for custom domains stored
  in a database), `WithNegativeCacheTTL` (default 1 minute, so junk SNI cannot flood your
  CA), `WithMaxCachedCerts` (default 1024, LRU, so junk SNI cannot exhaust memory).
- **`WithFallbackCertificate`** and **`ReloadingCertificate`** - for the common deployment
  where TLS is terminated at an edge (Cloudflare) and the origin serves one long-lived
  catch-all certificate. `ReloadingCertificate` handles Kubernetes Secret/ConfigMap
  rotation correctly (it re-reads and hashes the pair rather than trusting mtime, because
  the rotation swaps a `..data` symlink) and keeps the last known-good certificate when a
  reload fails.
- **`NewFileCertStore(dir)`** - a persistent `CertStore` on disk (`dir/<domain>.crt`,
  `dir/<domain>.key`, atomic writes, key mode 0600). `Get` re-reads on a cert/key mismatch so
  a handshake that races a renewal never observes the new certificate beside the old key across
  the two file renames - the pair swap is all-or-nothing to callers, matching
  `MemoryCertStore` and `store/redis`, and `store/conformance` now asserts this on a shared
  domain.
- **`store/redis`** - a shared `CertStore` backed by Redis or Valkey (one string key per
  domain, no TTL), so a cold-started node reads already-issued certificates back from the
  server without depending on the issuer. Constructed with `store/redis.New(client)` and
  namespaced with `WithKeyPrefix`. A separate package so the root module stays free of
  `github.com/redis/go-redis/v9`. This is a pure addition; nothing existing changes.
- **`ssehistory/redis`** - a shared `SSEHistory` backed by Redis or Valkey (one bounded
  LIST per connection, trimmed and TTL-refreshed atomically in one Lua script), so
  Last-Event-ID replay works when a client reconnects to *any* node, not only the node
  that recorded the buffer - the limitation `MemorySSEHistory` has. Constructed with
  `ssehistory/redis.New(client)`, with `WithKeyPrefix`, `WithPerConn`, and `WithTTL`. A
  separate package for the same reason. Also a pure addition. The `SSEHandler` re-arms the
  idle TTL on every keepalive, so a still-connected but low-traffic stream that emits no real
  event for longer than the TTL keeps its buffer for as long as it stays up instead of
  expiring mid-connection and answering the reconnect with a false gap - the same drop-in
  behaviour `MemorySSEHistory` gives, whose buffers are never reclaimed on a timer.
- **`store/conformance` and `ssehistory/conformance`** - shared assertion suites (mirroring
  `coordinator/conformance` and `presence/conformance`) run against both the in-memory and
  the RESP implementation of each abstraction. All five RESP backends
  (`coordinator/redis`, `presence/redis`, `store/redis`, `ssehistory/redis`, `persistence/redis`) are verified
  against both a real Redis server and a real Valkey server; a root `docker-compose.yml`
  starts one of each for local runs.
- **`WithServerErrorLog`** - route `http.Server`'s error log somewhere you control, so
  load balancer probes that open a TCP connection without completing a TLS handshake stop
  printing `http: TLS handshake error ...: EOF` to stderr.
- **New errors**: `ErrOriginNotAllowed`, `ErrPayloadTooLarge`, `ErrRegistryClosed`,
  `ErrRateLimited`, `ErrHistoryGap`.
- **Examples**: `chat`, `cluster`, `notification`, `sse-resume`, `graphql-ws`,
  `tls-cloudflare`, `offline-push`, `delivery-confirm`, `persistence`, `presence-watch`,
  `reauth-kick`, alongside the existing `echo` - twelve in total.

#### Opt-in reliability and lifecycle (all off by default, zero cost unless wired)

Every item below is a constructor option or optional interface. The default `Registry` and
handlers are unchanged: one fire-and-forget socket write, no persistence, no extra
round-trip, no new goroutine. None of this is a breaking change; a caller that adopts none
of it sees identical behavior to before.

- **`WithOfflineChannel(ch)`** - when `SendToGroup` finds the target offline cluster-wide
  (`DeliveryResult.None()`), the `Registry` calls `ch.Deliver(ctx, group, payload)` off the
  main delivery path. The new `OfflineChannel` interface and the optional
  `OfflineObserver.OfflineFallback(group, err)` hook (the core six `Observer` hooks are
  unchanged). `offline/webpush` is a VAPID Web Push implementation in a separate package.
- **`WithDeliveryConfirmation()` / `WithConfirmationTimeout(d)`** (default 5s) - the remote
  hand-off switches from `Tell` to `Ask`, so `DeliveryResult.Remote` counts confirmed remote
  socket writes instead of fan-out. New error `ErrConfirmationTimeout`. The default `Tell`
  path is untouched: `connActor` answers the confirmation unconditionally and that answer is
  a no-op under `Tell`.
- **`WithOutbox(o)` + `Registry.Ack(ctx, connID, msgID)`** - opt-in at-least-once. With an
  `Outbox` wired, `SendToConnection` persists then delivers, and a fresh registration
  redelivers unacked messages. `Outbox.Append(ctx, connID, payload) (msgID, seq, error)` mints
  the `msgID` itself and assigns the per-connection monotonic `Seq` from its own durable,
  shared state, so the sequence stays correct across a process restart and across nodes
  appending to the same connection (a per-process counter would restart at 1 and collide).
  The counter is reclaimed only by `DropConn` (or a `persistence/redis` `WithTTL`). New
  `Outbox` interface, `PersistedMessage` struct (the return shape of `Unacked`),
  `NewMemoryOutbox()` (process-local), and `persistence/redis` (a fifth RESP backend, HASH per
  connection with reserved sequence/ack-generation fields). `Registry.Ack` is a no-op
  returning nil with no `Outbox`. At-least-once means duplicates are possible; clients dedupe
  on `ID`/`Seq`. `WithOutboxEnvelope` makes real-time delivery and reconnect replay use the
  same ASCII base64 frame containing the message ID, sequence, and original payload. Without
  `WithOutboxEnvelope`, both paths deliver the original raw payload and the application owns
  any acknowledgement framing.
- **`Registry.WatchPresence` / `Registry.GroupMembers`** - optional presence capabilities
  discovered by type assertion. New `PresenceWatcher`, `PresenceDirectory`,
  `PresenceMetaJoiner` interfaces, `PresenceEvent`/`PresenceEntry`/`PresenceEventKind`
  types, and `ErrPresenceWatchUnsupported`. `MemoryPresence` and `presence/redis` implement
  all three (`presence/redis` watch is Redis Pub/Sub, best-effort). `WatchPresence` returns
  `ErrPresenceWatchUnsupported` on a backend without `PresenceWatcher`; `GroupMembers` falls
  back to the local group index without `PresenceDirectory`. `WithConnMeta` metadata now
  flows to the backend via `JoinWithMeta` when supported; `Refresh` never re-emits a join or
  drops metadata.
- **`Registry.Disconnect` / `Registry.DisconnectGroup` + `WithConnCloseHook`** - forced
  eviction driven by a close hook registered atomically at `Register` time (race-free with
  normal `Unregister`). WS sends `StatusPolicyViolation` (1008) + reason (clamped to the
  123-byte close-frame limit on a UTF-8 boundary, so an over-long reason still closes cleanly
  as 1008 rather than degrading to an abrupt 1006); SSE ends with a terminating comment.
  `Disconnect` drives the close only; teardown runs the existing unregister path, so
  takeover/reserve rollback is unchanged.
- **`WithWSReauth(interval, f)` / `WithSSEReauth(interval, f)`** - periodic re-authorization
  on the original handshake request; on failure the connection is closed (1008 for WS) and
  unregistered.
- **`WithWSCompression(mode)`** - `permessage-deflate` via coder/websocket's
  `CompressionMode`; default `CompressionDisabled` (zero value, wire untouched).
- **`WithWSGroupRate(perSecond, burst)`** - a per-group inbound token bucket shared by a
  node's local connections of that group, composing with `WithWSInboundRate` (both must
  pass). Empty group is never limited.
- **`observer/otel`** - an OpenTelemetry `Observer` (`NewObserver(meter, opts...)`) mapping
  the six hooks to counters/up-down-counters, plus the optional `OfflineFallback` hook. A
  separate package so the root module stays free of the OTel SDK.
- **`WithOwnerLease(c)`** - strict multi-instance connection ownership. The GoAkt actor
  directory is PA/EC eventually consistent, so two nodes can race `Register` for the same
  connection id, both observe no owner, and both succeed (split brain); `WithOwnerLease`
  closes that window with a CAS-arbitrated lease. `c` must implement the new
  `LinearizableFencingCoordinator`: `MemoryCoordinator` supplies it for one process, while
  `coordinator/redis.Coordinator` intentionally does not because asynchronous-replication
  failover cannot provide strict fencing. Multi-instance strict ownership requires a
  consensus-backed provider. Every takeover bumps a monotonically increasing generation.
  Every fencing-aware operation -
  every local delivery path (`SendToConnection`, `SendToGroup`, `Broadcast`) and the
  cross-node `connActor` delivery check, background lease renewal, `Presence` `Refresh`/`Leave`
  (`PresenceFencer`, implemented by `MemoryPresence` and `presence/redis`), SSE history append
  and takeover-time generation advance (`GenerationalHistory`, implemented by
  `MemorySSEHistory` and `ssehistory/redis`), `Outbox` `Ack` and takeover-time generation
  advance (`OutboxGenerationAdvancer`, implemented by `NewMemoryOutbox` and
  `persistence/redis`), and `Registry.RegisterHandle`/`ConnHandle.UnregisterHandle`'s
  entry-guarded unregister - rejects a caller whose generation has been superseded with the
  new `ErrStaleOwner`. A takeover whose physical eviction never completes (e.g.
  `ErrTakeoverTimeout`) restores the lease to whichever owner it preempted instead of
  permanently fencing out a connection that was never actually taken over. Without
  `WithOwnerLease` (the default) a `Registry` keeps its current single-instance semantics at
  zero cost, mirroring the `WithDeliveryConfirmation` opt-in precedent. New errors
  `ErrStaleOwner`, `ErrOwnerHeld`, `ErrOwnerLeaseUnsupported`. New optional interfaces
  `CASCoordinator` and `LinearizableFencingCoordinator` (`coordinator.go`), `GenerationalHistory` (`sse_history.go`),
  `OutboxGenerationAdvancer` (`persistence.go`), and `PresenceFencer` (`presence.go`).

### Fixed

- **Connection actor shutdown race** - `SendToConnection` and `Disconnect` now report
  `ErrConnectionNotFound` when a connection actor has already started shutdown but its PID is
  still briefly resolvable. Takeover, confirmed delivery, and group disconnect paths treat the
  same stale PID as unavailable instead of surfacing `actor is not alive` or logging a false
  failure.

### Changed

- `coordinator/redis.New` now takes a `redis.UniversalClient` instead of a
  `redis.Cmdable`, matching `presence/redis`, `store/redis`, and `ssehistory/redis`. All
  five RESP backends now share one constructor shape, so a single
  `redis.NewUniversalClient(...)` (standalone, Cluster, Sentinel, or Ring) can back every
  one of them. A `*redis.Client` already satisfies `UniversalClient`, so existing
  standalone callers need no change.
- `WithAllowedDomains` is now a *pattern* list, not an exact-match list. Existing exact
  domains keep working; `*.example.com` now also matches a single label
  (`a.example.com`, not `a.b.example.com`). Domains are normalized (lowercased, trailing
  dot stripped) before matching.
- `Manager.GetCertificate` with an empty SNI now serves the fallback certificate if one is
  configured, instead of failing. A domain that is explicitly *not allowed* is still
  rejected and never served the fallback.
- A `Manager` with no `CertIssuer` now returns `ErrNoIssuer` immediately rather than taking
  the coordinator lock first. A heterogeneous cluster in which only some nodes have an
  issuer will therefore not wait on a peer's in-flight issuance.
- `Broadcast`'s `WithExclude` list now travels inside the cluster envelope, so exclusions
  are honored on remote nodes too, not just the originating one.
- `presence/redis`'s internal Redis key layout changed (a safer Cluster hash-tag construction
  that no longer breaks on an empty group name or one starting with `}`, plus a co-located
  generation-fencing key). Existing keys from a running deployment do not migrate; they age
  out on their own TTL after an upgrade, which is safe since presence entries are always
  short-lived leases, never durable state.
- The certificate renewal actor `Manager.Start` spawns is now named uniquely per node
  (previously every node used the same fixed name, so a second replica's renewal actor failed
  to spawn with `ErrActorAlreadyExists` and `Server.ListenAndServe` failed with it). The
  cluster-single-fire cron schedule reference is unchanged, so renewal still runs exactly once
  per window across the cluster.
- `WSHandler.Drain()` / `SSEHandler.Drain()` now block until the drained connection's
  `Registry` unregister has actually completed, not merely until the socket/stream ends, so a
  rolling deploy no longer leaves a briefly stale actor or owner lease behind.

### Removed

- `WithWSAuthFunc`, `WithSSEAuthFunc` (replaced by `WithWSAuth` / `WithSSEAuth`).
- The `id string` forms of `WithWSOnMessage`, `WithWSOnConnect`, `WithWSOnDisconnect` and
  their SSE equivalents (replaced by the `*ConnInfo` forms).
- The `golang.org/x/net/websocket` dependency.

## 0.1.0

Initial release: `Registry` with two-tier (local-first, cluster-fallback) delivery,
WebSocket and SSE handlers, cluster-shared TLS via `Manager` + `Coordinator` +
`CertIssuer`, Cloudflare Origin CA issuance, Authenticated Origin Pulls, and `Server`.
