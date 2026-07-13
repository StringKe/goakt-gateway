# goakt-gateway

Cluster-aware ingress tier for [GoAkt](https://github.com/Tochemey/goakt): a WebSocket/SSE
connection registry with two-tier delivery, cluster-wide presence, and cluster-shared TLS
termination (Cloudflare Origin CA, Authenticated Origin Pulls).

## What it is

`goakt-gateway` turns a GoAkt cluster into its own ingress tier - the role an
nginx/caddy front-tier normally plays in front of a stateless HTTP fleet - for two
things specifically: giving long-lived WebSocket/SSE connections an addressable identity
so any node can deliver a message to them, and terminating TLS for domains shared
cluster-wide from a certificate arbitrated so that, as long as issuance completes within
the configured lock TTL, at most one process issues it (see
[Cluster-shared TLS](#cluster-shared-tls) for the lock-TTL caveat).

## Why it lives outside the actor core

This is a satellite library, not part of GoAkt itself, on purpose. A WebSocket gateway is
an opinionated shape on top of the actor system - a particular registry design, a
particular delivery model, a particular certificate-issuance workflow - and GoAkt's core
has no opinion on any of that. Keeping it as a separate module means:

- GoAkt's dependency footprint does not grow for users who never touch this;
- this library can iterate and version independently of GoAkt's release cadence;
- it is held to the same bar as any third-party consumer of GoAkt: it depends on GoAkt
  exclusively through the public `actor` API (`Spawn`, `ActorOf`, `TopicActor`,
  `Subscribe`/`Unsubscribe`, `ScheduleWithCron`, ...) - nothing here reaches into a GoAkt
  internal package, which is both proof that the public API is sufficient for this kind
  of integration and a guarantee that upgrading GoAkt does not silently break this
  library through an internal it was never supposed to depend on.

Two design boundaries drive every type in this package:

- **Plain HTTP requests are ordinary `net/http` handlers.** Nothing in this package puts
  an actor in the path of a request/response cycle that lives for a few milliseconds -
  that would be pure overhead.
- **Only long-lived WebSocket/SSE connections get an addressable identity.** Each
  accepted connection is registered in a local `Registry` and backed by a lightweight
  ephemeral actor (relocation disabled, long-lived passivation) whose only job is to
  relay a delivery to the socket it owns when another node needs to reach it.

## Install

```
go get github.com/StringKe/goakt-gateway
```

Requires Go 1.26 and `github.com/tochemey/goakt/v4` v4.3.1 or later. WebSocket support is
built on [`github.com/coder/websocket`](https://github.com/coder/websocket).

## Quickstart

A complete, runnable WebSocket echo server plus an HTTP handler that pushes a message
into a registered connection - the "HTTP handler talks to a websocket connection"
pattern the whole package exists for. A longer version of this lives in
[`examples/echo`](./examples/echo).

```go
package main

import (
	"context"
	"log"
	"net/http"

	"github.com/tochemey/goakt/v4/actor"
	golog "github.com/tochemey/goakt/v4/log"

	gateway "github.com/StringKe/goakt-gateway"
)

func main() {
	ctx := context.Background()

	system, err := actor.NewActorSystem("gateway-echo", actor.WithLogger(golog.DiscardLogger))
	if err != nil {
		log.Fatal(err)
	}
	if err := system.Start(ctx); err != nil {
		log.Fatal(err)
	}
	defer func() { _ = system.Stop(ctx) }()

	registry := gateway.NewRegistry(system, golog.DiscardLogger)
	defer func() { _ = registry.Close(ctx) }()

	mux := http.NewServeMux()

	mux.Handle("/ws", gateway.NewWSHandler(registry,
		gateway.WithWSAuth(func(r *http.Request) (*gateway.ConnInfo, error) {
			// Resolve the caller's identity once, here. Whatever this returns is what
			// the connection is registered as and what every callback below receives.
			return &gateway.ConnInfo{ID: r.URL.Query().Get("id")}, nil
		}),
		gateway.WithWSOnMessage(func(ctx context.Context, info *gateway.ConnInfo, payload []byte) {
			// The connection is always local to this process here, so
			// SendToConnection takes the direct-write fast path: no actor, no
			// cluster lookup.
			_ = registry.SendToConnection(ctx, info.ID, payload)
		}),
	))

	mux.HandleFunc("/send", func(w http.ResponseWriter, r *http.Request) {
		id := r.URL.Query().Get("id")
		if err := registry.SendToConnection(r.Context(), id, []byte(r.URL.Query().Get("msg"))); err != nil {
			http.Error(w, err.Error(), http.StatusNotFound)
			return
		}
		w.WriteHeader(http.StatusOK)
	})

	server, err := gateway.NewServer("127.0.0.1:8080", mux)
	if err != nil {
		log.Fatal(err)
	}
	log.Fatal(server.ListenAndServe(ctx))
}
```

Run it, connect a WebSocket client to `ws://127.0.0.1:8080/ws?id=alice`, then from
another terminal:

```
curl "http://127.0.0.1:8080/send?id=alice&msg=hello-from-http"
```

The message shows up on the WebSocket connection - delivered by an ordinary HTTP
handler, with no actor or cluster machinery involved because the connection is local.

## Registry and two-tier delivery

`Registry.SendToConnection`, `Registry.SendToGroup`, and `Registry.Broadcast` all follow
the same shape:

1. **Local hit: write directly.** If the target connection (or, for group/topic fan-out, a
   member) is registered on this node, the payload is written straight to the socket.
2. **Local miss: cluster fallback.** Only when the connection is not held locally does
   `Registry` resolve it through the cluster-aware actor directory
   (`ActorSystem.ActorOf`) and deliver remotely. For group and topic fan-out, remote
   members are reached through a small pub/sub bridge (see below) built on GoAkt's public
   `actor.Subscribe`/`actor.Unsubscribe` messaging.

Cross-node messaging - the only scenario that needs any cluster machinery at all - is
opt-in by construction: a single-node deployment never touches `ActorSystem.ActorOf` or
the topic actor.

Fan-out calls return a `DeliveryResult` rather than a bare error, because "did anything
happen?" is the question a caller actually needs answered:

```go
type DeliveryResult struct {
    Delivered int // written into a local connection's outbound buffer
    Dropped   int // a local connection had a full buffer, the message was dropped
    Remote    int // fanned out to remote nodes (whether the socket write happened is unknowable from here)
}

func (d DeliveryResult) Total() int  // Delivered + Dropped + Remote
func (d DeliveryResult) None() bool  // Total() == 0
```

`None()` is the signal to fall back to an offline channel (web push, email, a stored
inbox) - see [Groups and presence](#groups-and-presence).

### The pub/sub bridge

GoAkt's fork-only `ActorSystem.SubscribeTopic` convenience is not part of the public
API, so `Registry`'s topic fan-out is built from scratch on primitives that are: it
spawns its own small bridge actor per topic (`actor.ActorSystem.Spawn`), sends the
public `actor.Subscribe` message to `system.TopicActor()`, and forwards whatever the
topic actor delivers back to `Registry`'s local topic members. The bridge is torn down
with `actor.Unsubscribe` once the last local member leaves the topic. See `bridge.go`.

Because groups and topics both ride this bridge, **registering a connection with a group
or with topics requires an actor system started with `actor.WithPubSub()`**. A bridge that
cannot be established fails `Register`/`Join` with `ErrPubSubUnavailable` rather than
silently registering a connection that will never receive a broadcast.

## Groups and presence

**Topics are subscriptions. Groups are identities.** They are deliberately different
things:

| | Topic | Group |
|---|---|---|
| Means | "this connection is interested in `room:42`" | "this connection *is* `user:123`" |
| Cardinality | a connection joins many | a connection has exactly one |
| Set with | `WithConnTopics("room:42", ...)` | `WithConnGroup("user:123")` |
| Fan out with | `Broadcast(ctx, topic, payload)` | `SendToGroup(ctx, group, payload)` |
| Answers "is X online?" | no | yes, via `IsOnline` |

A group is how you address the human rather than the socket: one identity typically has
several live connections (a phone, a laptop, three browser tabs), and `SendToGroup` fans
out to all of them, everywhere in the cluster.

```go
registry := gateway.NewRegistry(system, logger,
    gateway.WithPresence(presence),          // cluster-wide; omit and you only know about this node
    gateway.WithPresenceTTL(30*time.Second), // default; Registry refreshes in the background at ttl/3
)
defer registry.Close(ctx) // stops the presence refresh goroutine

res, err := registry.SendToGroup(ctx, "user:123", payload)
if err != nil {
    return err
}
if res.None() {
    // Nobody is holding a socket for this identity anywhere in the cluster.
    // This is the moment to fall back to an offline channel.
    return webpush.Send(ctx, "user:123", payload)
}
```

### Presence

`Presence` is the cluster-wide answer to "which connection ids does this identity
currently have, and where?". It is an interface with a lease-based (TTL'd) model, and it
is optional:

- **No `Presence`** (the default): `IsOnline` and `LocalConnectionsOf` only see *this
  node's* connections. `SendToGroup` still reaches the whole cluster (the group bridge is
  a topic under the hood), but `DeliveryResult.Remote` can only report "one cluster fan-out
  happened", not how many remote members received it - so `None()` is not a reliable
  offline signal.
- **`NewMemoryPresence()`**: process-local. Correct for single-node deployments and tests.
- **`presence/redis.NewPresence(client)`**: backed by Redis or Valkey, shared by every
  node. This is the configuration in which `IsOnline` and `DeliveryResult.None()` become
  cluster-aware rather than single-node. It is a separate package so importing the root
  module never pulls in `github.com/redis/go-redis/v9` for applications that do not want it.
  See [Redis / Valkey backends](#redis--valkey-backends) for how it shares one instance with
  the other three backends. Note that presence membership is a lease-based estimate: a node
  that has just crashed keeps its members visible for up to one TTL, so with presence alone
  `None()` can miss a genuinely offline identity for that window. Add
  `WithDeliveryConfirmation` when you need `None()` to be exact - it counts acknowledged
  remote writes instead of trusting the lease, at the cost of making the offline fallback
  at-least-once (a slow node that acks late may get both an in-app and an offline copy).

```go
import (
	"github.com/redis/go-redis/v9"

	gateway "github.com/StringKe/goakt-gateway"
	gatewaypresence "github.com/StringKe/goakt-gateway/presence/redis"
)

client := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
registry := gateway.NewRegistry(system, logger,
    gateway.WithPresence(gatewaypresence.NewPresence(client)),
)
```

The Redis implementation models one group as one ZSET whose members are connection ids
scored by absolute lease expiry, so a single Lua script both sweeps expired members and
returns a consistent snapshot, all against a single key (and therefore a single Redis
Cluster slot). The trade-offs are spelled out in that package's doc comment.

Because leases expire, presence is eventually consistent: a node that dies without
unregistering leaves its connection ids visible for up to one TTL. `IsOnline` returning
true therefore means "someone recently held a socket for this identity", not "a socket is
guaranteed writable right now". Treat it as a routing hint; treat `DeliveryResult` as the
truth.

## Backpressure

Every registered connection has a bounded outbound buffer (`WithWSSendBuffer` /
`WithSSESendBuffer`, default 256). A slow consumer that stops reading fills it. The
library refuses to let that block the producer - a broadcast to 10,000 connections must
not stall on the one client with a stalled TCP window - so a full buffer is a decision
point, and `BackpressurePolicy` is how you make it:

| Policy | Behavior | Use when |
|---|---|---|
| `BackpressureDrop` (default) | Drop the message. `SendToConnection` returns `ErrBackpressure`; the message is counted in `DeliveryResult.Dropped` and reported to `Observer.DeliveryDropped`. The connection stays up. | Messages are snapshots/notifications, and losing one is better than losing the client. |
| `BackpressureClose` | Close the connection. The client is expected to reconnect and resynchronize. | The stream is an ordered log where a hole is corruption, and a client that cannot keep up is better off starting over. |

```go
gateway.NewWSHandler(registry,
    gateway.WithWSSendBuffer(1024),
    gateway.WithWSBackpressurePolicy(gateway.BackpressureClose),
)
```

There is no third "block until there is room" option, and there will not be: it converts
one slow client into a stalled fan-out for everyone.

Inbound pressure is separate and off by default: `WithWSReadLimit` (default 1 MiB) caps a
single message, and `WithWSInboundRate(perSecond, burst)` rate-limits a connection's
inbound message rate (`ErrRateLimited`).

## Observability

`Observer` is an optional hook set. Every method may be called from a connection's
goroutine, so implementations must be cheap and non-blocking (increment a counter, do not
write to a database). A nil `Observer` is the default and every call site checks for it.

```go
type Observer interface {
    ConnectionRegistered(id, group string)
    ConnectionUnregistered(id, group string)
    ConnectionReplaced(id, group string)      // a reconnect took over an existing id
    DeliveryDropped(id, group string)         // backpressure
    DeliveryFailed(id string, err error)
    BroadcastFanout(topic string, localMembers int)
}

registry := gateway.NewRegistry(system, logger, gateway.WithObserver(myMetrics))
```

The two worth alerting on: a rising `DeliveryDropped` rate means send buffers are too
small or consumers are too slow, and a rising `ConnectionReplaced` rate means clients are
reconnecting in a loop.

## WebSocket and SSE listeners

`NewWSHandler` and `NewSSEHandler` return ordinary `http.Handler` values:

```go
mux.Handle("/ws", gateway.NewWSHandler(registry,
    gateway.WithWSAuth(func(r *http.Request) (*gateway.ConnInfo, error) {
        claims, err := verifyBearerToken(r)
        if err != nil {
            return nil, err // rejects the handshake with 403
        }
        return &gateway.ConnInfo{
            ID:     claims.DeviceID,               // unique per socket
            Group:  "user:" + claims.UserID,       // the identity behind the socket
            Topics: []string{"room:" + roomOf(r)},
            Meta:   map[string]string{"tenant": claims.Tenant},
        }, nil
    }),
    gateway.WithWSOriginPatterns("app.example.com"),
    gateway.WithWSOnMessage(handleInbound),
))
mux.Handle("/events", gateway.NewSSEHandler(registry,
    gateway.WithSSEAuth(authenticate),
    gateway.WithSSEHistory(gateway.NewMemorySSEHistory(64)),
))
```

`WSAuthFunc`/`SSEAuthFunc` resolve the caller's identity exactly once, at handshake time,
and everything downstream (registration, `OnConnect`, `OnMessage`, `OnDisconnect`) is handed
the resulting `*ConnInfo`. `WithWSIDFunc`/`WithWSTopics` remain only as fallbacks for the
fields auth left empty; an empty `ConnInfo.ID` gets a generated UUID.

| Concern | How it's covered |
|---|---|
| Auth | `WithWSAuth`/`WithSSEAuth` run during the handshake/request; a non-nil error rejects the connection. |
| Origin | Same-host only by default (`coder/websocket`'s default). Widen with `WithWSOriginPatterns`, or disable with `WithWSInsecureSkipOriginCheck` - which is exactly as dangerous as it sounds, because a browser will then happily let any site open an authenticated socket to you with the user's cookies. |
| Liveness | `WithWSPingInterval` (default 30s) plus `WithWSPongTimeout` (default 10s) detect a half-open TCP connection that would otherwise linger for minutes. |
| Reconnect takeover | A reconnect with an already-registered id takes over: the old registration is replaced and its socket closed. This is the default precisely because of half-open TCP - the alternative locks a user out of their own account until the dead socket times out. Because takeover is on by default, the connection id is a security boundary: derive it from an authenticated principal (in `WithWSAuth`/`WithSSEAuth`), never from an unauthenticated request field, or a client can kick and impersonate any other connection by supplying its id. |
| Backpressure | See [Backpressure](#backpressure). |
| Clean shutdown | The connection is always unregistered (and its ephemeral actor stopped) when the socket closes. |
| Grouping | `WithConnGroup` for identity, `WithConnTopics` for subscriptions. Both require `actor.WithPubSub()`. |

### SSE resume

SSE is one-way (server to client); pair it with an ordinary HTTP endpoint for inbound
data. In exchange it gets the browser's built-in reconnect, and `WithSSEHistory` makes
that reconnect lossless over the window that matters:

```go
mux.Handle("/events", gateway.NewSSEHandler(registry,
    gateway.WithSSEHistory(gateway.NewMemorySSEHistory(64)), // retain 64 events per connection
    gateway.WithSSERetry(3*time.Second),                     // "retry:" hint to the browser
))
```

On reconnect the browser sends `Last-Event-ID`; the handler replays everything after it.
If that id is no longer retained, the client is told so explicitly with a
`gateway-gap` event (`SSEGapEventName`) rather than silently resuming from a hole, so the
application can resynchronize from its own source of truth.

Two limits worth internalizing. `MemorySSEHistory` is per-process, so replay only works if
the client reconnects to the *same node*. `ssehistory/redis.New(client)` removes that
limit: it records every connection's buffer in a shared Redis or Valkey backend, so a
client whose EventSource reconnects to *any* node in the deployment is replayed correctly
instead of being told the history is gone. It is wired the same way -
`WithSSEHistory(ssehistory/redis.New(client))` - and is one of the four interchangeable
[Redis / Valkey backends](#redis--valkey-backends). And history records what was written
toward a *registered* connection: it covers the real, often-long window between a socket
dying and the server noticing, not an arbitrary offline period. For a genuinely offline
client, `SendToGroup` returns `None()` and the offline channel is the right answer.

## Cluster-shared TLS

`Manager` lets every process terminate TLS for any hosted domain from one certificate,
with issuance arbitrated across every process that shares its `Coordinator` so that, as
long as your `CertIssuer` completes within the configured issuance lock TTL, at most one
process calls it:

```go
manager := gateway.NewManager(actorSystem, logger,
    gateway.WithCertIssuer(issuer),
    gateway.WithCoordinator(coordinator),      // optional; defaults to an in-memory, process-local one
    gateway.WithCertStore(myPersistentStore),  // optional; defaults to in-memory
    gateway.WithAllowedDomains("chat.example.com", "*.tenants.example.com"),
    gateway.WithRenewBefore(30*24*time.Hour),  // default
)

if err := manager.Start(ctx); err != nil { // registers the renewal schedule
    log.Fatal(err)
}
defer manager.Stop(ctx)

tlsConfig := manager.TLSConfig() // GetCertificate wired for SNI lookup
```

- **Issuance arbitration.** The first request for a domain (or the renewal schedule
  finding one close to expiry) races `Coordinator.TryLock` for that domain; only the
  winner calls the configured `CertIssuer`. Losers poll the `Coordinator` until the
  winner publishes, or give up with `ErrIssuanceTimeout`.
- **Issuance lock TTL caveat.** The lock is not renewed while the `CertIssuer` call is in
  flight (`Coordinator` has no lock-extension method). If issuance takes longer than
  `WithIssuanceLockTTL` (default 2 minutes), the lock can expire mid-issuance and a second
  process can acquire it and also call the issuer. `Manager` detects this after the fact
  and returns `ErrIssuanceLockExpired` instead of caching the result, but it does not
  prevent the extra `CertIssuer` call. Set `WithIssuanceLockTTL` comfortably longer than
  your `CertIssuer`'s worst-case latency.
- **Distribution.** The issued certificate is written to the `Coordinator` (so every
  process sharing it can serve it immediately) and to the configured `CertStore` (so a
  single-process restart doesn't need the `Coordinator` at all).
- **Renewal.** Driven by GoAkt's cron scheduler (`ActorSystem.ScheduleWithCron`, default:
  hourly, configurable via `WithRenewInterval`). In a GoAkt cluster, cron schedules fire
  once cluster-wide by scheduler design, so exactly one node re-checks/re-issues per
  tick regardless of cluster size.
- **SNI lookup.** `Manager.GetCertificate` implements the `tls.Config.GetCertificate`
  signature directly.

### Domain admission

An SNI-driven issuer facing the open internet is a way to burn a CA rate limit: anything
that can open a TCP connection can name any domain. Admission is therefore explicit, and
**deny-by-default**: a `Manager` configured with `WithCertIssuer` but neither
`WithAllowedDomains` nor `WithDomainPolicy` refuses every domain with
`ErrDomainNotAllowed` rather than issuing for whatever SNI value shows up. You must
declare admitted domains through one (or both) of:

- `WithAllowedDomains("chat.example.com", "*.tenants.example.com")` - a static list.
  Wildcards match a single label (`*.example.com` matches `a.example.com`, not
  `a.b.example.com`).
- `WithDomainPolicy(func(ctx, domain) (bool, error))` - a dynamic check, for the
  custom-domain case where the answer lives in a database.
- `WithNegativeCacheTTL` (default 1 minute) caches rejections and issuance failures, so a
  flood of junk SNI cannot turn into a flood of issuer calls.
- `WithMaxCachedCerts` (default 1024) bounds the in-memory certificate cache (LRU), so a
  flood of *valid* SNI cannot exhaust memory either.

This deny-by-default only applies once a `CertIssuer` is configured. Without one, `Manager`
can only ever serve a certificate a `CertStore` already has or a `WithFallbackCertificate`
provides, so admitting every domain costs nothing and remains the default.

### Fallback certificates and hot reload

Not every deployment issues its own certificates. The common shape - Cloudflare terminating
TLS at the edge, the origin serving one long-lived catch-all certificate - needs no issuer
at all, just a certificate that is reloaded when it rotates on disk:

```go
reloading, err := gateway.NewReloadingCertificate("/tls/tls.crt", "/tls/tls.key", time.Minute, logger)
if err != nil {
    log.Fatal(err)
}
reloading.Start(ctx)
defer reloading.Stop()

manager := gateway.NewManager(actorSystem, logger,
    gateway.WithFallbackCertificate(reloading.Get),
)
```

`WithFallbackCertificate` serves a certificate when the client sent no SNI, or when the
domain has no certificate and no issuer is configured. (A domain that is explicitly *not
allowed* is rejected, never served the fallback.)

`ReloadingCertificate` polls a SHA-256 hash of the file contents, never the mtime, and
re-parses with `tls.X509KeyPair` only when the bytes change - this is what makes it survive a
Kubernetes Secret/ConfigMap rotation, which swaps a `..data` symlink underneath the path
instead of rewriting the file, so the mtime a naive watcher would stat never moves. If a
reload fails (a half-written pair, a bad key), the last known-good certificate is kept and the
error is logged; a rotation typo does not take TLS down.

`NewFileCertStore(dir)` is the matching persistent `CertStore`: `dir/<domain>.crt` and
`dir/<domain>.key`, each written atomically, key mode 0600. `Get` re-reads on a cert/key
mismatch, so a handshake racing a renewal never sees the new certificate paired with the old
key across the two renames - the pair swap is all-or-nothing, the same as `MemoryCertStore`
and `store/redis`. `store/redis.New(client)` is the
shared alternative - one Redis or Valkey key per domain, so a cold-started node reads
already-issued certificates back without depending on the issuer, and every node sees the
same store. See [Redis / Valkey backends](#redis--valkey-backends).

### Coordinator

GoAkt's cluster KV store is not part of the public API this library depends on, and its
underlying storage documents its own lock as "recommended for efficiency, not
correctness" - the wrong substrate for an issuance lock, since a duplicated issuance
burns CA rate limit. `Coordinator` is a small interface this library owns instead:

```go
type Coordinator interface {
    Get(ctx context.Context, key string) (value []byte, ok bool, err error)
    Put(ctx context.Context, key string, value []byte, ttl time.Duration) error
    TryLock(ctx context.Context, key string, ttl time.Duration) (unlock func(context.Context) error, err error)
}
```

Two implementations ship:

- **`NewMemoryCoordinator`** (the default) is process-local - correct for a single
  process, local development, and tests, but does not coordinate issuance across
  processes.
- **`coordinator/redis.New`** is backed by Redis or Valkey (`SET NX PX` for the lock, a Lua
  compare-and-delete for release - a real mutual exclusion, not a best-effort one) and
  coordinates issuance across every process pointed at the same instance. It is a
  separate package specifically so importing the root module never pulls in
  `github.com/redis/go-redis/v9` for applications that do not want it:

```go
import (
	"github.com/redis/go-redis/v9"

	gateway "github.com/StringKe/goakt-gateway"
	gatewayredis "github.com/StringKe/goakt-gateway/coordinator/redis"
)

client := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
manager := gateway.NewManager(actorSystem, logger,
	gateway.WithCoordinator(gatewayredis.New(client)),
	gateway.WithCertIssuer(issuer),
	gateway.WithAllowedDomains("chat.example.com"), // required: see Domain admission
)
```

Bring your own implementation (backed by etcd, Consul, a database - anything that can do
a real TTL'd mutex) by implementing the three-method interface directly.

### Certificate issuers

```go
type CertIssuer interface {
    Issue(ctx context.Context, domain string) (*Certificate, error)
}
```

- **`StaticIssuer`** serves a fixed, pre-provisioned certificate.
- **`CloudflareOriginCAIssuer`** calls the
  [Cloudflare Origin CA API](https://developers.cloudflare.com/ssl/origin-configuration/origin-ca/)
  over plain `net/http` - no Cloudflare SDK dependency:

```go
issuer := &gateway.CloudflareOriginCAIssuer{
    APIToken:           os.Getenv("CF_ORIGIN_CA_KEY"),
    RequestCertificate: gateway.NewRSACertificateRequest(2048),
}
```

## Authenticated Origin Pulls

If traffic only ever reaches your gateway through Cloudflare, `AuthenticatedOriginPulls`
adds mTLS verification that inbound connections present Cloudflare's origin-pull client
certificate. `CAPEM` is supplied by the caller rather than embedded, so deployments track
Cloudflare's currently published CA bundle instead of trusting a copy vendored into this
library:

```go
pulls := &gateway.AuthenticatedOriginPulls{CAPEM: cloudflareOriginPullCA}

server, err := gateway.NewServer(":8443", mux,
    gateway.WithTLSManager(manager),
    gateway.WithAuthenticatedOriginPulls(pulls),
)
```

## Server and graceful shutdown

`Server` is an optional, thin wrapper around `*http.Server`. `http.Server.Shutdown`
cannot evict long-lived connections on its own - hijacked WebSocket sockets are ignored
entirely, and open SSE streams are waited on until the shutdown context expires -
so register handlers with `WithDrainOnShutdown`:

```go
wsHandler := gateway.NewWSHandler(registry)
sseHandler := gateway.NewSSEHandler(registry)

server, err := gateway.NewServer(":8443", mux,
    gateway.WithTLSManager(manager),
    gateway.WithDrainOnShutdown(wsHandler, sseHandler),
    gateway.WithServerErrorLog(stdlog.New(io.Discard, "", 0)), // optional: silence TLS probe noise
)
```

`Drain` sends a WebSocket `1001 GoingAway` close frame (SSE streams are simply closed), the
per-connection actors unregister, and reconnecting clients land on the replicas that stay
up. `WithServerErrorLog` exists because a TLS listener logs every failed handshake, and
load balancer readiness probes that connect without completing one otherwise produce a
steady drip of `http: TLS handshake error ...: EOF` on stderr.

## Redis / Valkey backends

Four of this library's pluggable abstractions ship a shared backend built on
[`github.com/redis/go-redis/v9`](https://github.com/redis/go-redis). The RESP protocol is
the abstraction: all four constructors share one signature - each takes a
`redis.UniversalClient` (which covers standalone, Cluster, Sentinel, and Ring) - and
go-redis speaks the identical protocol to a **Redis** server and to a **Valkey** server,
so one client, constructed once, backs any or all of them against either with no branch. Point the client wherever your deployment prefers - Valkey
is BSD-licensed, Redis is RSALv2/SSPL - and pick by license, not by capability. Every
backend uses only commands present and identical on Redis 7.2 and Valkey 8; none touches a
Redis-proprietary module or a post-7.2 command Valkey has not adopted.

| Abstraction | In-memory default | Shared backend | Import path | Key namespace |
|---|---|---|---|---|
| `Coordinator` (issuance lock + certificate distribution) | `NewMemoryCoordinator()` | `coordinator/redis.New(client)` | `github.com/StringKe/goakt-gateway/coordinator/redis` | `WithKeyPrefix` |
| `Presence` (cluster-wide online status) | `NewMemoryPresence()` | `presence/redis.NewPresence(client)` | `github.com/StringKe/goakt-gateway/presence/redis` | `WithKeyPrefix` |
| `CertStore` (certificate persistence) | `NewMemoryCertStore()` / `NewFileCertStore(dir)` | `store/redis.New(client)` | `github.com/StringKe/goakt-gateway/store/redis` | `WithKeyPrefix` (keys carry a `cert:` infix) |
| `SSEHistory` (Last-Event-ID replay buffer) | `NewMemorySSEHistory(perConn)` | `ssehistory/redis.New(client)` | `github.com/StringKe/goakt-gateway/ssehistory/redis` | `WithKeyPrefix` (default `gateway:ssehistory:`) |

One Redis or Valkey instance can carry all four at once: give each a distinct
`WithKeyPrefix` and their keys never collide, so a single server (or Cluster) backs a whole
gateway deployment's coordination, presence, certificate, and replay state.

```go
import (
	"github.com/redis/go-redis/v9"

	gateway "github.com/StringKe/goakt-gateway"
	rediscoordinator "github.com/StringKe/goakt-gateway/coordinator/redis"
	redispresence "github.com/StringKe/goakt-gateway/presence/redis"
	redisstore "github.com/StringKe/goakt-gateway/store/redis"
	redishistory "github.com/StringKe/goakt-gateway/ssehistory/redis"
)

// One client, pointed at either a Redis or a Valkey server.
client := redis.NewUniversalClient(&redis.UniversalOptions{Addrs: []string{"localhost:6379"}})

coordinator := rediscoordinator.New(client, rediscoordinator.WithKeyPrefix("gw:coord:"))
presence := redispresence.NewPresence(client, redispresence.WithKeyPrefix("gw:presence:"))
certStore := redisstore.New(client, redisstore.WithKeyPrefix("gw:certs:"))
history := redishistory.New(client, redishistory.WithKeyPrefix("gw:sse:"))
```

All four backends are validated by conformance suites (`coordinator/conformance`,
`presence/conformance`, `store/conformance`, `ssehistory/conformance`) that run the same
assertions against both the in-memory implementation and the RESP one, and the RESP suites
are run against a real Redis server and a real Valkey server (a two-service
`docker-compose.yml` locally; see [Development](#development)). Interchangeability is a
tested property, not a claim.

## Examples

| Example | Demonstrates |
|---|---|
| [`echo`](./examples/echo) | The smallest useful thing: a WebSocket echo endpoint plus an HTTP handler that pushes into a live connection. |
| [`chat`](./examples/chat) | Topic broadcast: room-based chat, fan-out to every member except the sender (`WithExclude`). |
| [`cluster`](./examples/cluster) | The whole point of the `Registry`: three nodes, and a message handed to node A arrives on a socket held by node C. |
| [`notification`](./examples/notification) | Groups, presence, and the offline fallback: `SendToGroup` to a multi-device identity, with `DeliveryResult.None()` driving a web-push retreat. `presence/redis` when `REDIS_ADDR` is set. |
| [`sse-resume`](./examples/sse-resume) | SSE `Last-Event-ID` replay across a reconnect, including the `gateway-gap` event when history has aged out. Set `REDIS_ADDR` to run two nodes over one shared `ssehistory/redis` and reconnect across them. |
| [`graphql-ws`](./examples/graphql-ws) | That `Registry` does not need `WSHandler`: a hand-rolled `graphql-transport-ws` server registering its own sockets. |
| [`tls-cloudflare`](./examples/tls-cloudflare) | Cluster-shared TLS end to end: Origin CA issuance plus Authenticated Origin Pulls mTLS. `coordinator/redis` + `store/redis` when `REDIS_ADDR` is set, including a cold start served from the store with no issuer. |
| [`offline-push`](./examples/offline-push) | `WithOfflineChannel`: `SendToGroup` to an identity with no live socket drives `gateway.OfflineChannel` automatically, with `offline/webpush` against a fake push service and `OfflineObserver.OfflineFallback` reporting the result. |
| [`delivery-confirm`](./examples/delivery-confirm) | `WithDeliveryConfirmation`: a two-node cluster where `DeliveryResult.Remote` counts a confirmed remote socket write (`Ask`) rather than a fire-and-forget hand-off, including the `ErrConfirmationTimeout` path. |
| [`persistence`](./examples/persistence) | Opt-in at-least-once: `WithOutbox` + `Registry.Ack` persist, deliver, wait for the client ack, and redeliver unacked messages on reconnect. `persistence/redis` when `REDIS_ADDR` is set. |
| [`presence-watch`](./examples/presence-watch) | `WatchPresence` and `GroupMembers`: a live join/leave event stream plus cluster-wide member enumeration with metadata. `presence/redis` (Pub/Sub watch, directory) when `REDIS_ADDR` is set. |
| [`reauth-kick`](./examples/reauth-kick) | `WithWSReauth` periodic re-authorization plus `Registry.Disconnect`/`DisconnectGroup` forced eviction, both ending the socket with a 1008 close and reason. |

A note that costs people an afternoon otherwise, documented at length in
[`examples/cluster`](./examples/cluster): GoAkt derives its memberlist gossip label from the
actor system's *name*, so nodes whose actor systems are named differently silently refuse
to cluster with each other, and every cross-node lookup then fails with a bare
`ErrConnectionNotFound`. **Every node in a cluster must use the same actor system name.**

## Opt-in reliability and lifecycle features

Everything below is off by default. The default `Registry` and handlers behave exactly as
the rest of this README describes: a single fire-and-forget socket write, no persistence, no
extra round-trip, no background goroutine you did not ask for. Each feature is a constructor
option you add only when you need it, and each one names its own cost in the sentence that
turns it on. A deployment that wires none of them pays for none of them.

### Offline fallback

`WithOfflineChannel(ch)` closes the gap between "the identity is not connected here" and
"do something about it". When `SendToGroup` finds no live socket for the target anywhere in
the cluster (`DeliveryResult.None()` is true), the `Registry` calls `ch.Deliver(ctx, group,
payload)` for you instead of leaving that `if result.None()` check to every call site.

```go
registry := gateway.NewRegistry(system, logger,
    gateway.WithPresence(presence),                 // None() is only trustworthy with a Presence backend
    gateway.WithOfflineChannel(myWebPush),          // gateway.OfflineChannel
)
```

The fallback runs off the main delivery path: `SendToGroup` returns its `DeliveryResult` as
before, and `Deliver`'s error is reported through the optional `OfflineObserver.OfflineFallback`
hook and the logger, not returned to the caller. `offline/webpush` is a ready
`OfflineChannel` backed by VAPID Web Push; bring any `Deliver(ctx, group, payload)` for email,
APNs, or a store-and-forward queue. Boundary: the fallback fires exactly when `None()` is
true, so it inherits `None()`'s accuracy. Without a `Presence` backend `None()` just means
"not on this node" and never fires in a cluster; with presence alone a stale lease can suppress
it for up to one TTL; only `WithDeliveryConfirmation` makes it exact, and then it is
at-least-once - a node that acknowledges a write late can still trigger the fallback, so an
identity may receive both an in-app and an offline copy. That is the deliberate trade: never
lose a notification, at the cost of a possible duplicate.

### Delivery confirmation

By default a cross-node delivery is `actor.Tell`: `DeliveryResult.Remote` counts what was
*handed to the cluster*, not what reached a socket. `WithDeliveryConfirmation()` switches the
remote hand-off to `Ask`, so the originating node waits for the remote `connActor` to report
that the socket write succeeded, and `Remote` then counts confirmed writes into a remote
outbound buffer.

```go
registry := gateway.NewRegistry(system, logger,
    gateway.WithDeliveryConfirmation(),
    gateway.WithConfirmationTimeout(3*time.Second),  // default 5s
)
```

Cost and boundary: this adds a network round-trip per remote target and a per-call timeout.
A target that does not confirm within `WithConfirmationTimeout` (default 5s) is not counted
in `Remote`, and the timed-out branch surfaces `ErrConfirmationTimeout` to the delivery's
error path. This is what lets `SendToGroup`'s `None()` be exact under presence - a crashed
node's stale lease no longer counts, because only an acknowledgement counts - but it also
means a slow node that buffers the payload and acks after the timeout is treated as unreached,
so a paired `WithOfflineChannel` is at-least-once and may duplicate. Confirmation still ends at
the remote *buffer*, not at the client: it proves the socket accepted the bytes, not that the
client read them. The default path is untouched -
`connActor` answers the confirmation unconditionally, and under `Tell` that answer is a
no-op, so nodes that never opt in do zero extra work.

### Presence watch and directory

Two questions the point-in-time `IsOnline`/`GroupMembers` pair cannot answer well: "tell me
the moment a member of this group joins or leaves" and "enumerate every connection for this
group across the whole cluster, with metadata". Both are optional capabilities a `Presence`
backend may implement.

```go
events, cancel, err := registry.WatchPresence(ctx, "user:123")   // PresenceWatcher
defer cancel()
for ev := range events {
    // ev.Kind is PresenceJoin or PresenceLeave
}

entries, err := registry.GroupMembers(ctx, "user:123")           // PresenceDirectory
```

`MemoryPresence` implements both, as does `presence/redis`. `WatchPresence` returns
`ErrPresenceWatchUnsupported` if the configured backend does not implement `PresenceWatcher`;
`GroupMembers` falls back to this node's local group index when the backend does not
implement `PresenceDirectory`. Boundary: `presence/redis` watch is Redis Pub/Sub and is
therefore best-effort - events published while a watcher is disconnected are not replayed,
and a full watcher channel drops rather than blocks. Treat it as a liveness hint that lets
you skip polling, not a durable event log. To carry per-connection metadata into
`GroupMembers`, register with `WithConnMeta`; the `Registry` forwards it to the backend's
`JoinWithMeta` when the backend supports it, and a bare `Refresh` never re-emits a join or
loses the metadata.

### Persistence and at-least-once delivery

The default socket write is at-most-once: a message to a connection that is not registered
anywhere is gone. `WithOutbox(o)` turns `SendToConnection` into store-then-deliver, and
`Registry.Ack` plus reconnect redelivery close the loop into at-least-once.

```go
registry := gateway.NewRegistry(system, logger,
    gateway.WithOutbox(gateway.NewMemoryOutbox()),  // or persistence/redis for a cluster
)

// application, on receiving the client's ack frame:
_ = registry.Ack(ctx, connID, msgID)
```

With an `Outbox` wired, `SendToConnection` first `Append`s the payload - the **`Outbox`
itself** mints the UUID `msgID` and assigns the per-connection monotonic `Seq`, returning both
- and then delivers it; a fresh registration for that connection id automatically redelivers
everything still unacked. The application calls `Registry.Ack` once the client has
acknowledged, and the stored copy is dropped. `NewMemoryOutbox()` is process-local (single
node, tests); `persistence/redis` survives process restart and works across nodes. The `Seq`
is assigned by the store, not by the sending process, precisely so it stays monotonic across a
restart and across nodes appending to the same connection: a per-process counter would restart
at 1 on reboot and collide with a still-stored message, so two payloads could land on the same
`Seq` and a `Seq`-deduping client would silently drop one. The counter is reclaimed only by
`Outbox.DropConn` (or a `persistence/redis` `WithTTL`), which is also what keeps the store
from accumulating one entry per connection id ever seen. Boundary: at-least-once means
**duplicates are possible** - an unacked message is redelivered on reconnect even if the
client did in fact receive it, so clients must dedupe on the `ID`/`Seq` they read back through
`Outbox.Unacked`. Boundary: `SendToConnection` does not itself put `msgID`/`Seq` on the wire -
it writes the raw `payload` `send` was given, same as without an `Outbox` - so an application
wiring this in today must communicate the `msgID` to the client through its own framing (or
have the client ack by an id derived from the payload it defines) for the very first delivery
attempt; only a reconnect's `Outbox.Unacked` exposes `msgID`/`Seq` directly to the
application. `EncodeOutboxEnvelope`/`DecodeOutboxEnvelope` in `persistence.go` are a ready,
documented wire frame for closing this gap, but no handler wires them yet.
`Registry.Ack` is a no-op returning nil when no `Outbox` is configured, so the call site is
safe to keep unconditionally. The bare `send` primitive is unchanged: the library does not
wrap the payload or impose a wire format, so your ack framing is yours to define.

### Reauth and disconnect

A handshake authenticates the caller once; authorization then drifts under a long-lived
socket. `WithWSReauth`/`WithSSEReauth` re-run your auth function on an interval and tear the
connection down the instant it fails, and `Registry.Disconnect`/`DisconnectGroup` let you
evict a connection or an entire identity on demand (a ban, a permission revocation, a forced
re-login).

```go
gateway.NewWSHandler(registry,
    gateway.WithWSAuth(authFn),
    gateway.WithWSReauth(30*time.Second, authFn),   // fails -> 1008 close + Unregister
)

n, err := registry.DisconnectGroup(ctx, "user:123", "session revoked")
```

The mechanism is a close hook registered at `Register` time (`WithConnCloseHook`), so there
is no window between registration and the connection becoming closable, and the hook is
race-free against a normal `Unregister`. A WS connection closed this way receives a
`StatusPolicyViolation` (1008) frame with the reason (clamped to the 123-byte close-frame
limit on a UTF-8 boundary, so an over-long reason still produces a clean 1008 rather than an
abrupt 1006); an SSE stream is ended with a terminating comment. `Disconnect` drives the close
only; the socket's own teardown runs the
existing unregister path, so takeover and reserve/rollback correctness is unchanged.
Boundary: WS reauth re-runs the auth function against the original handshake request, so it
can only re-validate header/cookie-style credentials (the hijacked body is gone), and it
checks liveness of the credential, not a re-parsed identity applied to the live socket.

### Compression and rate limiting

`WithWSCompression(mode)` enables `permessage-deflate` on the WebSocket (coder/websocket's
`CompressionMode`); the default is `CompressionDisabled`, its zero value, so the wire is
untouched unless you ask. `WithWSGroupRate(perSecond, burst)` adds a per-group inbound token
bucket shared by every local connection of that group, composing with the per-connection
`WithWSInboundRate` (a frame must pass both):

```go
gateway.NewWSHandler(registry,
    gateway.WithWSCompression(websocket.CompressionContextTakeover),
    gateway.WithWSInboundRate(20, 40),      // per connection
    gateway.WithWSGroupRate(100, 200),      // per group, shared across its local sockets
)
```

Boundary: the group bucket is per node (it throttles the connections a single node holds for
that group, not the group cluster-wide), and connections with an empty group are never
group-limited. Exceeding either limit closes the offending connection with the configured
backpressure policy.

### Strict multi-instance correctness

Everything above - including reconnect takeover - relies on GoAkt's actor directory to know
who currently owns a connection id. That directory is PA/EC eventually consistent
(memberlist/olric last-write-wins), not an atomic cluster lock: two nodes racing `Register`
for the same id can both observe "no owner" and both succeed, producing two live owners for
one id (split brain). Sequential takeover - one node evicting an owner it can already see -
works without anything below and is unaffected by it; what needs `WithOwnerLease` is a true
race between two `Register` calls that never see each other's write in time.

```go
coordinator := rediscoordinator.New(client)  // must implement gateway.CASCoordinator;
                                              // MemoryCoordinator and coordinator/redis both do
registry := gateway.NewRegistry(system, logger,
    gateway.WithOwnerLease(coordinator),
)
```

`WithOwnerLease(c)` makes every `Register` acquire a CAS-arbitrated lease for the connection
id from `c` before publishing it locally. The lease value carries the owning node's id, a
monotonically increasing **generation**, and an expiry; a takeover
(`WithReplaceExisting`) can only succeed by winning a compare-and-swap that bumps the
generation, so of two nodes racing `Register` for the same id, exactly one wins the CAS and
the other fails fast with `ErrOwnerHeld` - never both, and the loser never publishes a local
entry to race against. Once a node holds a generation, every fencing-aware operation carries
it and is rejected with `ErrStaleOwner` the instant a newer generation exists elsewhere:

- **Local and cross-node delivery** - `SendToConnection`, `SendToGroup` and `Broadcast` fence
  every local write (not only the cross-node `connActor` path) against the target entry's
  generation before it ever reaches the socket, so a superseded local entry this node has not
  yet evicted cannot keep delivering on the old owner's behalf. A remote `connActor` re-checks
  the generation of whichever entry currently answers to the id, so a same-node reconnect that
  has since replaced it is still served correctly rather than rejected as stale.
- **Background lease renewal** - the old owner's own refresh loop observes the takeover and
  self-evicts the local connection (closing it with `ownerLeaseStaleEvictReason`), with no
  message from the new owner required.
- **Presence `Refresh`/`Leave`** (`PresenceFencer`, implemented by `MemoryPresence` and
  `presence/redis`) - a stale owner's delayed refresh cannot keep its own superseded membership
  alive, and its delayed teardown cannot delete a takeover's already-(re)established membership.
- **SSE history** (`GenerationalHistory`, implemented by `MemorySSEHistory` and
  `ssehistory/redis`) - an event append from a superseded generation is rejected rather than
  recorded, and a takeover's `SSEHandler.open` advances the history's generation floor itself
  the moment the new owner registers, so a still-draining previous owner's queued write cannot
  land in shared history merely because the new owner has not appended anything of its own yet.
- **Outbox `Ack`** (`OutboxGenerationAdvancer`, implemented by `persistence/redis` and
  `NewMemoryOutbox`) - an ack from a superseded generation is rejected rather than applied, and
  `Register` raises the Ack fencing floor itself the moment a takeover is confirmed, so a stale
  owner's in-flight ack cannot slip in just because the new owner has not acked anything yet.
- **Entry-guarded unregister** (`Registry.RegisterHandle` / `ConnHandle.UnregisterHandle`) - a
  handler holding a stale handle cannot tear down the entry a takeover has since replaced.
- **Failed takeovers restore, not strand, the prior owner** - if the physical eviction behind a
  `WithReplaceExisting` takeover never completes (e.g. `ErrTakeoverTimeout`), the lease is
  restored to whichever owner it preempted instead of leaving the failed attempt's own claim (or
  a bare tombstone) permanently fencing out a connection that was never actually taken over.

Without `WithOwnerLease` (the default), a `Registry` has exactly the single-instance-safe,
zero-cost semantics it always had: no lease acquisition, no generation fencing, no extra
`Coordinator` round trip, and the split-brain window above is only as safe as the actor
directory's own consistency window - fine for sequential takeover, not for a genuine
concurrent race on the same id. Turning it on costs one CAS round trip per `Register` and a
background renewal goroutine per node; `WithOwnerLease` mirrors the
`WithDeliveryConfirmation` precedent of paying for a guarantee only when you ask for it.

## What this library does and does not guarantee

**Does:**

- A locally registered connection is always written to directly, with no actor mailbox
  or cluster round-trip on the hot path.
- `SendToConnection` for a connection registered anywhere in a GoAkt cluster resolves and
  delivers correctly, given the cluster's own consistency model for actor directory
  propagation (not instantaneous - see the multi-node tests for the propagation delay this
  library's own test suite budgets for).
- `Broadcast`'s `WithExclude` is honored on every node, not just the originating one: the
  exclusion list travels inside the cluster envelope.
- A reconnect on an existing connection id takes over even when the old registration
  lives on a different node: the takeover evicts the remote owner (closing its socket and
  freeing the cluster-unique actor name) and then claims the id, rather than the two
  fighting. Eviction is bounded - if the remote owner cannot be displaced within the
  takeover budget (directory propagation stall, partition) `Register` returns
  `ErrTakeoverTimeout` instead of hanging. This is a **sequential** takeover guarantee: one
  `Register` observes the existing owner and evicts it before claiming the id. It does not by
  itself resolve two nodes calling `Register` for the same id *at the same instant*, both
  observing no owner in the (eventually consistent) actor directory, and both succeeding -
  see "Strict multi-instance correctness" for the opt-in that closes that window.
- Certificate issuance is deduplicated within a process (`singleflight`) and, with a
  shared `Coordinator`, arbitrated across every process sharing it so at most one
  process calls the configured `CertIssuer` for a given domain at a time, **provided
  issuance completes within the configured issuance lock TTL** (see the lock TTL caveat
  below).
- `MemoryCoordinator.TryLock` and `coordinator/redis.Coordinator.TryLock` are both real
  mutual exclusion: an unlock can never release a lock a later caller has since
  acquired. This is a shared conformance suite (`coordinator/conformance`), run against
  both implementations, so a regression to e.g. a bare `DEL` in either would fail tests.
  `Presence`, `CertStore`, and `SSEHistory` have the same treatment
  (`presence/conformance`, `store/conformance`, `ssehistory/conformance`); the RESP
  backend of each is run against both a real Redis and a real Valkey server, so
  "interchangeable between Redis and Valkey" is a tested property.

**Does not:**

- Guarantee message delivery *by default*. `SendToConnection`/`SendToGroup`/`Broadcast` are
  best-effort out of the box, same as any actor `Tell`: no acknowledgement, no retry, no
  persistence. The opt-in escape is `WithOutbox` + `Registry.Ack` (at-least-once, with
  client-side dedupe on the redelivered `ID`/`Seq`); see "Persistence and at-least-once
  delivery". Nothing changes for callers who do not wire it.
- Guarantee that a fan-out actually reached a remote socket *by default*.
  `DeliveryResult.Remote` counts what was *handed to the cluster*, not what was written to a
  socket. Only `Delivered` (a local connection's outbound buffer accepted it) is first-hand
  knowledge, and even that is a buffer, not an ack from the client. A cross-node delivery
  that arrives at a node whose connection just died is silently lost. `WithDeliveryConfirmation`
  upgrades `Remote` to count confirmed remote socket writes at the cost of a round-trip and a
  timeout; it still ends at the remote buffer, not the client.
- Tell you the truth about cluster-wide presence without a `Presence` backend. Without one,
  `IsOnline` and `LocalConnectionsOf` see only this node, and `DeliveryResult.None()` is not
  a trustworthy "user is offline" signal - it only means "not here". Wire
  `presence/redis` before you build an offline-notification path on `None()`.
- Guarantee `IsOnline` is instantaneously correct even *with* a `Presence` backend. Leases
  are TTL'd; a node that dies without unregistering leaves its ids visible for up to one
  TTL, so `true` means "recently held a socket", not "writable right now".
- Guarantee ordering across `Broadcast` calls to different topics, or between a local
  direct write and a remote delivery of the same broadcast racing each other. Delivery into
  a single connection preserves the order in which that connection's buffer accepted the
  messages, and nothing more.
- Persist anything *by default*. A message sent to a connection that is not registered
  anywhere is gone; there is no queue, no inbox, no store-and-forward. `SSEHistory` replays
  what was already routed to a still-registered connection, and the opt-in `WithOutbox`
  stores unacked messages for redelivery on reconnect - but neither is on unless you wire it.
- Provide its own certificate validation, ACME support, or CA. `CertIssuer` is a seam;
  bring your own (an ACME implementation is a natural third one, deliberately not
  shipped here to keep the dependency tree minimal).
- Renew the issuance lock while a `CertIssuer` call is in flight. `Coordinator` has no
  lock-extension method, so if issuance takes longer than `WithIssuanceLockTTL`, the lock
  can expire mid-issuance and a second process can acquire it and also call the issuer;
  `Manager` returns `ErrIssuanceLockExpired` after the fact rather than caching the
  result, but does not prevent the extra call. Set the TTL comfortably longer than your
  `CertIssuer`'s worst-case latency.
- Re-check domain admission for an already-cached certificate. Once a domain is in the hot
  cache it is served until eviction or expiry without re-consulting `WithDomainPolicy`, so
  "revoke a custom domain within N seconds" is not a guarantee this library makes.
- Drain in-flight application state on shutdown beyond closing the socket/stream -
  `Drain` terminates connections, it does not flush or persist anything for you.

## Development

```
go build ./...
go vet ./...
go test -race ./...
```

The five RESP backends (`coordinator/redis`, `presence/redis`, `store/redis`,
`ssehistory/redis`, `persistence/redis`) have conformance tests that are skipped unless
`TEST_REDIS_ADDR` is set. Because interchangeability between Redis and Valkey is the whole
point, run the same suite against both. `docker-compose.yml` starts one of each:

```
docker compose up -d                                          # redis on :6399, valkey on :6400

TEST_REDIS_ADDR=127.0.0.1:6399 go test ./... -race -count=1   # against Redis
TEST_REDIS_ADDR=127.0.0.1:6400 go test ./... -race -count=1   # against Valkey

docker compose down
```

Both runs must show `coordinator/redis`, `presence/redis`, `store/redis`,
`ssehistory/redis`, and `persistence/redis` as `ok` (not `[no test files]` or skipped),
and `presence/redis`'s Watch/Directory tests run under the same gate. Point `TEST_REDIS_ADDR` at
any single instance to run just one backend, e.g.
`TEST_REDIS_ADDR=127.0.0.1:6399 go test ./store/redis/...`.

## See also

- [GoAkt](https://github.com/Tochemey/goakt) - the actor system this library sits on top of.
- [CHANGELOG.md](./CHANGELOG.md) - breaking changes and migration notes.
- [`examples/`](./examples) - seven runnable samples, indexed above.
