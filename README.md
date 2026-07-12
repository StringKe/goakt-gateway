# goakt-gateway

Cluster-aware ingress tier for [GoAkt](https://github.com/Tochemey/goakt): a WebSocket/SSE
connection registry with two-tier delivery, and cluster-shared TLS termination
(Cloudflare Origin CA, Authenticated Origin Pulls).

## What it is

`goakt-gateway` turns a GoAkt cluster into its own ingress tier - the role an
nginx/caddy front-tier normally plays in front of a stateless HTTP fleet - for two
things specifically: giving long-lived WebSocket/SSE connections an addressable identity
so any node can deliver a message to them, and terminating TLS for domains shared
cluster-wide from a certificate issued exactly once.

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

Requires Go 1.26 and `github.com/tochemey/goakt/v4` v4.3.1 or later.

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

	mux := http.NewServeMux()

	mux.Handle("/ws", gateway.NewWSHandler(registry,
		gateway.WithWSIDFunc(func(r *http.Request) string { return r.URL.Query().Get("id") }),
		gateway.WithWSOnMessage(func(ctx context.Context, id string, payload []byte) {
			// The connection is always local to this process here, so
			// SendToConnection takes the direct-write fast path: no actor, no
			// cluster lookup.
			_ = registry.SendToConnection(ctx, id, payload)
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

`Registry.SendToConnection` and `Registry.Broadcast` both follow the same shape:

1. **Local hit: write directly.** If the target connection (or, for broadcast, a topic
   member) is registered on this node, the payload is written straight to the socket.
2. **Local miss: cluster fallback.** Only when the connection is not held locally does
   `Registry` resolve it through the cluster-aware actor directory
   (`ActorSystem.ActorOf`) and deliver remotely. For `Broadcast`, remote topic members
   are reached through a small pub/sub bridge (see below) built on GoAkt's public
   `actor.Subscribe`/`actor.Unsubscribe` messaging.

Cross-node messaging - the only scenario that needs any cluster machinery at all - is
opt-in by construction: a single-node deployment never touches `ActorSystem.ActorOf` or
the topic actor.

### The pub/sub bridge

GoAkt's fork-only `ActorSystem.SubscribeTopic` convenience is not part of the public
API, so `Registry`'s topic fan-out is built from scratch on primitives that are: it
spawns its own small bridge actor per topic (`actor.ActorSystem.Spawn`), sends the
public `actor.Subscribe` message to `system.TopicActor()`, and forwards whatever the
topic actor delivers back to `Registry`'s local topic members. The bridge is torn down
with `actor.Unsubscribe` once the last local member leaves the topic. See `bridge.go`.

## WebSocket and SSE listeners

`NewWSHandler` and `NewSSEHandler` return ordinary `http.Handler` values:

```go
mux.Handle("/ws", gateway.NewWSHandler(registry,
    gateway.WithWSIDFunc(func(r *http.Request) string { return r.URL.Query().Get("user_id") }),
    gateway.WithWSAuthFunc(verifyBearerToken),
    gateway.WithWSTopics(func(r *http.Request) []string { return []string{roomOf(r)} }),
    gateway.WithWSOnMessage(handleInbound),
))
mux.Handle("/events", gateway.NewSSEHandler(registry,
    gateway.WithSSEIDFunc(func(r *http.Request) string { return r.URL.Query().Get("user_id") }),
))
```

| Concern | How it's covered |
|---|---|
| Auth | `WithWSAuthFunc`/`WithSSEAuthFunc` run during the handshake/request; a non-nil error rejects the connection (403). |
| Backpressure | Each connection gets a fixed-size outbound buffer (default 256). A full buffer returns `ErrBackpressure` from `SendToConnection`/`Broadcast` instead of blocking the caller. |
| Clean shutdown | The connection is always unregistered (and its ephemeral actor stopped) when the socket closes. |
| Topic grouping | `WithWSTopics`/`WithSSETopics` join the connection to `Registry` topics at registration time, for use with `Broadcast`. |

SSE is one-way (server to client); pair it with an ordinary HTTP endpoint for inbound
data.

## Cluster-shared TLS

`Manager` lets every process terminate TLS for any hosted domain from one certificate,
issued exactly once across every process that shares its `Coordinator`:

```go
manager := gateway.NewManager(actorSystem, logger,
    gateway.WithCertIssuer(issuer),
    gateway.WithCoordinator(coordinator),      // optional; defaults to an in-memory, process-local one
    gateway.WithCertStore(myPersistentStore),  // optional; defaults to in-memory
    gateway.WithAllowedDomains("chat.example.com", "api.example.com"),
    gateway.WithRenewBefore(30*24*time.Hour), // default
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
- **Distribution.** The issued certificate is written to the `Coordinator` (so every
  process sharing it can serve it immediately) and to the configured `CertStore` (so a
  single-process restart doesn't need the `Coordinator` at all).
- **Renewal.** Driven by GoAkt's cron scheduler (`ActorSystem.ScheduleWithCron`, default:
  hourly, configurable via `WithRenewInterval`). In a GoAkt cluster, cron schedules fire
  once cluster-wide by scheduler design, so exactly one node re-checks/re-issues per
  tick regardless of cluster size.
- **SNI lookup.** `Manager.GetCertificate` implements the `tls.Config.GetCertificate`
  signature directly.

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
- **`coordinator/redis.New`** is backed by Redis (`SET NX PX` for the lock, a Lua
  compare-and-delete for release - a real mutual exclusion, not a best-effort one) and
  coordinates issuance across every process pointed at the same Redis instance. It is a
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
)
```

`Drain` closes every socket, the per-connection actors unregister, and reconnecting
clients land on the replicas that stay up.

## What this library does and does not guarantee

**Does:**

- A locally registered connection is always written to directly, with no actor mailbox
  or cluster round-trip on the hot path.
- `SendToConnection`/`Broadcast` for a connection registered anywhere in a GoAkt cluster
  resolve and deliver correctly, given the cluster's own consistency model for actor
  directory propagation (not instantaneous - see the multi-node tests for the
  propagation delay this library's own test suite budgets for).
- Certificate issuance is deduplicated within a process (`singleflight`) and, with a
  shared `Coordinator`, arbitrated across every process sharing it so at most one
  process calls the configured `CertIssuer` for a given domain at a time.
- `MemoryCoordinator.TryLock` and `coordinator/redis.Coordinator.TryLock` are both real
  mutual exclusion: an unlock can never release a lock a later caller has since
  acquired.

**Does not:**

- Guarantee message delivery. `SendToConnection`/`Broadcast` are best-effort, same as any
  actor `Tell`: no acknowledgement, no retry, no persistence. Build your own ack/retry
  layer on top if you need it.
- Guarantee ordering across `Broadcast` calls to different topics, or between a local
  direct write and a remote delivery of the same broadcast racing each other.
- Provide its own certificate validation, ACME support, or CA. `CertIssuer` is a seam;
  bring your own (an ACME implementation is a natural third one, deliberately not
  shipped here to keep the dependency tree minimal).
- Drain in-flight application state on shutdown beyond closing the socket/stream -
  `Drain` terminates connections, it does not flush or persist anything for you.

## Development

```
go build ./...
go vet ./...
go test -race ./...
```

Redis-backed `Coordinator` conformance tests are skipped unless `TEST_REDIS_ADDR` is
set:

```
TEST_REDIS_ADDR=localhost:6379 go test ./coordinator/redis/...
```

## See also

- [GoAkt](https://github.com/Tochemey/goakt) - the actor system this library sits on top of.
- [`examples/echo`](./examples/echo) - a complete runnable sample.
