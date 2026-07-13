// MIT License
//
// Copyright (c) 2026 StringKe
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

// Package gateway turns a GoAkt cluster into a single, cluster-aware ingress tier for
// HTTP, WebSocket, and Server-Sent Events (SSE) traffic - the role an nginx/caddy
// front-tier normally plays in front of a stateless HTTP fleet.
//
// Two design boundaries drive every type in this package:
//
//   - Plain HTTP requests are served by ordinary net/http handlers with zero actor
//     involvement. Actorizing a request/response cycle that lives for a few
//     milliseconds is pure overhead; nothing in this package puts one in the way of a
//     short-lived HTTP handler.
//   - Only long-lived WebSocket/SSE connections get an addressable identity. Each
//     accepted connection is registered in a local Registry and, so that it can be
//     addressed from any node in the cluster, backed by a lightweight ephemeral actor
//     (relocation disabled, long-lived passivation) whose sole job is to relay a
//     delivery to the socket it owns.
//
// # Delivery
//
// Message delivery to a connection is two-tier: Registry.SendToConnection checks the
// local connection table first and, on a hit, writes the socket directly with no actor
// or cluster machinery involved. Only on a miss does it fall back to a cluster-wide
// actor lookup (actor.ActorSystem.ActorOf) and a remote Tell to the node that owns the
// connection. Registry.SendToGroup and Registry.Broadcast follow the same shape for
// fan-out: local members are written directly, and remote nodes are reached through a
// small pub/sub bridge built entirely on GoAkt's public actor.Subscribe/actor.Unsubscribe
// API (see bridge.go) - this package has no dependency on any GoAkt internal package.
// Registering a connection with a group or with topics therefore requires an actor system
// started with actor.WithPubSub.
//
// Fan-out returns a DeliveryResult rather than a bare error. Only DeliveryResult.Delivered
// is first-hand knowledge (a local connection's outbound buffer accepted the payload);
// DeliveryResult.Remote counts what was handed to the cluster, and whether it reached a
// socket is unknowable from the sending node. DeliveryResult.None reports that nothing at
// all happened, which is the signal to fall back to an offline channel such as web push.
//
// # Identity, groups, and presence
//
// A connection id identifies one socket. A ConnInfo.Group identifies the party behind it
// ("user:123"), which typically holds several sockets at once - a phone, a laptop, three
// browser tabs. SendToGroup addresses the party; Broadcast addresses a topic subscription.
//
// Presence answers "which connections does this identity hold, cluster-wide?" through
// TTL'd leases that Registry refreshes in the background. It is optional: without it,
// Registry.IsOnline and Registry.LocalConnectionsOf see only the local node, and
// DeliveryResult.None is not a trustworthy cluster-wide offline signal. NewMemoryPresence
// covers a single process; the presence/redis subpackage shares presence across a cluster.
// Because leases expire, presence is eventually consistent: IsOnline reporting true means
// a socket was recently held, not that one is writable right now.
//
// # Opt-in reliability and lifecycle
//
// Everything above is the default behavior and costs nothing extra. Five capabilities are
// available as constructor options that a deployment adds only when it needs them; a
// Registry that wires none of them behaves exactly as described above.
//
//   - WithOfflineChannel routes a SendToGroup that finds the target offline cluster-wide
//     (DeliveryResult.None) to an OfflineChannel such as offline/webpush, off the main
//     delivery path.
//   - WithDeliveryConfirmation switches the remote hand-off from Tell to Ask so
//     DeliveryResult.Remote counts confirmed remote socket writes, bounded by
//     WithConfirmationTimeout (default 5s, ErrConfirmationTimeout on expiry). It still ends
//     at the remote outbound buffer, not the client. The default Tell path is unaffected.
//   - WithOutbox plus Registry.Ack make SendToConnection at-least-once: persist, deliver,
//     wait for the client ack, redeliver unacked messages on reconnect. The Outbox assigns
//     each message a per-connection Seq from its own durable state, so the sequence stays
//     monotonic across restarts and across nodes appending to the same connection; a
//     per-process counter could not. Duplicates are still possible, so clients dedupe on the
//     PersistedMessage ID/Seq. NewMemoryOutbox is process-local; the persistence/redis
//     subpackage survives restart and works across nodes. WithOutboxEnvelope makes both
//     real-time delivery and reconnect replay use the same ASCII base64 frame containing the
//     message ID, sequence, and original payload. Without WithOutboxEnvelope, delivery uses
//     the original raw payload.
//   - Registry.WatchPresence and Registry.GroupMembers expose optional PresenceWatcher and
//     PresenceDirectory capabilities (both implemented by MemoryPresence and presence/redis);
//     the redis watch is best-effort Redis Pub/Sub, not a durable event log.
//   - Registry.Disconnect / Registry.DisconnectGroup evict a connection or an identity via a
//     close hook registered at Register time; WithWSReauth / WithSSEReauth re-run auth on an
//     interval and tear the connection down on failure. WithWSCompression and
//     WithWSGroupRate tune the WebSocket wire and per-group inbound rate.
//
// # Strict multi-instance correctness
//
// The GoAkt actor directory a Registry otherwise relies on to make a connection addressable
// cluster-wide is PA/EC eventually consistent, not an atomic cluster lock: two nodes racing
// Register for the same connection id can both observe no owner and both succeed (split
// brain). Sequential takeover - one node evicting an owner it can already see - works without
// anything below; only a genuine concurrent race needs it. WithOwnerLease(c) only accepts a
// LinearizableFencingCoordinator, which explicitly declares linearizable fencing across failover. The
// MemoryCoordinator provides that capability only for a single process. The Redis or Valkey
// Coordinator remains valid for certificate coordination, but asynchronous replication cannot
// provide strict OwnerLease fencing across failover and it deliberately does not implement
// LinearizableFencingCoordinator. A multi-instance deployment therefore supplies a consensus-backed
// LinearizableFencingCoordinator. WithOwnerLease closes the window by acquiring a CAS-arbitrated lease per
// connection id before Register publishes it locally, and fencing every subsequent
// operation - every local and cross-node delivery path (SendToConnection, SendToGroup,
// Broadcast, and a remote connActor's own delivery), background lease renewal, Presence
// Refresh/Leave (PresenceFencer), SSE history append (GenerationalHistory), Outbox Ack
// (OutboxGenerationAdvancer), and handler unregister (Registry.RegisterHandle /
// ConnHandle.UnregisterHandle) - by the generation that lease bumps on every takeover, so a
// dispossessed owner is rejected with ErrStaleOwner rather than silently continuing to act. A
// takeover whose physical eviction never completes (e.g. ErrTakeoverTimeout) restores the
// lease to whichever owner it preempted instead of leaving that owner permanently fenced out
// by an attempt that never actually succeeded. Without WithOwnerLease, Registry keeps its
// current single-instance-safe, zero-cost semantics unchanged.
//
// # Redis / Valkey backends
//
// Five abstractions have a shared backend on github.com/redis/go-redis/v9: Coordinator
// (coordinator/redis), Presence (presence/redis), CertStore (store/redis), SSEHistory
// (ssehistory/redis), and Outbox (persistence/redis). Each takes a redis.UniversalClient;
// go-redis speaks the identical RESP protocol to a Redis and a Valkey server, and every
// backend uses only commands present on both, so one client pointed at either server works
// with no code change. One instance can carry all five under distinct WithKeyPrefix
// namespaces. Interchangeability is checked by conformance suites run against both a real
// Redis and a real Valkey server.
//
// # Backpressure
//
// Every connection has a bounded outbound buffer. A slow consumer that fills it must never
// stall a fan-out to everyone else, so a full buffer is resolved by BackpressurePolicy:
// BackpressureDrop (the default) discards the message, returns ErrBackpressure, and keeps
// the connection; BackpressureClose closes the connection and lets the client reconnect and
// resynchronize. There is deliberately no blocking policy.
//
// # TLS
//
// The TLS side of the package (CertIssuer, CertStore, Manager) lets every node in the
// cluster terminate TLS for any hosted domain from one certificate, issued exactly once
// cluster-wide: issuance is arbitrated through a storage-agnostic Coordinator this
// library owns (see coordinator.go), the resulting certificate is distributed to every
// node through the same Coordinator, and renewal is driven by GoAkt's cluster-single-fire
// cron schedule (actor.ActorSystem.ScheduleWithCron) so only one node re-issues per
// renewal window. Domain admission (WithAllowedDomains, WithDomainPolicy) plus a negative
// cache and a bounded certificate cache keep a flood of arbitrary SNI from burning CA rate
// limit or memory. Deployments that terminate TLS at an edge instead can skip issuance
// entirely and serve one hot-reloadable certificate through WithFallbackCertificate and
// ReloadingCertificate. See Manager, CertIssuer, CertStore, and Coordinator.
package gateway
