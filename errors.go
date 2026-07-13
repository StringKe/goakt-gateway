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

package gateway

import "errors"

var (
	// ErrConnectionNotFound is returned by Registry.SendToConnection when the target
	// connection id is not registered locally and cannot be located anywhere else in
	// the cluster either.
	ErrConnectionNotFound = errors.New("gateway: connection not found")

	// ErrConnectionExists is returned by Registry.Register when the given connection id
	// is already registered.
	ErrConnectionExists = errors.New("gateway: connection already registered")

	// ErrConnectionClosed is returned when writing to a connection that has already
	// been unregistered/closed.
	ErrConnectionClosed = errors.New("gateway: connection closed")

	// ErrBackpressure is returned when a connection's outbound buffer is full and the
	// write could not be queued without blocking.
	ErrBackpressure = errors.New("gateway: connection send buffer full")

	// ErrUnauthorized is returned by the WebSocket/SSE upgrade path when the configured
	// AuthFunc rejects the incoming request.
	ErrUnauthorized = errors.New("gateway: unauthorized")

	// ErrPubSubUnavailable is returned when a topic bridge is requested but the actor
	// system was not started with pub/sub enabled (see actor.ActorSystem.TopicActor).
	ErrPubSubUnavailable = errors.New("gateway: pub/sub is not enabled on this actor system")

	// ErrDomainNotAllowed is returned by Manager.GetCertificate and EnsureCertificate
	// when the requested SNI/domain is not covered by the configured allow list.
	ErrDomainNotAllowed = errors.New("gateway: domain not allowed")

	// ErrNoIssuer is returned by Manager when certificate issuance is required but no
	// CertIssuer was configured.
	ErrNoIssuer = errors.New("gateway: no certificate issuer configured")

	// ErrIssuanceTimeout is returned when this node lost the coordinator-arbitrated
	// issuance race for a domain and the winning node did not publish a certificate
	// before the wait deadline elapsed.
	ErrIssuanceTimeout = errors.New("gateway: timed out waiting for coordinated certificate issuance")

	// ErrCertificateNotFound is returned by CertStore.Get when no certificate is stored
	// for the given domain.
	ErrCertificateNotFound = errors.New("gateway: certificate not found")

	// ErrOriginPullVerificationFailed is returned when Authenticated Origin Pulls is
	// enabled and the inbound client certificate does not chain to the configured CA.
	ErrOriginPullVerificationFailed = errors.New("gateway: origin pull client certificate verification failed")

	// ErrLockNotAcquired is returned by Coordinator.TryLock when the lock is already
	// held by someone else.
	ErrLockNotAcquired = errors.New("gateway: lock not acquired")

	// ErrOriginNotAllowed is returned by the WebSocket upgrade path when the request's
	// Origin header matches none of the configured origin patterns.
	ErrOriginNotAllowed = errors.New("gateway: websocket origin not allowed")

	// ErrPayloadTooLarge tags an inbound message that exceeds the connection's configured
	// read limit. The WebSocket handler does not return it: coder/websocket closes the
	// socket with status 1009 (message too big) on its own, and the handler logs this
	// sentinel as the reason. It is not delivered to any caller.
	ErrPayloadTooLarge = errors.New("gateway: inbound payload exceeds the configured read limit")

	// ErrRegistryClosed is returned by Registry.Register once the Registry has been closed.
	ErrRegistryClosed = errors.New("gateway: registry is closed")

	// ErrRateLimited tags an inbound message rejected by a connection's inbound rate limit.
	// The WebSocket handler does not return it: it logs this sentinel and, under
	// BackpressureClose, uses it as the socket's close reason. It is not delivered to any
	// caller.
	ErrRateLimited = errors.New("gateway: inbound message rate limit exceeded")

	// ErrHistoryGap is returned by SSEHistory.Since when the requested Last-Event-ID
	// cannot be located for the connection, because it was evicted from a bounded buffer,
	// because it belongs to a different connection, or because the connection's history is
	// gone entirely. The events that are still retained are returned alongside it: the
	// caller learns both what it can replay and that something was lost. SSEHandler turns
	// it into an SSEGapEventName event on the wire instead of silently resuming. It is part
	// of the SSEHistory contract that third-party implementations must honour.
	ErrHistoryGap = errors.New("gateway: sse history gap, the requested Last-Event-ID is no longer retained")

	// ErrIssuanceLockExpired is returned by Manager when a CertIssuer call outlives the
	// configured issuance lock TTL (see WithIssuanceLockTTL). The Coordinator lock has no
	// renewal/heartbeat, so a slow issuer can let the lock expire and a second process
	// acquire it while the first is still mid-issuance; returning this error instead of
	// silently caching the result makes that window visible rather than quietly risking a
	// duplicate CertIssuer call. Configure a WithIssuanceLockTTL comfortably longer than
	// your CertIssuer's worst-case latency to avoid it.
	ErrIssuanceLockExpired = errors.New("gateway: certificate issuance took longer than the issuance lock ttl")

	// ErrPresenceWatchUnsupported is returned by Registry.WatchPresence when no Presence
	// backend is configured or the configured one does not implement PresenceWatcher.
	ErrPresenceWatchUnsupported = errors.New("gateway: presence backend does not support Watch")

	// ErrConfirmationTimeout is returned by the cross-node delivery path when
	// WithDeliveryConfirmation is enabled and the remote connActor did not acknowledge the
	// write within the configured confirmation timeout (see WithConfirmationTimeout).
	ErrConfirmationTimeout = errors.New("gateway: cross-node delivery confirmation timed out")

	// ErrStaleOwner is returned by an owner-lease-fenced operation (refresh, release, and
	// (in later phases) delivery/presence/outbox calls that carry a connection generation)
	// once a newer generation has taken over the connection. It tells the caller its
	// generation is no longer current, not that the operation itself failed transiently.
	ErrStaleOwner = errors.New("gateway: operation rejected: a newer generation has taken over this connection")

	// ErrOwnerHeld is returned by a non-takeover owner lease acquisition when the lease is
	// currently held by another node and has not yet expired.
	ErrOwnerHeld = errors.New("gateway: connection owner lease is held by another node")

	// ErrOwnerLeaseUnsupported is returned when WithOwnerLease is configured with a
	// Coordinator that does not implement CASCoordinator: owner lease fencing requires an
	// atomic compare-and-swap primitive that a plain Coordinator does not provide.
	ErrOwnerLeaseUnsupported = errors.New("gateway: WithOwnerLease requires a Coordinator implementing CASCoordinator")
)
