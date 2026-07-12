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

	// ErrIssuanceLockExpired is returned by Manager when a CertIssuer call outlives the
	// configured issuance lock TTL (see WithIssuanceLockTTL). The Coordinator lock has no
	// renewal/heartbeat, so a slow issuer can let the lock expire and a second process
	// acquire it while the first is still mid-issuance; returning this error instead of
	// silently caching the result makes that window visible rather than quietly risking a
	// duplicate CertIssuer call. Configure a WithIssuanceLockTTL comfortably longer than
	// your CertIssuer's worst-case latency to avoid it.
	ErrIssuanceLockExpired = errors.New("gateway: certificate issuance took longer than the issuance lock ttl")
)
