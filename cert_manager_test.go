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

package gateway_test

import (
	"context"
	"crypto/tls"
	"fmt"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/tochemey/goakt/v4/actor"
	"github.com/tochemey/goakt/v4/log"
	"github.com/tochemey/goakt/v4/testkit"

	gateway "github.com/StringKe/goakt-gateway"
)

// certRenewalClusterKind is a no-op actor.Actor registered as the cluster's declared kind
// set, purely to satisfy ClusterConfig.Validate (which requires at least one non-default
// kind or grain). It is never spawned; Manager.Start spawns its own internal renewal actor,
// and GoAkt's cluster spawn preconditions do not require a plain (non-singleton) actor's
// kind to be pre-registered.
type certRenewalClusterKind struct{}

func (certRenewalClusterKind) PreStart(*actor.Context) error { return nil }
func (certRenewalClusterKind) Receive(*actor.ReceiveContext) {}
func (certRenewalClusterKind) PostStop(*actor.Context) error { return nil }

// fakeIssuer is a CertIssuer that counts how many times Issue was actually called (as
// opposed to served from a cache/singleflight dedup), optionally sleeping to widen the
// window for concurrent callers to race.
type fakeIssuer struct {
	calls atomic.Int64
	delay time.Duration
	ttl   time.Duration
}

func (f *fakeIssuer) Issue(_ context.Context, domain string) (*gateway.Certificate, error) {
	f.calls.Add(1)
	if f.delay > 0 {
		time.Sleep(f.delay)
	}
	cert, key := generateTestCertificate(domain, time.Now().Add(f.ttl))
	return &gateway.Certificate{
		Domain:   domain,
		CertPEM:  cert,
		KeyPEM:   key,
		NotAfter: time.Now().Add(f.ttl),
	}, nil
}

func TestManagerGetCertificateSNILookup(t *testing.T) {
	system := newTestSystem(t)
	issuer := &fakeIssuer{ttl: time.Hour}
	manager := gateway.NewManager(system, log.DiscardLogger,
		gateway.WithCertIssuer(issuer),
		gateway.WithAllowedDomains("a.example.com", "b.example.com"),
		gateway.WithRenewInterval(""),
		gateway.WithRenewBefore(time.Minute),
	)

	cert, err := manager.GetCertificate(&tls.ClientHelloInfo{ServerName: "a.example.com"})
	require.NoError(t, err)
	require.NotNil(t, cert)

	other, err := manager.GetCertificate(&tls.ClientHelloInfo{ServerName: "b.example.com"})
	require.NoError(t, err)
	require.NotNil(t, other)

	require.Equal(t, int64(2), issuer.calls.Load(), "one issuance call per distinct SNI domain")

	// asking for a.example.com again must be served from the hot cache, not re-issued.
	again, err := manager.GetCertificate(&tls.ClientHelloInfo{ServerName: "a.example.com"})
	require.NoError(t, err)
	require.NotNil(t, again)
	require.Equal(t, int64(2), issuer.calls.Load())
}

func TestManagerGetCertificateEmptySNI(t *testing.T) {
	system := newTestSystem(t)
	manager := gateway.NewManager(system, log.DiscardLogger, gateway.WithRenewInterval(""))

	_, err := manager.GetCertificate(&tls.ClientHelloInfo{ServerName: ""})
	require.Error(t, err)
}

func TestManagerAllowedDomains(t *testing.T) {
	system := newTestSystem(t)
	issuer := &fakeIssuer{ttl: time.Hour}
	manager := gateway.NewManager(system, log.DiscardLogger,
		gateway.WithCertIssuer(issuer),
		gateway.WithAllowedDomains("allowed.example.com"),
		gateway.WithRenewInterval(""),
	)

	_, err := manager.EnsureCertificate(context.Background(), "not-allowed.example.com")
	require.ErrorIs(t, err, gateway.ErrDomainNotAllowed)
	require.Zero(t, issuer.calls.Load(), "a refused domain must never reach the issuer")

	_, err = manager.EnsureCertificate(context.Background(), "allowed.example.com")
	require.NoError(t, err)
	require.EqualValues(t, 1, issuer.calls.Load())
}

// TestManagerDenyByDefaultWithIssuerAndNoAdmissionConfigured is the deny-by-default
// regression: a Manager configured with a CertIssuer but neither WithAllowedDomains nor
// WithDomainPolicy must refuse every domain rather than treating an unconfigured admission
// check as allow-any, which would let an arbitrary SNI value trigger a real CA issuance.
func TestManagerDenyByDefaultWithIssuerAndNoAdmissionConfigured(t *testing.T) {
	system := newTestSystem(t)
	issuer := &fakeIssuer{ttl: time.Hour}
	manager := gateway.NewManager(system, log.DiscardLogger,
		gateway.WithCertIssuer(issuer),
		gateway.WithRenewInterval(""),
	)

	_, err := manager.EnsureCertificate(context.Background(), "unconfigured.example.com")
	require.ErrorIs(t, err, gateway.ErrDomainNotAllowed)
	require.Zero(t, issuer.calls.Load(), "deny-by-default must refuse before ever reaching the issuer")
}

// TestManagerAllowedDomainsWildcard pins wildcard allow-list matching to the RFC 6125 rule
// TLS clients use for a wildcard SAN: exactly one additional label, never zero and never two.
func TestManagerAllowedDomainsWildcard(t *testing.T) {
	cases := []struct {
		domain  string
		allowed bool
	}{
		{"a.example.com", true},
		{"b.example.com", true},
		{"A.Example.com", true}, // SNI case is not significant
		{"example.com", false},  // a wildcard does not match the bare parent
		{"a.b.example.com", false},
		{"a.example.com.evil.com", false},
		{"exampleXcom", false},
	}

	// one actor system for every case: ActorSystem names reject the dots in the subtest
	// names, and a Manager with renewal disabled does not touch the system anyway.
	system := newTestSystem(t)

	for _, tc := range cases {
		t.Run(tc.domain, func(t *testing.T) {
			issuer := &fakeIssuer{ttl: time.Hour}
			manager := gateway.NewManager(system, log.DiscardLogger,
				gateway.WithCertIssuer(issuer),
				gateway.WithAllowedDomains("*.example.com"),
				gateway.WithRenewInterval(""),
			)

			_, err := manager.EnsureCertificate(context.Background(), tc.domain)
			if tc.allowed {
				require.NoError(t, err)
				require.EqualValues(t, 1, issuer.calls.Load())
				return
			}
			require.ErrorIs(t, err, gateway.ErrDomainNotAllowed)
			require.Zero(t, issuer.calls.Load())
		})
	}
}

// countingPolicy is a DomainPolicy that records how often it was consulted, so the tests can
// assert the negative cache actually spares the (potentially database-backed) policy.
type countingPolicy struct {
	calls   atomic.Int64
	allowed map[string]bool
}

func (p *countingPolicy) policy(_ context.Context, domain string) (bool, error) {
	p.calls.Add(1)
	return p.allowed[domain], nil
}

// TestManagerDomainPolicyGatesIssuance is the multi-tenant custom-domain shape: the servable
// domains are not statically known, so a policy decides. A domain the policy does not know
// must never reach the CA.
func TestManagerDomainPolicyGatesIssuance(t *testing.T) {
	issuer := &fakeIssuer{ttl: time.Hour}
	policy := &countingPolicy{allowed: map[string]bool{"tenant.example.com": true}}

	manager := gateway.NewManager(newTestSystem(t), log.DiscardLogger,
		gateway.WithCertIssuer(issuer),
		gateway.WithDomainPolicy(policy.policy),
		gateway.WithRenewInterval(""),
		gateway.WithRenewBefore(time.Minute),
	)

	_, err := manager.EnsureCertificate(context.Background(), "attacker.example.com")
	require.ErrorIs(t, err, gateway.ErrDomainNotAllowed)
	require.Zero(t, issuer.calls.Load(), "a domain outside the policy must not trigger issuance")

	_, err = manager.EnsureCertificate(context.Background(), "tenant.example.com")
	require.NoError(t, err)
	require.EqualValues(t, 1, issuer.calls.Load())
}

// TestManagerStaticAllowListShortCircuitsPolicy verifies the cheap check runs first: a domain
// covered by WithAllowedDomains is admitted without paying for a policy lookup.
func TestManagerStaticAllowListShortCircuitsPolicy(t *testing.T) {
	issuer := &fakeIssuer{ttl: time.Hour}
	policy := &countingPolicy{allowed: map[string]bool{}}

	manager := gateway.NewManager(newTestSystem(t), log.DiscardLogger,
		gateway.WithCertIssuer(issuer),
		gateway.WithAllowedDomains("*.example.com"),
		gateway.WithDomainPolicy(policy.policy),
		gateway.WithRenewInterval(""),
	)

	_, err := manager.EnsureCertificate(context.Background(), "static.example.com")
	require.NoError(t, err)
	require.Zero(t, policy.calls.Load(), "the static allow list must be answered without consulting the policy")

	// a domain the static list does not cover still reaches the policy, which refuses it.
	_, err = manager.EnsureCertificate(context.Background(), "other.example.org")
	require.ErrorIs(t, err, gateway.ErrDomainNotAllowed)
	require.EqualValues(t, 1, policy.calls.Load())
}

// TestManagerNegativeCacheStopsRepeatedRefusals is the P0 regression: a peer flooding
// handshakes with an SNI value that is not servable must translate into neither a CA call nor
// a policy lookup per handshake.
func TestManagerNegativeCacheStopsRepeatedRefusals(t *testing.T) {
	issuer := &fakeIssuer{ttl: time.Hour}
	policy := &countingPolicy{allowed: map[string]bool{}}

	manager := gateway.NewManager(newTestSystem(t), log.DiscardLogger,
		gateway.WithCertIssuer(issuer),
		gateway.WithDomainPolicy(policy.policy),
		gateway.WithNegativeCacheTTL(time.Minute),
		gateway.WithRenewInterval(""),
	)

	for range 100 {
		_, err := manager.GetCertificate(&tls.ClientHelloInfo{ServerName: "flood.example.com"})
		require.ErrorIs(t, err, gateway.ErrDomainNotAllowed)
	}

	require.Zero(t, issuer.calls.Load(), "no handshake for a refused domain may reach the CA")
	require.EqualValues(t, 1, policy.calls.Load(), "the refusal must be answered from the negative cache within its ttl")
}

// TestManagerNegativeCacheExpires verifies the negative cache is a cache and not a permanent
// deny list: once the ttl elapses, a domain that has since become servable is admitted.
func TestManagerNegativeCacheExpires(t *testing.T) {
	issuer := &fakeIssuer{ttl: time.Hour}
	policy := &countingPolicy{allowed: map[string]bool{}}

	manager := gateway.NewManager(newTestSystem(t), log.DiscardLogger,
		gateway.WithCertIssuer(issuer),
		gateway.WithDomainPolicy(policy.policy),
		gateway.WithNegativeCacheTTL(50*time.Millisecond),
		gateway.WithRenewBefore(time.Minute),
		gateway.WithRenewInterval(""),
	)

	_, err := manager.EnsureCertificate(context.Background(), "late.example.com")
	require.ErrorIs(t, err, gateway.ErrDomainNotAllowed)

	// the tenant binds the domain after the refusal was cached.
	policy.allowed["late.example.com"] = true

	require.Eventually(t, func() bool {
		_, err := manager.EnsureCertificate(context.Background(), "late.example.com")
		return err == nil
	}, 3*time.Second, 20*time.Millisecond)
	require.EqualValues(t, 1, issuer.calls.Load())
}

// TestManagerNegativeCacheStopsRepeatedIssuanceFailures verifies that a domain whose issuance
// fails is negatively cached too: a failing CA must not be hammered once per handshake.
func TestManagerNegativeCacheStopsRepeatedIssuanceFailures(t *testing.T) {
	issuer := &failingIssuer{}
	manager := gateway.NewManager(newTestSystem(t), log.DiscardLogger,
		gateway.WithCertIssuer(issuer),
		gateway.WithAllowedDomains("broken.example.com"),
		gateway.WithNegativeCacheTTL(time.Minute),
		gateway.WithRenewInterval(""),
	)

	for range 50 {
		_, err := manager.GetCertificate(&tls.ClientHelloInfo{ServerName: "broken.example.com"})
		require.Error(t, err)
	}
	require.EqualValues(t, 1, issuer.calls.Load(), "a failed issuance must be remembered for the negative cache ttl")
}

// failingIssuer is a CertIssuer whose every call fails, counting attempts.
type failingIssuer struct {
	calls atomic.Int64
}

func (f *failingIssuer) Issue(_ context.Context, domain string) (*gateway.Certificate, error) {
	f.calls.Add(1)
	return nil, fmt.Errorf("issuance unavailable for %q", domain)
}

// TestManagerFallbackCertificate covers the "TLS terminated at a CDN edge, origin serves one
// catch-all certificate" deployment: no issuer at all, SNI irrelevant.
func TestManagerFallbackCertificate(t *testing.T) {
	certPEM, keyPEM := generateTestCertificate("catch-all.example.com", time.Now().Add(24*time.Hour))
	fallback, err := tls.X509KeyPair(certPEM, keyPEM)
	require.NoError(t, err)

	var loads atomic.Int64
	manager := gateway.NewManager(newTestSystem(t), log.DiscardLogger,
		gateway.WithRenewInterval(""),
		gateway.WithFallbackCertificate(func() (*tls.Certificate, error) {
			loads.Add(1)
			return &fallback, nil
		}),
	)

	// no SNI at all: there is nothing to look a certificate up by, so the fallback is served
	// instead of failing the handshake.
	cert, err := manager.GetCertificate(&tls.ClientHelloInfo{ServerName: ""})
	require.NoError(t, err)
	require.Equal(t, fallback.Certificate, cert.Certificate)

	// an SNI value with no certificate and no issuer to obtain one also falls back.
	cert, err = manager.GetCertificate(&tls.ClientHelloInfo{ServerName: "whatever.example.com"})
	require.NoError(t, err)
	require.Equal(t, fallback.Certificate, cert.Certificate)
	require.EqualValues(t, 2, loads.Load())
}

// TestManagerFallbackNotServedToRefusedDomain pins the security boundary of the fallback: an
// explicitly refused domain fails the handshake rather than being handed a valid certificate,
// which would make the admission check pointless.
func TestManagerFallbackNotServedToRefusedDomain(t *testing.T) {
	certPEM, keyPEM := generateTestCertificate("catch-all.example.com", time.Now().Add(24*time.Hour))
	fallback, err := tls.X509KeyPair(certPEM, keyPEM)
	require.NoError(t, err)

	manager := gateway.NewManager(newTestSystem(t), log.DiscardLogger,
		gateway.WithRenewInterval(""),
		gateway.WithAllowedDomains("allowed.example.com"),
		gateway.WithFallbackCertificate(func() (*tls.Certificate, error) { return &fallback, nil }),
	)

	_, err = manager.GetCertificate(&tls.ClientHelloInfo{ServerName: "refused.example.com"})
	require.ErrorIs(t, err, gateway.ErrDomainNotAllowed)
}

// TestManagerReloadingCertificateAsFallback wires the two halves of the catch-all deployment
// together: the origin serves a single certificate mounted from disk, rotated underneath it.
func TestManagerReloadingCertificateAsFallback(t *testing.T) {
	dir := t.TempDir()
	certPath := filepath.Join(dir, "tls.crt")
	keyPath := filepath.Join(dir, "tls.key")
	writeCertPair(t, certPath, keyPath, "edge.example.com")

	reloader, err := gateway.NewReloadingCertificate(certPath, keyPath, 20*time.Millisecond, log.DiscardLogger)
	require.NoError(t, err)
	reloader.Start(context.Background())
	t.Cleanup(reloader.Stop)

	manager := gateway.NewManager(newTestSystem(t), log.DiscardLogger,
		gateway.WithRenewInterval(""),
		gateway.WithFallbackCertificate(reloader.Get),
	)

	cert, err := manager.GetCertificate(&tls.ClientHelloInfo{ServerName: "anything.example.com"})
	require.NoError(t, err)
	require.Equal(t, "edge.example.com", cert.Leaf.Subject.CommonName)

	writeCertPair(t, certPath, keyPath, "rotated-edge.example.com")
	require.Eventually(t, func() bool {
		cert, err := manager.GetCertificate(&tls.ClientHelloInfo{ServerName: "anything.example.com"})
		return err == nil && cert.Leaf.Subject.CommonName == "rotated-edge.example.com"
	}, 5*time.Second, 20*time.Millisecond)
}

func TestManagerNoIssuerConfigured(t *testing.T) {
	system := newTestSystem(t)
	manager := gateway.NewManager(system, log.DiscardLogger, gateway.WithRenewInterval(""))

	_, err := manager.EnsureCertificate(context.Background(), "example.com")
	require.ErrorIs(t, err, gateway.ErrNoIssuer)
}

// TestManagerIssuanceLockExpiredDuringIssuance verifies that a CertIssuer call slower
// than the configured issuance lock TTL surfaces ErrIssuanceLockExpired instead of
// silently caching a result whose single-issuer guarantee is no longer certain (see
// WithIssuanceLockTTL).
func TestManagerIssuanceLockExpiredDuringIssuance(t *testing.T) {
	system := newTestSystem(t)
	issuer := &fakeIssuer{ttl: time.Hour, delay: 100 * time.Millisecond}
	manager := gateway.NewManager(system, log.DiscardLogger,
		gateway.WithCertIssuer(issuer),
		gateway.WithAllowedDomains("slow.example.com"),
		gateway.WithIssuanceLockTTL(20*time.Millisecond),
		gateway.WithRenewInterval(""),
	)

	_, err := manager.EnsureCertificate(context.Background(), "slow.example.com")
	require.ErrorIs(t, err, gateway.ErrIssuanceLockExpired)
}

// TestManagerIssuanceSingleFlight verifies that N concurrent EnsureCertificate calls for
// the same, previously-unissued domain result in exactly one call to the underlying
// CertIssuer - the local single-flight dedup layer that sits underneath (and, with a
// shared Coordinator, in front of) the coordinated TryLock arbitration.
func TestManagerIssuanceSingleFlight(t *testing.T) {
	system := newTestSystem(t)
	issuer := &fakeIssuer{ttl: time.Hour, delay: 100 * time.Millisecond}
	manager := gateway.NewManager(system, log.DiscardLogger,
		gateway.WithCertIssuer(issuer),
		gateway.WithAllowedDomains("concurrent.example.com"),
		gateway.WithRenewInterval(""),
	)

	const concurrency = 20
	var wg sync.WaitGroup
	errs := make([]error, concurrency)
	for i := range concurrency {
		wg.Go(func() {
			_, errs[i] = manager.EnsureCertificate(context.Background(), "concurrent.example.com")
		})
	}
	wg.Wait()

	for _, err := range errs {
		require.NoError(t, err)
	}
	require.Equal(t, int64(1), issuer.calls.Load(), "exactly one issuance call for a concurrent cold start on one process")
}

// TestManagerStartTriggersRenewal verifies the renewal schedule Start registers actually
// fires and calls back into the issuer: with a certificate whose lifetime is shorter than
// renewBefore, the very first cron tick after issuance must observe it as due for renewal.
//
// The cron expression uses go-quartz's field syntax directly ("*/1 * * * * *": every
// second) rather than the "@every" shorthand some cron libraries support - go-quartz (used
// by actor.ActorSystem.ScheduleWithCron) does not implement that shorthand, so this is also
// the first test to actually exercise Manager.Start/renewAll rather than disabling renewal
// via WithRenewInterval("").
func TestManagerStartTriggersRenewal(t *testing.T) {
	system := newTestSystem(t)
	issuer := &fakeIssuer{ttl: 300 * time.Millisecond}
	manager := gateway.NewManager(system, log.DiscardLogger,
		gateway.WithCertIssuer(issuer),
		gateway.WithAllowedDomains("renews.example.com"),
		gateway.WithRenewBefore(2*time.Second),
		gateway.WithRenewInterval("*/1 * * * * *"),
	)

	ctx := context.Background()
	require.NoError(t, manager.Start(ctx))
	t.Cleanup(func() { _ = manager.Stop(context.Background()) })

	_, err := manager.EnsureCertificate(ctx, "renews.example.com")
	require.NoError(t, err)
	require.EqualValues(t, 1, issuer.calls.Load())

	require.Eventually(t, func() bool {
		return issuer.calls.Load() >= 2
	}, 5*time.Second, 50*time.Millisecond,
		"the renewal schedule's cron tick must call the issuer again once the cached certificate is within renewBefore of expiry")
}

// TestManagerFromStore_InvalidPEMFallsBackToIssue verifies that a certificate already
// present in the CertStore but corrupted (invalid PEM/key material) is not fatal: Manager
// must log and fall back to a fresh issuance rather than panicking or returning the broken
// certificate.
func TestManagerFromStore_InvalidPEMFallsBackToIssue(t *testing.T) {
	system := newTestSystem(t)
	store := gateway.NewMemoryCertStore()
	require.NoError(t, store.Put(context.Background(), &gateway.Certificate{
		Domain:  "corrupt.example.com",
		CertPEM: []byte("not a valid certificate"),
		KeyPEM:  []byte("not a valid key"),
		// Far from expiry so the certificate is not discarded as stale before control
		// reaches parseAndVerify - the branch under test.
		NotAfter: time.Now().Add(365 * 24 * time.Hour),
	}))

	issuer := &fakeIssuer{ttl: time.Hour}
	manager := gateway.NewManager(system, log.DiscardLogger,
		gateway.WithCertIssuer(issuer),
		gateway.WithCertStore(store),
		gateway.WithAllowedDomains("corrupt.example.com"),
		gateway.WithRenewBefore(time.Hour),
		gateway.WithRenewInterval(""),
	)

	var cert *tls.Certificate
	var err error
	require.NotPanics(t, func() {
		cert, err = manager.EnsureCertificate(context.Background(), "corrupt.example.com")
	})
	require.NoError(t, err)
	require.NotNil(t, cert)
	require.EqualValues(t, 1, issuer.calls.Load(), "the corrupt stored certificate must be discarded and re-issued, exactly once")
}

// TestManagerStartClusterUniquePerNodeActorName is the P0 regression for the cert renewal
// actor name collision: in cluster mode, ActorSystem.Spawn checks actor-name uniqueness
// cluster-wide (via the cluster actor directory), not just on the local node. A fixed
// renewal actor name would therefore make every node after the first fail Manager.Start
// with errors.ErrActorAlreadyExists, aborting startup on that node. This spins up a real
// two-node GoAkt cluster and asserts both Manager.Start calls succeed, and that the shared
// certRenewalReference still delivers each cron tick to exactly one node rather than to
// both, so the per-node-unique actor name did not weaken the existing cluster-wide
// single-fire guarantee (which GoAkt keys by schedule reference, not by actor identity).
func TestManagerStartClusterUniquePerNodeActorName(t *testing.T) {
	ctx := context.Background()
	logger := log.DiscardLogger

	mn := testkit.NewMultiNodes(t, logger, []actor.Actor{&certRenewalClusterKind{}}, nil)
	mn.Start()
	t.Cleanup(mn.Stop)

	node1 := mn.StartNode(ctx, "node-1")
	node2 := mn.StartNode(ctx, "node-2")

	const domain = "cluster-renewal.example.com"
	// Shared across both Managers so its call counter is the one signal both nodes affect;
	// each Manager otherwise gets its own default (unshared) Coordinator/CertStore, as two
	// genuinely independent processes would, so a shared Coordinator lock cannot itself mask
	// a double-fire by serializing the two nodes' renewal attempts.
	issuer := &fakeIssuer{ttl: 300 * time.Millisecond}

	manager1 := gateway.NewManager(node1.ActorSystem(), logger,
		gateway.WithCertIssuer(issuer),
		gateway.WithAllowedDomains(domain),
		gateway.WithRenewBefore(2*time.Second),
		gateway.WithRenewInterval("*/1 * * * * *"),
	)
	manager2 := gateway.NewManager(node2.ActorSystem(), logger,
		gateway.WithCertIssuer(issuer),
		gateway.WithAllowedDomains(domain),
		gateway.WithRenewBefore(2*time.Second),
		gateway.WithRenewInterval("*/1 * * * * *"),
	)

	// Before the fix, the second Start call below fails with errors.ErrActorAlreadyExists:
	// both nodes try to spawn an actor with the identical fixed name, which the cluster
	// actor directory rejects cluster-wide, not just locally. The pause between the two
	// Start calls gives the cluster's actor directory time to propagate node 1's
	// registration to node 2 before it spawns - without it, a re-introduced name collision
	// could go unnoticed by this test purely because of replication lag.
	require.NoError(t, manager1.Start(ctx))
	t.Cleanup(func() { _ = manager1.Stop(context.Background()) })
	time.Sleep(2 * time.Second)
	require.NoError(t, manager2.Start(ctx))
	t.Cleanup(func() { _ = manager2.Stop(context.Background()) })

	// Prime both nodes' local hot cache with the same, already-due-for-renewal domain.
	_, err := manager1.EnsureCertificate(ctx, domain)
	require.NoError(t, err)
	_, err = manager2.EnsureCertificate(ctx, domain)
	require.NoError(t, err)
	require.EqualValues(t, 2, issuer.calls.Load(), "both nodes cold-issuing independently")

	// The next cron tick must renew the domain exactly once cluster-wide: the fire claim is
	// keyed by the schedule reference (identical on both nodes), not by the target actor's
	// name, so exactly one of the two nodes' renewal actors receives it.
	require.Eventually(t, func() bool {
		return issuer.calls.Load() >= 3
	}, 5*time.Second, 20*time.Millisecond,
		"the renewal schedule must still fire in a cluster once both nodes' uniquely-named actors are registered")

	// Give a near-simultaneous second delivery (the failure mode of a broken single-fire
	// guarantee) time to land, while staying well under the 1s tick period so a legitimate
	// next tick cannot be mistaken for a double-fire on this one.
	time.Sleep(150 * time.Millisecond)
	require.EqualValues(t, 3, issuer.calls.Load(),
		"exactly one node's renewal actor must have received this tick, not both")
}
