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

// This file is a white-box (package gateway) test file so it can reach the unexported
// certLockCoordinatorKeyPrefix/certCoordinatorKeyPrefix key formats and defaultRenewInterval
// Manager uses internally. See cert_manager_test.go (package gateway_test) for the
// black-box Manager tests. newRaceTestSystem is shared from registry_race_test.go.
package gateway

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"sync"
	"testing"
	"time"

	"github.com/reugn/go-quartz/quartz"
	"github.com/stretchr/testify/require"

	"github.com/tochemey/goakt/v4/log"
)

// TestDefaultRenewIntervalIsValidCron pins the default renewal schedule to a
// go-quartz-parseable expression: a descriptor form like "@every 1h" would be rejected
// by go-quartz, which would abort Manager.Start on any system that did not override
// WithRenewInterval.
func TestDefaultRenewIntervalIsValidCron(t *testing.T) {
	_, err := quartz.NewCronTrigger(defaultRenewInterval)
	require.NoError(t, err)
}

// generateInternalTestCertificate creates a minimal, self-signed leaf certificate for
// domain, PEM-encoded along with its private key. Mirrors testcert_test.go's
// generateTestCertificate, duplicated here because this white-box file cannot import the
// external test package.
func generateInternalTestCertificate(domain string, notAfter time.Time) (certPEM, keyPEM []byte) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		panic(err)
	}

	template := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: domain},
		DNSNames:     []string{domain},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     notAfter,
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}

	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		panic(err)
	}

	certPEM = pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyPEM = pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)})
	return certPEM, keyPEM
}

// internalFakeIssuer is a minimal CertIssuer for the white-box tests in this file.
type internalFakeIssuer struct {
	mu    sync.Mutex
	calls int
	ttl   time.Duration
}

func (f *internalFakeIssuer) Issue(_ context.Context, domain string) (*Certificate, error) {
	f.mu.Lock()
	f.calls++
	f.mu.Unlock()
	certPEM, keyPEM := generateInternalTestCertificate(domain, time.Now().Add(f.ttl))
	return &Certificate{
		Domain:   domain,
		CertPEM:  certPEM,
		KeyPEM:   keyPEM,
		NotAfter: time.Now().Add(f.ttl),
	}, nil
}

func (f *internalFakeIssuer) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

// reusedPEMIssuer hands out the same, once-generated PEM pair for every domain. The LRU
// bound tests issue thousands of certificates and only care about cache bookkeeping, so
// paying for a fresh RSA key per domain (as internalFakeIssuer does) would dominate their
// runtime for no added coverage. The shared leaf is a "*.example.com" wildcard so that it
// actually covers every "d<i>.example.com" the bound tests request: Manager verifies the
// leaf's SAN against the requested domain, so a single-name leaf reused across domains
// would (correctly) be rejected.
type reusedPEMIssuer struct {
	certPEM []byte
	keyPEM  []byte
	ttl     time.Duration
}

func newReusedPEMIssuer(ttl time.Duration) *reusedPEMIssuer {
	certPEM, keyPEM := generateInternalTestCertificate("*.example.com", time.Now().Add(ttl))
	return &reusedPEMIssuer{certPEM: certPEM, keyPEM: keyPEM, ttl: ttl}
}

func (r *reusedPEMIssuer) Issue(_ context.Context, domain string) (*Certificate, error) {
	return &Certificate{
		Domain:   domain,
		CertPEM:  r.certPEM,
		KeyPEM:   r.keyPEM,
		NotAfter: time.Now().Add(r.ttl),
	}, nil
}

// TestManagerHotCacheIsBounded verifies the hot certificate cache cannot be grown without
// limit by a peer that sends a new SNI value on every handshake: the cache is an LRU capped
// at WithMaxCachedCerts.
func TestManagerHotCacheIsBounded(t *testing.T) {
	ctx := context.Background()
	system := newRaceTestSystem(t)

	const capacity = 64
	issuer := newReusedPEMIssuer(time.Hour)
	manager := NewManager(system, log.DiscardLogger,
		WithCertIssuer(issuer),
		WithAllowedDomains("*.example.com"),
		WithMaxCachedCerts(capacity),
		WithRenewBefore(time.Minute),
		WithRenewInterval(""),
		WithIssuanceLockTTL(10*time.Second),
	)

	const domains = 2000
	for i := range domains {
		_, err := manager.EnsureCertificate(ctx, fmt.Sprintf("d%d.example.com", i))
		require.NoError(t, err)
	}

	require.Equal(t, capacity, manager.certs.len())
	// the most recent domain is still hot, the first one was evicted long ago.
	_, ok := manager.certs.get(fmt.Sprintf("d%d.example.com", domains-1))
	require.True(t, ok)
	_, ok = manager.certs.get("d0.example.com")
	require.False(t, ok)
}

// TestManagerNegativeCacheIsBounded verifies the same bound applies to the negative cache:
// remembering every refused SNI value forever would just move the memory-growth problem.
func TestManagerNegativeCacheIsBounded(t *testing.T) {
	ctx := context.Background()
	system := newRaceTestSystem(t)

	const capacity = 64
	issuer := &internalFakeIssuer{ttl: time.Hour}
	manager := NewManager(system, log.DiscardLogger,
		WithCertIssuer(issuer),
		WithAllowedDomains("allowed.example.com"),
		WithMaxCachedCerts(capacity),
		WithNegativeCacheTTL(time.Minute),
		WithRenewInterval(""),
	)

	for i := range 2000 {
		_, err := manager.EnsureCertificate(ctx, fmt.Sprintf("refused%d.example.com", i))
		require.ErrorIs(t, err, ErrDomainNotAllowed)
	}

	require.Equal(t, capacity, manager.negatives.len())
	require.Equal(t, 0, issuer.callCount())
}

// TestManagerRenewalHolderDies_TTLRecovery simulates a coordinated issuance lock holder
// that acquires the lock and then never calls unlock (e.g. the process crashed
// mid-issuance): a second caller's EnsureCertificate call must not hang forever. It
// observes Manager's documented behavior: waitForCoordinatedCert gives up and returns
// ErrIssuanceTimeout once its own deadline (now + lockTTL) elapses, since it only polls
// the Coordinator for a published certificate and never itself retries acquiring the
// lock.
func TestManagerRenewalHolderDies_TTLRecovery(t *testing.T) {
	ctx := context.Background()
	system := newRaceTestSystem(t)

	coordinator := NewMemoryCoordinator()

	const domain = "holder-dies.example.com"
	const lockTTL = 400 * time.Millisecond

	// Simulate a caller that won the issuance race and then crashed before publishing a
	// certificate or unlocking: acquire the lock directly and never release it.
	_, err := coordinator.TryLock(ctx, certLockCoordinatorKeyPrefix+domain, lockTTL)
	require.NoError(t, err)

	// an issuer must be configured for this path to be reached at all: doEnsure refuses to
	// take the issuance lock when there is no issuer to make progress with.
	manager := NewManager(system, log.DiscardLogger,
		WithCoordinator(coordinator),
		WithCertIssuer(&internalFakeIssuer{ttl: time.Hour}),
		WithAllowedDomains(domain),
		WithRenewInterval(""),
		WithIssuanceLockTTL(lockTTL),
	)

	_, err = manager.EnsureCertificate(ctx, domain)
	require.ErrorIs(t, err, ErrIssuanceTimeout)
}

// TestManagerFromCoordinator_CorruptRecordFallsBackToIssue verifies that a corrupted
// certificate record already present in the Coordinator (e.g. from an incompatible
// previous version, or storage corruption) is not fatal: Manager must log and fall back
// to issuing a fresh certificate rather than panicking on the malformed JSON.
func TestManagerFromCoordinator_CorruptRecordFallsBackToIssue(t *testing.T) {
	ctx := context.Background()
	system := newRaceTestSystem(t)

	coordinator := NewMemoryCoordinator()
	const domain = "corrupt-coordinator.example.com"
	require.NoError(t, coordinator.Put(ctx, certCoordinatorKeyPrefix+domain, []byte("{not valid json"), 0))

	issuer := &internalFakeIssuer{ttl: time.Hour}
	manager := NewManager(system, log.DiscardLogger,
		WithCoordinator(coordinator),
		WithCertIssuer(issuer),
		WithAllowedDomains(domain),
		WithRenewInterval(""),
		WithIssuanceLockTTL(10*time.Second),
	)

	var ensureErr error
	require.NotPanics(t, func() {
		_, ensureErr = manager.EnsureCertificate(ctx, domain)
	})
	require.NoError(t, ensureErr)
	require.Equal(t, 1, issuer.callCount(), "the corrupt coordinator record must be discarded and re-issued, exactly once")
}

// staticReturnStore is a CertStore that always returns one preconfigured (cert, err) from
// Get, so a test can hand Manager a store that returns nil, a certificate for the wrong
// domain, or one whose self-reported expiry disagrees with its leaf. Put/Delete are no-ops
// so a fresh re-issuance never overwrites the malicious record mid-test.
type staticReturnStore struct {
	cert *Certificate
	err  error
}

func (s *staticReturnStore) Get(context.Context, string) (*Certificate, error) {
	return s.cert, s.err
}
func (s *staticReturnStore) Put(context.Context, *Certificate) error { return nil }
func (s *staticReturnStore) Delete(context.Context, string) error    { return nil }

// mismatchedCert builds a Certificate whose self-reported Domain and NotAfter can be set
// independently of the leaf's actual SAN and expiry, so a test can simulate an adapter that
// lies about what it returns.
func mismatchedCert(reportedDomain, leafDomain string, reportedNotAfter, leafNotAfter time.Time) *Certificate {
	certPEM, keyPEM := generateInternalTestCertificate(leafDomain, leafNotAfter)
	return &Certificate{
		Domain:   reportedDomain,
		CertPEM:  certPEM,
		KeyPEM:   keyPEM,
		NotAfter: reportedNotAfter,
	}
}

// TestManagerRejectsUntrustedStoreCertificate is the P0 certificate trust-boundary
// regression: whatever a CertStore returns is treated as an untrusted claim, not a fact.
// A store returning nil, a certificate whose leaf does not cover the requested domain, a
// certificate whose self-reported Domain disagrees with the request, or one whose
// self-reported NotAfter disagrees with the leaf must all be discarded rather than served
// or cached, and Manager must re-issue instead of panicking, serving the wrong domain its
// certificate, or caching a fabricated expiry.
func TestManagerRejectsUntrustedStoreCertificate(t *testing.T) {
	ctx := context.Background()
	const domain = "good.example.com"

	cases := []struct {
		name  string
		store *staticReturnStore
	}{
		{
			// A store returning (nil, nil) must not panic on a nil dereference.
			name:  "nil certificate",
			store: &staticReturnStore{cert: nil, err: nil},
		},
		{
			// Self-reported Domain matches, but the leaf's SAN covers a different name:
			// serving it would hand good.example.com the certificate for evil.example.com.
			name: "leaf does not cover requested domain",
			store: &staticReturnStore{cert: mismatchedCert(
				domain, "evil.example.com",
				time.Now().Add(365*24*time.Hour), time.Now().Add(365*24*time.Hour),
			)},
		},
		{
			// The leaf covers the requested name, but the adapter reports it under a
			// different Domain: caching it would key material under the wrong SNI.
			name: "self-reported domain disagrees with request",
			store: &staticReturnStore{cert: mismatchedCert(
				"evil.example.com", domain,
				time.Now().Add(365*24*time.Hour), time.Now().Add(365*24*time.Hour),
			)},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			system := newRaceTestSystem(t)
			issuer := &internalFakeIssuer{ttl: time.Hour}
			manager := NewManager(system, log.DiscardLogger,
				WithCertStore(tc.store),
				WithCertIssuer(issuer),
				WithAllowedDomains(domain),
				WithRenewBefore(time.Minute),
				WithRenewInterval(""),
				WithIssuanceLockTTL(10*time.Second),
			)

			var cert *tls.Certificate
			var err error
			require.NotPanics(t, func() {
				cert, err = manager.EnsureCertificate(ctx, domain)
			})
			require.NoError(t, err)
			require.NotNil(t, cert)
			require.Equal(t, 1, issuer.callCount(), "the untrusted stored certificate must be discarded and re-issued exactly once")

			// The served certificate is the freshly issued one that actually covers the
			// requested domain, never the store's material.
			require.NotNil(t, cert.Leaf)
			require.NoError(t, cert.Leaf.VerifyHostname(domain), "the served certificate must cover the requested domain")

			// The hot cache holds the fresh certificate keyed by the requested domain, and
			// nothing was cached under the attacker-reported name.
			cached, ok := manager.certs.get(domain)
			require.True(t, ok)
			require.NoError(t, cached.tlsCert.Leaf.VerifyHostname(domain))
			_, evilCached := manager.certs.get("evil.example.com")
			require.False(t, evilCached, "no material may be cached under the attacker-reported domain")
		})
	}
}

// TestManagerStoreExpiryLieIsIgnored pins that only the leaf's own NotAfter is trusted: a
// store that reports a far-future expiry for a certificate whose leaf actually expires
// imminently must not have that lie accepted. Because the leaf's real expiry is within
// renewBefore, the certificate is refused as too-soon-to-expire and re-issued, and the hot
// cache records the fresh certificate's real expiry rather than the fabricated one.
func TestManagerStoreExpiryLieIsIgnored(t *testing.T) {
	ctx := context.Background()
	const domain = "expiry-lie.example.com"

	// Leaf really expires in 10 minutes; the store lies and claims a year.
	store := &staticReturnStore{cert: mismatchedCert(
		domain, domain,
		time.Now().Add(365*24*time.Hour), time.Now().Add(10*time.Minute),
	)}

	system := newRaceTestSystem(t)
	issuer := &internalFakeIssuer{ttl: time.Hour}
	manager := NewManager(system, log.DiscardLogger,
		WithCertStore(store),
		WithCertIssuer(issuer),
		WithAllowedDomains(domain),
		// Between the leaf's real 10m expiry and the fresh certificate's 1h lifetime, so the
		// stored certificate reads as due-for-renewal and the fresh one reads as valid.
		WithRenewBefore(30*time.Minute),
		WithRenewInterval(""),
		WithIssuanceLockTTL(10*time.Second),
	)

	cert, err := manager.EnsureCertificate(ctx, domain)
	require.NoError(t, err)
	require.NotNil(t, cert)
	require.Equal(t, 1, issuer.callCount(), "a certificate whose real leaf expiry is imminent must be re-issued, not served on a fabricated expiry")

	cached, ok := manager.certs.get(domain)
	require.True(t, ok)
	require.Less(t, time.Until(cached.notAfter), 2*time.Hour, "the cached expiry must be the leaf's real value, not the store's year-long lie")
	require.Greater(t, time.Until(cached.notAfter), 30*time.Minute, "the cached certificate is the freshly issued one")
}

// TestManagerRejectsUntrustedCoordinatorCertificate is the fromCoordinator half of the
// certificate trust-boundary regression: a coordinator record whose leaf does not cover the
// requested domain (corruption, a tampered shared store, or a version skew) must be
// discarded rather than served, cached, or written back to the local store, and Manager
// must re-issue.
func TestManagerRejectsUntrustedCoordinatorCertificate(t *testing.T) {
	ctx := context.Background()
	system := newRaceTestSystem(t)

	coordinator := NewMemoryCoordinator()
	const domain = "coordinator-mismatch.example.com"

	// A record that claims the requested domain but whose leaf covers a different name.
	record, err := encodeCertRecord(mismatchedCert(
		domain, "evil.example.com",
		time.Now().Add(365*24*time.Hour), time.Now().Add(365*24*time.Hour),
	))
	require.NoError(t, err)
	require.NoError(t, coordinator.Put(ctx, certCoordinatorKeyPrefix+domain, record, 0))

	store := NewMemoryCertStore()
	issuer := &internalFakeIssuer{ttl: time.Hour}
	manager := NewManager(system, log.DiscardLogger,
		WithCoordinator(coordinator),
		WithCertStore(store),
		WithCertIssuer(issuer),
		WithAllowedDomains(domain),
		WithRenewBefore(time.Minute),
		WithRenewInterval(""),
		WithIssuanceLockTTL(10*time.Second),
	)

	var cert *tls.Certificate
	require.NotPanics(t, func() {
		cert, err = manager.EnsureCertificate(ctx, domain)
	})
	require.NoError(t, err)
	require.NotNil(t, cert)
	require.Equal(t, 1, issuer.callCount(), "the mismatched coordinator record must be discarded and re-issued exactly once")
	require.NoError(t, cert.Leaf.VerifyHostname(domain))

	// The mismatched record must not have been written back into the local store.
	stored, getErr := store.Get(ctx, "evil.example.com")
	require.ErrorIs(t, getErr, ErrCertificateNotFound)
	require.Nil(t, stored)
}

// TestManagerCoordinatorArbitratesAcrossManagers verifies the actual point of the
// Coordinator abstraction: issuance arbitration is scoped to the shared Coordinator, not
// to any single ActorSystem/cluster. Two independent Manager instances (each with its
// own actor system, as two separate processes would have) racing EnsureCertificate for
// the same domain against ONE shared Coordinator must still call the underlying
// CertIssuer exactly once and converge on the same certificate.
func TestManagerCoordinatorArbitratesAcrossManagers(t *testing.T) {
	ctx := context.Background()
	coordinator := NewMemoryCoordinator()
	issuer := &internalFakeIssuer{ttl: time.Hour}

	const domain = "shared-coordinator.example.com"
	const managerCount = 5

	managers := make([]*Manager, managerCount)
	for i := range managers {
		system := newRaceTestSystem(t)
		managers[i] = NewManager(system, log.DiscardLogger,
			WithCoordinator(coordinator),
			WithCertIssuer(issuer),
			WithAllowedDomains(domain),
			WithRenewInterval(""),
			WithIssuanceLockTTL(10*time.Second),
			// well under the issuer's 1h ttl, so a freshly issued cert always reads
			// back as fresh enough for a waiting manager to accept.
			WithRenewBefore(time.Minute),
		)
	}

	var wg sync.WaitGroup
	certs := make([][]byte, managerCount)
	errs := make([]error, managerCount)
	for i, manager := range managers {
		wg.Go(func() {
			cert, err := manager.EnsureCertificate(ctx, domain)
			errs[i] = err
			if err == nil && len(cert.Certificate) > 0 {
				certs[i] = cert.Certificate[0]
			}
		})
	}
	wg.Wait()

	for _, err := range errs {
		require.NoError(t, err)
	}
	require.Equal(t, 1, issuer.callCount(), "exactly one manager must call the shared issuer")

	for i := 1; i < managerCount; i++ {
		require.NotEmpty(t, certs[i])
		require.Equal(t, certs[0], certs[i], "every manager must serve the same certificate bytes")
	}
}
