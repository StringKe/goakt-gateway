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
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
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

	manager := NewManager(system, log.DiscardLogger,
		WithCoordinator(coordinator),
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
		wg.Add(1)
		go func(i int, manager *Manager) {
			defer wg.Done()
			cert, err := manager.EnsureCertificate(ctx, domain)
			errs[i] = err
			if err == nil && len(cert.Certificate) > 0 {
				certs[i] = cert.Certificate[0]
			}
		}(i, manager)
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
