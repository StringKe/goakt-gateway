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
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/tochemey/goakt/v4/log"

	gateway "github.com/StringKe/goakt-gateway"
)

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

	_, err = manager.EnsureCertificate(context.Background(), "allowed.example.com")
	require.NoError(t, err)
}

func TestManagerNoIssuerConfigured(t *testing.T) {
	system := newTestSystem(t)
	manager := gateway.NewManager(system, log.DiscardLogger, gateway.WithRenewInterval(""))

	_, err := manager.EnsureCertificate(context.Background(), "example.com")
	require.ErrorIs(t, err, gateway.ErrNoIssuer)
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
		gateway.WithRenewInterval(""),
	)

	const concurrency = 20
	var wg sync.WaitGroup
	errs := make([]error, concurrency)
	for i := range concurrency {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, errs[i] = manager.EnsureCertificate(context.Background(), "concurrent.example.com")
		}(i)
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
		// Far from expiry so the fromStore expiry check passes and control actually
		// reaches parseCertificate - the branch under test.
		NotAfter: time.Now().Add(365 * 24 * time.Hour),
	}))

	issuer := &fakeIssuer{ttl: time.Hour}
	manager := gateway.NewManager(system, log.DiscardLogger,
		gateway.WithCertIssuer(issuer),
		gateway.WithCertStore(store),
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
