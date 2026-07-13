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

// Package conformance is a shared test suite every gateway.CertStore implementation must
// pass, so gateway.MemoryCertStore, gateway.FileCertStore and store/redis.Store (and any
// third-party implementation) are held to the exact same contract.
package conformance

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	gateway "github.com/StringKe/goakt-gateway"
)

// Run exercises newStore against the gateway.CertStore contract. newStore must return a
// fresh, empty CertStore; Run calls it once per subtest so implementations backed by a
// shared external service (e.g. Redis) do not see state leak between subtests as long as
// newStore picks a fresh key namespace or database per call.
//
// The suite stores real self-signed certificates rather than arbitrary bytes: gateway
// .FileCertStore reads NotAfter back from the leaf certificate instead of storing it
// separately, so only a parseable certificate lets one suite validate all three backends.
// Test domains use only characters FileCertStore accepts (letters, digits, dot, dash),
// since it rejects anything that could escape its directory.
func Run(t *testing.T, newStore func() gateway.CertStore) {
	t.Helper()

	t.Run("Get on an absent domain returns ErrCertificateNotFound", func(t *testing.T) {
		s := newStore()
		cert, err := s.Get(context.Background(), "absent.example.com")
		require.ErrorIs(t, err, gateway.ErrCertificateNotFound)
		require.Nil(t, cert)
	})

	t.Run("Put then Get round-trips every field including NotAfter to the second", func(t *testing.T) {
		s := newStore()
		ctx := context.Background()
		// A non-round wall-clock time confirms the stored NotAfter survives a second-level
		// round-trip (x509 encodes validity to the second) rather than being dropped.
		notAfter := time.Date(2027, 3, 14, 15, 9, 26, 0, time.UTC)
		want := newTestCert(t, "a.example.com", notAfter)
		require.NoError(t, s.Put(ctx, want))

		got, err := s.Get(ctx, "a.example.com")
		require.NoError(t, err)
		require.Equal(t, want.Domain, got.Domain)
		require.Equal(t, want.CertPEM, got.CertPEM)
		require.Equal(t, want.KeyPEM, got.KeyPEM)
		require.True(t, want.NotAfter.Equal(got.NotAfter), "NotAfter must round-trip: want %s, got %s", want.NotAfter, got.NotAfter)
	})

	t.Run("Put overwrites a previous certificate for the same domain", func(t *testing.T) {
		s := newStore()
		ctx := context.Background()
		first := newTestCert(t, "a.example.com", time.Date(2027, 1, 1, 0, 0, 0, 0, time.UTC))
		second := newTestCert(t, "a.example.com", time.Date(2028, 1, 1, 0, 0, 0, 0, time.UTC))
		require.NoError(t, s.Put(ctx, first))
		require.NoError(t, s.Put(ctx, second))

		got, err := s.Get(ctx, "a.example.com")
		require.NoError(t, err)
		require.Equal(t, second.CertPEM, got.CertPEM)
		require.Equal(t, second.KeyPEM, got.KeyPEM)
	})

	t.Run("Delete removes a stored certificate", func(t *testing.T) {
		s := newStore()
		ctx := context.Background()
		require.NoError(t, s.Put(ctx, newTestCert(t, "a.example.com", defaultNotAfter)))
		require.NoError(t, s.Delete(ctx, "a.example.com"))

		_, err := s.Get(ctx, "a.example.com")
		require.ErrorIs(t, err, gateway.ErrCertificateNotFound)
	})

	t.Run("Delete on an absent domain is not an error", func(t *testing.T) {
		s := newStore()
		require.NoError(t, s.Delete(context.Background(), "absent.example.com"))
	})

	t.Run("distinct domains do not interfere", func(t *testing.T) {
		s := newStore()
		ctx := context.Background()
		certA := newTestCert(t, "a.example.com", defaultNotAfter)
		certB := newTestCert(t, "b.example.com", defaultNotAfter)
		require.NoError(t, s.Put(ctx, certA))
		require.NoError(t, s.Put(ctx, certB))

		gotA, err := s.Get(ctx, "a.example.com")
		require.NoError(t, err)
		require.Equal(t, certA.CertPEM, gotA.CertPEM)
		gotB, err := s.Get(ctx, "b.example.com")
		require.NoError(t, err)
		require.Equal(t, certB.CertPEM, gotB.CertPEM)

		require.NoError(t, s.Delete(ctx, "a.example.com"))
		_, err = s.Get(ctx, "a.example.com")
		require.ErrorIs(t, err, gateway.ErrCertificateNotFound)
		_, err = s.Get(ctx, "b.example.com")
		require.NoError(t, err, "deleting one domain must not affect another")
	})

	t.Run("concurrent Put and Get on distinct domains are safe", func(t *testing.T) {
		s := newStore()
		ctx := context.Background()

		const concurrency = 20
		certs := make([]*gateway.Certificate, concurrency)
		for i := range certs {
			certs[i] = newTestCert(t, fmt.Sprintf("host-%d.example.com", i), defaultNotAfter)
		}

		var wg sync.WaitGroup
		for i := range concurrency {
			wg.Go(func() {
				require.NoError(t, s.Put(ctx, certs[i]))
				got, err := s.Get(ctx, certs[i].Domain)
				require.NoError(t, err)
				require.Equal(t, certs[i].Domain, got.Domain)
			})
		}
		wg.Wait()

		for i := range concurrency {
			got, err := s.Get(ctx, certs[i].Domain)
			require.NoError(t, err)
			require.Equal(t, certs[i].CertPEM, got.CertPEM)
		}
	})

	t.Run("concurrent Put and Get on one domain never tear the cert/key pair", func(t *testing.T) {
		// A renewal Puts a new cert/key pair for a domain a TLS handshake may Get at the same
		// instant. The swap must be all-or-nothing: a Get must never return one version's
		// certificate with another version's key, which tls.X509KeyPair rejects with "private
		// key does not match public key". MemoryCertStore (one map write) and store/redis (one
		// SET of a single value) cannot tear across the pair; this holds every implementation,
		// including the two-file disk-backed one, to that same bar - the axis the distinct-domain
		// concurrency subtest above cannot exercise.
		s := newStore()
		ctx := context.Background()
		const domain = "renew.example.com"

		// Seed the domain first so readers never race the very first creation, when one of the
		// two files legitimately does not exist yet - a not-found is a different state this
		// subtest is not about.
		require.NoError(t, s.Put(ctx, newTestCert(t, domain, defaultNotAfter)))

		const renewals = 50
		versions := make([]*gateway.Certificate, renewals)
		for i := range versions {
			versions[i] = newTestCert(t, domain, defaultNotAfter)
		}

		stop := make(chan struct{})
		var wg sync.WaitGroup

		wg.Go(func() {
			defer close(stop)
			for _, v := range versions {
				if err := s.Put(ctx, v); err != nil {
					t.Errorf("Put during renewal failed: %v", err)
					return
				}
			}
		})

		const readers = 8
		for range readers {
			wg.Go(func() {
				for {
					select {
					case <-stop:
						return
					default:
					}
					got, err := s.Get(ctx, domain)
					if err != nil {
						// t.Errorf (not require) so a torn read fails cleanly from this
						// non-test goroutine instead of calling FailNow off the test goroutine.
						t.Errorf("Get during renewal failed: %v", err)
						return
					}
					if _, err := tls.X509KeyPair(got.CertPEM, got.KeyPEM); err != nil {
						t.Errorf("Get returned a cert that does not match its key: %v", err)
						return
					}
				}
			})
		}
		wg.Wait()
	})
}

// defaultNotAfter is the expiry used where the exact value does not matter to the assertion.
var defaultNotAfter = time.Date(2027, 1, 1, 0, 0, 0, 0, time.UTC)

// newTestCert generates a real self-signed ECDSA certificate for domain expiring at
// notAfter, returned as a gateway.Certificate. A real certificate is required because
// gateway.FileCertStore recovers NotAfter by parsing the stored leaf, so fabricated PEM
// bytes would fail to round-trip through the disk-backed implementation.
func newTestCert(t *testing.T, domain string, notAfter time.Time) *gateway.Certificate {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)

	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	require.NoError(t, err)

	tmpl := &x509.Certificate{
		SerialNumber: serial,
		Subject:      pkix.Name{CommonName: domain},
		DNSNames:     []string{domain},
		NotBefore:    notAfter.Add(-24 * time.Hour),
		NotAfter:     notAfter,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	require.NoError(t, err)

	keyDER, err := x509.MarshalECPrivateKey(key)
	require.NoError(t, err)

	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})

	return &gateway.Certificate{
		Domain:   domain,
		CertPEM:  certPEM,
		KeyPEM:   keyPEM,
		NotAfter: notAfter,
	}
}
