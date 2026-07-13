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
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/tochemey/goakt/v4/log"

	gateway "github.com/StringKe/goakt-gateway"
)

func TestFileCertStoreRoundTrip(t *testing.T) {
	dir := t.TempDir()
	store, err := gateway.NewFileCertStore(dir)
	require.NoError(t, err)

	ctx := context.Background()
	notAfter := time.Now().Add(72 * time.Hour).Truncate(time.Second).UTC()
	certPEM, keyPEM := generateTestCertificate("file.example.com", notAfter)

	require.NoError(t, store.Put(ctx, &gateway.Certificate{
		Domain:   "file.example.com",
		CertPEM:  certPEM,
		KeyPEM:   keyPEM,
		NotAfter: notAfter,
	}))

	// the on-disk layout is a plain PEM pair other tooling can read/produce.
	require.FileExists(t, filepath.Join(dir, "file.example.com.crt"))
	require.FileExists(t, filepath.Join(dir, "file.example.com.key"))

	got, err := store.Get(ctx, "file.example.com")
	require.NoError(t, err)
	require.Equal(t, certPEM, got.CertPEM)
	require.Equal(t, keyPEM, got.KeyPEM)
	// NotAfter is not persisted separately, it is read back from the leaf certificate.
	require.WithinDuration(t, notAfter, got.NotAfter, time.Second)

	require.NoError(t, store.Delete(ctx, "file.example.com"))
	_, err = store.Get(ctx, "file.example.com")
	require.ErrorIs(t, err, gateway.ErrCertificateNotFound)

	// Delete is idempotent.
	require.NoError(t, store.Delete(ctx, "file.example.com"))
}

func TestFileCertStoreGetMissing(t *testing.T) {
	store, err := gateway.NewFileCertStore(t.TempDir())
	require.NoError(t, err)

	_, err = store.Get(context.Background(), "absent.example.com")
	require.ErrorIs(t, err, gateway.ErrCertificateNotFound)
}

func TestMemoryCertStoreOwnsCertificateMaterial(t *testing.T) {
	store := gateway.NewMemoryCertStore()
	original := &gateway.Certificate{
		Domain:   "owned.example.com",
		CertPEM:  []byte("certificate"),
		KeyPEM:   []byte("private-key"),
		NotAfter: time.Now().Add(time.Hour),
	}
	require.NoError(t, store.Put(context.Background(), original))

	original.CertPEM[0] = 'X'
	original.KeyPEM[0] = 'X'
	first, err := store.Get(context.Background(), original.Domain)
	require.NoError(t, err)
	require.Equal(t, []byte("certificate"), first.CertPEM)
	require.Equal(t, []byte("private-key"), first.KeyPEM)

	first.CertPEM[0] = 'Y'
	first.KeyPEM[0] = 'Y'
	second, err := store.Get(context.Background(), original.Domain)
	require.NoError(t, err)
	require.Equal(t, []byte("certificate"), second.CertPEM)
	require.Equal(t, []byte("private-key"), second.KeyPEM)
}

// TestFileCertStoreRejectsUnsafeDomains verifies that a domain name (which ultimately comes
// from the peer-supplied SNI server name) cannot be used to read or write a file outside the
// store directory.
func TestFileCertStoreRejectsUnsafeDomains(t *testing.T) {
	dir := t.TempDir()
	store, err := gateway.NewFileCertStore(dir)
	require.NoError(t, err)

	ctx := context.Background()
	for _, domain := range []string{
		"",
		"..",
		"../evil",
		"../../etc/passwd",
		"sub/dir.example.com",
		`back\slash.example.com`,
		"a..b.example.com",
		"space domain.example.com",
	} {
		t.Run(domain, func(t *testing.T) {
			_, err := store.Get(ctx, domain)
			require.Error(t, err)
			require.NotErrorIs(t, err, gateway.ErrCertificateNotFound)

			err = store.Put(ctx, &gateway.Certificate{Domain: domain})
			require.Error(t, err)

			err = store.Delete(ctx, domain)
			require.Error(t, err)
		})
	}
}

// TestManagerWithFileCertStore verifies the store plugs into Manager: a certificate written
// to the directory before the Manager ever runs is served without calling the issuer at all,
// which is the cold-start-without-the-CA property a persistent store exists for.
func TestManagerWithFileCertStore(t *testing.T) {
	dir := t.TempDir()
	store, err := gateway.NewFileCertStore(dir)
	require.NoError(t, err)

	ctx := context.Background()
	notAfter := time.Now().Add(365 * 24 * time.Hour)
	certPEM, keyPEM := generateTestCertificate("warm.example.com", notAfter)
	require.NoError(t, store.Put(ctx, &gateway.Certificate{
		Domain:   "warm.example.com",
		CertPEM:  certPEM,
		KeyPEM:   keyPEM,
		NotAfter: notAfter,
	}))

	issuer := &fakeIssuer{ttl: time.Hour}
	manager := gateway.NewManager(newTestSystem(t), log.DiscardLogger,
		gateway.WithCertIssuer(issuer),
		gateway.WithCertStore(store),
		gateway.WithAllowedDomains("warm.example.com"),
		gateway.WithRenewBefore(time.Hour),
		gateway.WithRenewInterval(""),
	)

	cert, err := manager.EnsureCertificate(ctx, "warm.example.com")
	require.NoError(t, err)
	require.NotNil(t, cert)
	require.Zero(t, issuer.calls.Load(), "a certificate already in the store must not be re-issued")
}

// writeCertPair writes a freshly generated certificate/key pair for domain into certPath and
// keyPath, returning nothing: tests assert on the served leaf's common name, which is domain.
func writeCertPair(t *testing.T, certPath, keyPath, domain string) {
	t.Helper()
	certPEM, keyPEM := generateTestCertificate(domain, time.Now().Add(24*time.Hour))
	require.NoError(t, os.WriteFile(certPath, certPEM, 0o644))
	require.NoError(t, os.WriteFile(keyPath, keyPEM, 0o600))
}

func TestNewReloadingCertificateMissingFiles(t *testing.T) {
	dir := t.TempDir()
	_, err := gateway.NewReloadingCertificate(
		filepath.Join(dir, "tls.crt"), filepath.Join(dir, "tls.key"), time.Second, log.DiscardLogger)
	require.Error(t, err, "there is nothing to serve if the initial load fails")
}

// TestReloadingCertificateInPlaceRewrite covers the simplest rotation shape: the certificate
// and key files are overwritten in place (a bare-metal/systemd deployment, or a test).
func TestReloadingCertificateInPlaceRewrite(t *testing.T) {
	dir := t.TempDir()
	certPath := filepath.Join(dir, "tls.crt")
	keyPath := filepath.Join(dir, "tls.key")
	writeCertPair(t, certPath, keyPath, "old.example.com")

	reloader, err := gateway.NewReloadingCertificate(certPath, keyPath, 20*time.Millisecond, log.DiscardLogger)
	require.NoError(t, err)

	cert, err := reloader.Get()
	require.NoError(t, err)
	require.Equal(t, "old.example.com", cert.Leaf.Subject.CommonName)

	reloader.Start(context.Background())
	t.Cleanup(reloader.Stop)

	writeCertPair(t, certPath, keyPath, "new.example.com")

	require.Eventually(t, func() bool {
		cert, err := reloader.Get()
		return err == nil && cert.Leaf.Subject.CommonName == "new.example.com"
	}, 5*time.Second, 20*time.Millisecond, "the rotated certificate must be picked up by the poller")
}

// TestReloadingCertificateKubernetesSecretSwap reproduces the layout a Kubernetes secret
// volume actually has - tls.crt is a symlink into "..data", itself a symlink to a timestamped
// directory that kubelet atomically re-points on rotation. Nothing is ever written to the
// paths the process holds, so a reloader that only stats mtime on tls.crt never notices.
func TestReloadingCertificateKubernetesSecretSwap(t *testing.T) {
	dir := t.TempDir()

	// initial version: ..2026_01_01/{tls.crt,tls.key}, ..data -> ..2026_01_01,
	// tls.crt -> ..data/tls.crt, tls.key -> ..data/tls.key
	first := filepath.Join(dir, "..2026_01_01")
	require.NoError(t, os.Mkdir(first, 0o700))
	writeCertPair(t, filepath.Join(first, "tls.crt"), filepath.Join(first, "tls.key"), "old.example.com")
	require.NoError(t, os.Symlink("..2026_01_01", filepath.Join(dir, "..data")))

	certPath := filepath.Join(dir, "tls.crt")
	keyPath := filepath.Join(dir, "tls.key")
	require.NoError(t, os.Symlink(filepath.Join("..data", "tls.crt"), certPath))
	require.NoError(t, os.Symlink(filepath.Join("..data", "tls.key"), keyPath))

	reloader, err := gateway.NewReloadingCertificate(certPath, keyPath, 20*time.Millisecond, log.DiscardLogger)
	require.NoError(t, err)
	cert, err := reloader.Get()
	require.NoError(t, err)
	require.Equal(t, "old.example.com", cert.Leaf.Subject.CommonName)

	reloader.Start(context.Background())
	t.Cleanup(reloader.Stop)

	// rotation: write the new version into a new directory and atomically re-point "..data"
	// at it by renaming a temporary symlink over it, exactly as kubelet does.
	second := filepath.Join(dir, "..2026_01_02")
	require.NoError(t, os.Mkdir(second, 0o700))
	writeCertPair(t, filepath.Join(second, "tls.crt"), filepath.Join(second, "tls.key"), "rotated.example.com")

	tmpLink := filepath.Join(dir, "..data_tmp")
	require.NoError(t, os.Symlink("..2026_01_02", tmpLink))
	require.NoError(t, os.Rename(tmpLink, filepath.Join(dir, "..data")))

	require.Eventually(t, func() bool {
		cert, err := reloader.Get()
		return err == nil && cert.Leaf.Subject.CommonName == "rotated.example.com"
	}, 5*time.Second, 20*time.Millisecond, "an atomic ..data symlink swap must be detected by content, not by mtime")
}

// TestReloadingCertificateCorruptFileKeepsPrevious verifies that a botched rotation does not
// take TLS termination down: the last certificate that loaded successfully keeps being
// served, and a later, valid rotation still converges.
func TestReloadingCertificateCorruptFileKeepsPrevious(t *testing.T) {
	dir := t.TempDir()
	certPath := filepath.Join(dir, "tls.crt")
	keyPath := filepath.Join(dir, "tls.key")
	writeCertPair(t, certPath, keyPath, "good.example.com")

	reloader, err := gateway.NewReloadingCertificate(certPath, keyPath, 20*time.Millisecond, log.DiscardLogger)
	require.NoError(t, err)
	reloader.Start(context.Background())
	t.Cleanup(reloader.Stop)

	require.NoError(t, os.WriteFile(certPath, []byte("-----BEGIN CERTIFICATE-----\ngarbage\n"), 0o644))

	require.Never(t, func() bool {
		cert, err := reloader.Get()
		return err != nil || cert.Leaf.Subject.CommonName != "good.example.com"
	}, time.Second, 50*time.Millisecond, "a corrupt certificate file must never displace the working one")

	writeCertPair(t, certPath, keyPath, "recovered.example.com")
	require.Eventually(t, func() bool {
		cert, err := reloader.Get()
		return err == nil && cert.Leaf.Subject.CommonName == "recovered.example.com"
	}, 5*time.Second, 20*time.Millisecond, "the reloader must recover once a valid pair is written again")
}

// TestReloadingCertificateStopIsIdempotent guards the polling goroutine's lifecycle: Stop
// before Start, and a double Stop, must not panic or block.
func TestReloadingCertificateStopIsIdempotent(t *testing.T) {
	dir := t.TempDir()
	certPath := filepath.Join(dir, "tls.crt")
	keyPath := filepath.Join(dir, "tls.key")
	writeCertPair(t, certPath, keyPath, "lifecycle.example.com")

	reloader, err := gateway.NewReloadingCertificate(certPath, keyPath, 20*time.Millisecond, log.DiscardLogger)
	require.NoError(t, err)

	reloader.Stop()
	reloader.Start(context.Background())
	reloader.Start(context.Background())
	reloader.Stop()
	reloader.Stop()
}
