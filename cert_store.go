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

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/tochemey/goakt/v4/log"
)

// CertStore persists issued certificates so that a cold start does not have to re-issue
// one for every hosted domain. Manager always keeps a copy in the configured Coordinator
// for fast, shared SNI lookups; a CertStore is the layer behind that - by default an
// in-memory MemoryCertStore, but applications facing a full restart of every node should
// plug in a persistent implementation (e.g. backed by a database or object storage) to
// avoid depending on the issuer being reachable/willing to re-issue on every cold start.
//
// Implementations must be safe for concurrent use.
type CertStore interface {
	// Get returns the certificate stored for domain. It returns ErrCertificateNotFound
	// if none is stored.
	Get(ctx context.Context, domain string) (*Certificate, error)
	// Put stores cert, overwriting any previous certificate for the same domain.
	Put(ctx context.Context, cert *Certificate) error
	// Delete removes the certificate stored for domain, if any.
	Delete(ctx context.Context, domain string) error
}

// MemoryCertStore is the default, in-process CertStore. It does not survive a process
// restart, which is why Manager also always keeps a copy in the configured Coordinator:
// on a single node restart the certificate is fetched back from there, and on a full
// cold start (no Coordinator, or a fresh one) it is re-issued.
type MemoryCertStore struct {
	mu    sync.RWMutex
	certs map[string]*Certificate
}

// enforce compilation error
var _ CertStore = (*MemoryCertStore)(nil)

// NewMemoryCertStore creates an empty MemoryCertStore.
func NewMemoryCertStore() *MemoryCertStore {
	return &MemoryCertStore{certs: make(map[string]*Certificate)}
}

// Get implements CertStore.
func (m *MemoryCertStore) Get(_ context.Context, domain string) (*Certificate, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	cert, ok := m.certs[domain]
	if !ok {
		return nil, ErrCertificateNotFound
	}
	return cloneCertificate(cert), nil
}

// Put implements CertStore.
func (m *MemoryCertStore) Put(_ context.Context, cert *Certificate) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if cert == nil {
		return fmt.Errorf("gateway: certificate is required")
	}
	m.certs[cert.Domain] = cloneCertificate(cert)
	return nil
}

// Delete implements CertStore.
func (m *MemoryCertStore) Delete(_ context.Context, domain string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.certs, domain)
	return nil
}

// FileCertStore is a CertStore backed by a directory on disk: the certificate for a domain
// lives in <dir>/<domain>.crt and its private key in <dir>/<domain>.key. It survives a
// process restart, so a node that comes back up with a warm volume (or a persistent
// container filesystem) serves its domains without depending on the issuer or the
// Coordinator being reachable.
//
// Certificate.NotAfter is not stored separately: it is read back from the leaf certificate,
// which keeps the on-disk format a plain PEM pair that other tooling (openssl, a k8s secret)
// can produce and consume.
type FileCertStore struct {
	dir string
}

// enforce compilation error
var _ CertStore = (*FileCertStore)(nil)

// NewFileCertStore creates a FileCertStore rooted at dir, creating the directory if it does
// not exist. The directory holds private keys, so it is created with 0700 permissions.
func NewFileCertStore(dir string) (*FileCertStore, error) {
	if dir == "" {
		return nil, fmt.Errorf("gateway: file certificate store directory is required")
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, fmt.Errorf("gateway: failed to create certificate store directory %q: %w", dir, err)
	}
	return &FileCertStore{dir: dir}, nil
}

// Get implements CertStore.
//
// Put renames the .crt file into place and then the .key file, two operations that are each
// atomic but not atomic together, so a Get racing a renewal can briefly observe the new
// certificate beside the not-yet-replaced old key (or the reverse). To keep the pair swap
// all-or-nothing to callers - the way MemoryCertStore's map write and store/redis's single-key
// SET already are - Get re-reads on a cert/key mismatch a bounded number of times; a torn pair
// is transient and the retry sees both files settled. A pair that stays mismatched across every
// retry is a genuinely inconsistent on-disk state (e.g. a pair an external tool wrote wrong),
// which is returned as read and left for the TLS layer to reject rather than looped on forever.
func (f *FileCertStore) Get(_ context.Context, domain string) (*Certificate, error) {
	certPath, keyPath, err := f.paths(domain)
	if err != nil {
		return nil, err
	}

	const maxTornPairRetries = 8
	var certPEM, keyPEM []byte
	for attempt := 0; ; attempt++ {
		certPEM, err = os.ReadFile(certPath)
		if err != nil {
			if os.IsNotExist(err) {
				return nil, ErrCertificateNotFound
			}
			return nil, fmt.Errorf("gateway: failed to read certificate for %q: %w", domain, err)
		}
		keyPEM, err = os.ReadFile(keyPath)
		if err != nil {
			if os.IsNotExist(err) {
				return nil, ErrCertificateNotFound
			}
			return nil, fmt.Errorf("gateway: failed to read private key for %q: %w", domain, err)
		}

		if _, pairErr := tls.X509KeyPair(certPEM, keyPEM); pairErr == nil || attempt >= maxTornPairRetries {
			break
		}
	}

	notAfter, err := leafNotAfter(certPEM)
	if err != nil {
		return nil, fmt.Errorf("gateway: stored certificate for %q is invalid: %w", domain, err)
	}

	return &Certificate{
		Domain:   domain,
		CertPEM:  certPEM,
		KeyPEM:   keyPEM,
		NotAfter: notAfter,
	}, nil
}

// Put implements CertStore. Both files are written to a temporary name and renamed into
// place so a concurrent Get never observes a half-written PEM file.
//
// Both temporary files are written and fsync'd before either is renamed, and the two renames
// then run back to back. This keeps the window in which a concurrent Get can see the new
// certificate beside the still-old key down to the gap between two adjacent rename syscalls -
// no fsync in between - which Get's bounded re-read reliably outlasts. Renaming the cert and
// only then starting to write and fsync the key temp would instead hold that mismatched pair
// on disk for as long as the key's fsync takes (milliseconds), far too long for Get to hide.
func (f *FileCertStore) Put(_ context.Context, cert *Certificate) error {
	certPath, keyPath, err := f.paths(cert.Domain)
	if err != nil {
		return err
	}

	tmpCert, err := writeTempFile(certPath, cert.CertPEM, 0o644)
	if err != nil {
		return fmt.Errorf("gateway: failed to write certificate for %q: %w", cert.Domain, err)
	}
	defer func() { _ = os.Remove(tmpCert) }()

	tmpKey, err := writeTempFile(keyPath, cert.KeyPEM, 0o600)
	if err != nil {
		return fmt.Errorf("gateway: failed to write private key for %q: %w", cert.Domain, err)
	}
	defer func() { _ = os.Remove(tmpKey) }()

	if err := os.Rename(tmpCert, certPath); err != nil {
		return fmt.Errorf("gateway: failed to write certificate for %q: %w", cert.Domain, err)
	}
	if err := os.Rename(tmpKey, keyPath); err != nil {
		return fmt.Errorf("gateway: failed to write private key for %q: %w", cert.Domain, err)
	}
	return nil
}

// Delete implements CertStore.
func (f *FileCertStore) Delete(_ context.Context, domain string) error {
	certPath, keyPath, err := f.paths(domain)
	if err != nil {
		return err
	}
	if err := os.Remove(certPath); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("gateway: failed to delete certificate for %q: %w", domain, err)
	}
	if err := os.Remove(keyPath); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("gateway: failed to delete private key for %q: %w", domain, err)
	}
	return nil
}

// paths maps domain to its certificate/key file paths. The domain reaching a Manager comes
// from the peer-supplied SNI server name, so it is validated rather than trusted: anything
// that could escape the store directory or address a different file than intended is
// rejected outright instead of being sanitized into some other domain's file name.
func (f *FileCertStore) paths(domain string) (certPath, keyPath string, err error) {
	if domain == "" {
		return "", "", fmt.Errorf("gateway: certificate domain is required")
	}
	if domain == "." || domain == ".." || strings.ContainsAny(domain, `/\`) || strings.Contains(domain, "..") {
		return "", "", fmt.Errorf("gateway: invalid certificate domain %q", domain)
	}
	for _, r := range domain {
		valid := (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') ||
			r == '.' || r == '-' || r == '_'
		if !valid {
			return "", "", fmt.Errorf("gateway: invalid certificate domain %q", domain)
		}
	}
	return filepath.Join(f.dir, domain+".crt"), filepath.Join(f.dir, domain+".key"), nil
}

// writeTempFile writes data to a fresh temporary file alongside path (same directory, so a
// later rename onto path stays within one filesystem and is atomic), with the given mode, and
// fsyncs it to disk. It returns the temporary file's name for the caller to rename into place;
// splitting the write from the rename is what lets Put fsync both files before renaming either.
// On any error the temporary file is removed and no name is returned.
func writeTempFile(path string, data []byte, perm os.FileMode) (string, error) {
	tmp, err := os.CreateTemp(filepath.Dir(path), "."+filepath.Base(path)+".tmp")
	if err != nil {
		return "", err
	}
	tmpName := tmp.Name()

	if err := func() error {
		if err := tmp.Chmod(perm); err != nil {
			return err
		}
		if _, err := tmp.Write(data); err != nil {
			return err
		}
		if err := tmp.Sync(); err != nil {
			return err
		}
		return tmp.Close()
	}(); err != nil {
		_ = tmp.Close()
		_ = os.Remove(tmpName)
		return "", err
	}
	return tmpName, nil
}

// leafNotAfter returns the expiry of the first certificate in a PEM chain.
func leafNotAfter(certPEM []byte) (time.Time, error) {
	for block, rest := pem.Decode(certPEM); block != nil; block, rest = pem.Decode(rest) {
		if block.Type != "CERTIFICATE" {
			continue
		}
		leaf, err := x509.ParseCertificate(block.Bytes)
		if err != nil {
			return time.Time{}, err
		}
		return leaf.NotAfter, nil
	}
	return time.Time{}, fmt.Errorf("gateway: no CERTIFICATE block found")
}

const (
	// certReloadRetryInterval/certReloadMaxRetries bound the retries ReloadingCertificate
	// performs on a failed load. A Kubernetes secret volume swaps its "..data" symlink
	// atomically, but tls.LoadX509KeyPair-style loading opens the certificate and the key
	// as two separate reads: they can straddle the swap and yield a new certificate paired
	// with the old key ("private key does not match public key"). Re-reading both files a
	// few times closes that window instead of serving a stale certificate until the next
	// poll.
	certReloadRetryInterval = 50 * time.Millisecond
	certReloadMaxRetries    = 5
)

// ReloadingCertificate serves a single certificate loaded from a PEM certificate/key file
// pair and keeps it up to date while the process runs, which is what a certificate mounted
// from a Kubernetes secret (rotated in place by kubelet, or by cert-manager) requires.
//
// Change detection hashes the file contents rather than comparing modification times: a
// Kubernetes secret volume is a symlink tree (tls.crt -> ..data/tls.crt) whose "..data"
// link is atomically re-pointed at a new directory, so the path a process holds resolves to
// a different inode without any write ever landing on the old one, and the mtime a naive
// watcher would stat is not the one that moved.
//
// A reload that fails (truncated write, mismatched pair, unreadable file) keeps the last
// certificate that did load: a botched rotation must not take TLS termination down with it.
//
// Pass Get to WithFallbackCertificate, or use it directly as tls.Config.GetCertificate via
// a closure, to serve the same catch-all certificate regardless of SNI.
type ReloadingCertificate struct {
	certPath string
	keyPath  string
	interval time.Duration
	logger   log.Logger

	current atomic.Pointer[tls.Certificate]
	// loaded is the sha256 of the certificate/key material currently served, used to skip
	// re-parsing on the (overwhelmingly common) poll that finds nothing changed.
	loaded atomic.Pointer[[sha256.Size]byte]

	mu     sync.Mutex
	cancel context.CancelFunc
	done   chan struct{}
}

// NewReloadingCertificate loads the certificate/key pair at certPath/keyPath and returns a
// ReloadingCertificate that re-checks them every interval once Start is called. It fails if
// the initial load fails, since there would be nothing to serve. An interval <= 0 disables
// polling: the certificate is loaded once and never refreshed.
func NewReloadingCertificate(certPath, keyPath string, interval time.Duration, logger log.Logger) (*ReloadingCertificate, error) {
	if logger == nil {
		logger = log.DiscardLogger
	}
	c := &ReloadingCertificate{
		certPath: certPath,
		keyPath:  keyPath,
		interval: interval,
		logger:   logger,
	}
	if _, err := c.reload(); err != nil {
		return nil, err
	}
	return c, nil
}

// Get returns the certificate currently loaded. It never blocks on the filesystem, so it is
// safe to call from a TLS handshake.
func (c *ReloadingCertificate) Get() (*tls.Certificate, error) {
	cert := c.current.Load()
	if cert == nil {
		return nil, ErrCertificateNotFound
	}
	return cert, nil
}

// Start begins polling the certificate/key files for changes until ctx is canceled or Stop
// is called. Calling it twice without an intervening Stop is a no-op.
func (c *ReloadingCertificate) Start(ctx context.Context) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.cancel != nil || c.interval <= 0 {
		return
	}

	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	c.cancel = cancel
	c.done = done
	go c.poll(runCtx, done)
}

// Stop stops the polling goroutine started by Start and waits for it to exit. It is
// idempotent.
func (c *ReloadingCertificate) Stop() {
	c.mu.Lock()
	cancel, done := c.cancel, c.done
	c.cancel, c.done = nil, nil
	c.mu.Unlock()

	if cancel == nil {
		return
	}
	cancel()
	<-done
}

// poll re-loads the certificate every interval. A failed reload is logged and the previous
// certificate stays in service.
func (c *ReloadingCertificate) poll(ctx context.Context, done chan struct{}) {
	defer close(done)

	ticker := time.NewTicker(c.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			changed, err := c.reload()
			switch {
			case err != nil:
				c.logger.Warnf("gateway: failed to reload certificate from %q, keeping the previous one: %v", c.certPath, err)
			case changed:
				c.logger.Infof("gateway: reloaded certificate from %q", c.certPath)
			}
		}
	}
}

// reload reads the certificate/key pair and installs it if its content differs from what is
// already loaded. It reports whether the served certificate changed.
func (c *ReloadingCertificate) reload() (bool, error) {
	var lastErr error
	for attempt := range certReloadMaxRetries {
		if attempt > 0 {
			time.Sleep(certReloadRetryInterval)
		}

		certPEM, err := os.ReadFile(c.certPath)
		if err != nil {
			lastErr = err
			continue
		}
		keyPEM, err := os.ReadFile(c.keyPath)
		if err != nil {
			lastErr = err
			continue
		}

		sum := sha256.Sum256(append(append([]byte{}, certPEM...), keyPEM...))
		if loaded := c.loaded.Load(); loaded != nil && *loaded == sum {
			return false, nil
		}

		cert, err := tls.X509KeyPair(certPEM, keyPEM)
		if err != nil {
			lastErr = err
			continue
		}
		if len(cert.Certificate) > 0 {
			if leaf, err := x509.ParseCertificate(cert.Certificate[0]); err == nil {
				cert.Leaf = leaf
			}
		}

		c.current.Store(&cert)
		c.loaded.Store(&sum)
		return true, nil
	}
	return false, fmt.Errorf("gateway: failed to load certificate from %q and %q: %w", c.certPath, c.keyPath, lastErr)
}

// certRecord is the JSON wire format Manager uses to distribute a Certificate through
// the configured Coordinator. Plain JSON keeps the shared certificate distribution path
// independent of any actor messaging format, since a Coordinator value is opaque bytes.
type certRecord struct {
	Domain   string `json:"domain"`
	CertPEM  []byte `json:"cert_pem"`
	KeyPEM   []byte `json:"key_pem"`
	NotAfter int64  `json:"not_after_unix"`
}

func encodeCertRecord(cert *Certificate) ([]byte, error) {
	return json.Marshal(certRecord{
		Domain:   cert.Domain,
		CertPEM:  cert.CertPEM,
		KeyPEM:   cert.KeyPEM,
		NotAfter: cert.NotAfter.Unix(),
	})
}

func decodeCertRecord(data []byte) (*Certificate, error) {
	var record certRecord
	if err := json.Unmarshal(data, &record); err != nil {
		return nil, err
	}
	return &Certificate{
		Domain:   record.Domain,
		CertPEM:  record.CertPEM,
		KeyPEM:   record.KeyPEM,
		NotAfter: time.Unix(record.NotAfter, 0).UTC(),
	}, nil
}
