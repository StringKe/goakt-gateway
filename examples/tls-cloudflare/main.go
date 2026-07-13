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

// Command tls-cloudflare demonstrates gateway.Manager wired for the two TLS shapes
// described in README.md:
//
//   - Per-domain Cloudflare Origin CA issuance, shared across multiple processes
//     through a coordinator/redis Coordinator so only one process ever calls the CA,
//     gated by a dynamic WithDomainPolicy standing in for a "bound custom domains"
//     database table.
//   - A single catch-all certificate (WithFallbackCertificate + NewReloadingCertificate)
//     behind Cloudflare Authenticated Origin Pulls mTLS, the shape a Cloudflare SaaS
//     Custom Hostname / edge-terminated deployment needs at the origin.
//
// It runs standalone: without CLOUDFLARE_ORIGIN_CA_KEY it issues from a locally
// generated demo CA instead of calling Cloudflare, and without a reachable Redis it
// falls back to gateway.NewMemoryCoordinator (still correct, just not cross-process).
// See README.md for what this does and does not prove.
package main

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	goredis "github.com/redis/go-redis/v9"
	goredislogging "github.com/redis/go-redis/v9/logging"
	"github.com/tochemey/goakt/v4/actor"
	golog "github.com/tochemey/goakt/v4/log"

	gateway "github.com/StringKe/goakt-gateway"
	rediscoordinator "github.com/StringKe/goakt-gateway/coordinator/redis"
	redisstore "github.com/StringKe/goakt-gateway/store/redis"
)

// boundDomain/unboundDomain are the two SNI values every step below is run against: one
// simulates a customer who has bound their domain in the (here, in-memory) tenant table,
// the other simulates an SNI value that was never bound - which is what any internet
// host scanning for open TLS ports on an IP will send.
const (
	boundDomain   = "app.example.com"
	unboundDomain = "unbound.example.com"
)

func main() {
	ctx := context.Background()

	// go-redis logs background connection-pool errors (e.g. a Redis it cannot reach) to
	// stderr on its own, independent of anything this example prints; silenced so the
	// "Redis unreachable, falling back" story is told exactly once, by buildCoordinator.
	goredislogging.Disable()

	system, err := actor.NewActorSystem("gateway-tls-cloudflare", actor.WithLogger(golog.DiscardLogger))
	if err != nil {
		log.Fatalf("create actor system: %v", err)
	}
	if err := system.Start(ctx); err != nil {
		log.Fatalf("start actor system: %v", err)
	}
	defer func() { _ = system.Stop(ctx) }()

	coordinator, coordinatorDesc := buildCoordinator(ctx)
	log.Printf("coordinator: %s", coordinatorDesc)

	certStore, certStoreDesc, closeCertStore := buildCertStore(ctx)
	defer closeCertStore()
	log.Printf("cert store: %s", certStoreDesc)

	issuer, issuerDesc := buildIssuer()
	log.Printf("issuer: %s", issuerDesc)

	runSharedIssuanceDemo(ctx, system, coordinator, certStore, issuer)
	runColdStartDemo(ctx, system, certStore)
	runOriginPullsAndFallbackDemo(ctx, system)

	log.Println("done")
}

// buildCoordinator returns a Redis-backed Coordinator when REDIS_ADDR (default
// "localhost:6379") is reachable, so the shared-issuance demo below is a genuine
// cross-process (well, cross-Coordinator-client) test of coordinator/redis; otherwise it
// degrades to gateway.NewMemoryCoordinator so the example still runs on a machine with
// no Redis. Either way the three simulated processes still exercise the same
// TryLock-arbitrated-issuance code path in Manager - only the "cross-process" part of
// the claim depends on which Coordinator this returns.
func buildCoordinator(ctx context.Context) (gateway.Coordinator, string) {
	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		addr = "localhost:6379"
	}

	client := goredis.NewClient(&goredis.Options{Addr: addr})
	pingCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()
	if err := client.Ping(pingCtx).Err(); err != nil {
		_ = client.Close()
		return gateway.NewMemoryCoordinator(),
			fmt.Sprintf("in-process MemoryCoordinator (Redis at %s unreachable: %v)", addr, err)
	}

	coord := rediscoordinator.New(client, rediscoordinator.WithKeyPrefix("goakt-gateway-example:"))
	return coord, fmt.Sprintf("coordinator/redis at %s", addr)
}

// buildCertStore returns a Redis/Valkey-backed CertStore when REDIS_ADDR is reachable, so
// an issued certificate is persisted where every process (and a cold-restarted one) can
// read it back without calling the issuer again; otherwise it falls back to an on-disk
// FileCertStore so the example still runs with no server. It deliberately shares one
// Redis/Valkey instance with buildCoordinator under the same key prefix: the store's keys
// carry a "cert:" infix that keeps them from colliding with the coordinator's, which is the
// intended "four backends, one instance, distinct namespaces" deployment. The returned
// closer releases whatever resource the store holds (a Redis client, or a temp directory).
func buildCertStore(ctx context.Context) (gateway.CertStore, string, func()) {
	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		addr = "localhost:6379"
	}

	client := goredis.NewClient(&goredis.Options{Addr: addr})
	pingCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()
	if err := client.Ping(pingCtx).Err(); err != nil {
		_ = client.Close()
		dir, mkErr := os.MkdirTemp("", "goakt-gateway-tls-cloudflare-certs-*")
		if mkErr != nil {
			log.Fatalf("create temp cert store directory: %v", mkErr)
		}
		store, fsErr := gateway.NewFileCertStore(dir)
		if fsErr != nil {
			log.Fatalf("create file cert store: %v", fsErr)
		}
		return store,
			fmt.Sprintf("on-disk FileCertStore at %s (Redis at %s unreachable: %v)", dir, addr, err),
			func() { _ = os.RemoveAll(dir) }
	}

	store := redisstore.New(client, redisstore.WithKeyPrefix("goakt-gateway-example:"))
	return store, fmt.Sprintf("store/redis at %s", addr), func() { _ = client.Close() }
}

// countingIssuer wraps a gateway.CertIssuer and counts calls to Issue, which is how the
// shared-issuance demo proves that three Managers racing to EnsureCertificate the same
// domain still only reach the upstream CA once.
type countingIssuer struct {
	inner gateway.CertIssuer
	calls atomic.Int64
}

func (c *countingIssuer) Issue(ctx context.Context, domain string) (*gateway.Certificate, error) {
	c.calls.Add(1)
	return c.inner.Issue(ctx, domain)
}

// buildIssuer returns a CloudflareOriginCAIssuer when CLOUDFLARE_ORIGIN_CA_KEY is set,
// and otherwise a StaticIssuer serving a locally generated certificate, so the example
// runs without a Cloudflare account. The demo CA it signs with stands in for Cloudflare
// Origin CA and is never used for anything a real client is asked to trust.
func buildIssuer() (*countingIssuer, string) {
	if token := os.Getenv("CLOUDFLARE_ORIGIN_CA_KEY"); token != "" {
		issuer := &gateway.CloudflareOriginCAIssuer{
			APIToken:           token,
			RequestCertificate: gateway.NewRSACertificateRequest(2048),
		}
		return &countingIssuer{inner: issuer}, "Cloudflare Origin CA (CLOUDFLARE_ORIGIN_CA_KEY set)"
	}

	originCA, err := newDemoCA("goakt-gateway example Origin CA (offline stand-in for Cloudflare Origin CA)")
	if err != nil {
		log.Fatalf("generate demo Origin CA: %v", err)
	}
	leaf, err := newDemoLeaf(boundDomain, originCA, true, 90*24*time.Hour)
	if err != nil {
		log.Fatalf("generate demo origin certificate: %v", err)
	}
	cert := &gateway.Certificate{
		Domain:   boundDomain,
		CertPEM:  leaf.certPEM,
		KeyPEM:   leaf.keyPEM,
		NotAfter: leaf.cert.NotAfter,
	}
	issuer := gateway.NewStaticIssuer(cert, boundDomain)
	return &countingIssuer{inner: issuer}, "static demo certificate (CLOUDFLARE_ORIGIN_CA_KEY not set)"
}

// runSharedIssuanceDemo simulates three separate gateway processes - each with its own
// Manager, the actor system reused only for convenience - that all share one Coordinator, one
// persistent CertStore, and one upstream issuer, exactly as three real nodes pointed at the
// same Redis/Valkey would. It proves two things: that a domain covered by the DomainPolicy is
// issued exactly once despite three concurrent racers (the Coordinator lock arbitrates which
// single racer calls the issuer; the others resolve the certificate the winner stored), and
// that a domain the policy refuses never reaches the issuer at all. The shared store is also
// what runColdStartDemo below reads back from, which is why it must be the one persistent
// store here, not a per-Manager in-memory one.
func runSharedIssuanceDemo(ctx context.Context, system actor.ActorSystem, coordinator gateway.Coordinator, certStore gateway.CertStore, issuer *countingIssuer) {
	log.Println("=== shared issuance across simulated processes ===")

	// boundDomains simulates a database table of tenant-bound custom domains. It is only
	// ever read after this point, so concurrent EnsureCertificate calls need no lock
	// around it.
	boundDomains := map[string]bool{boundDomain: true}
	var policyCalls atomic.Int64
	policy := func(_ context.Context, domain string) (bool, error) {
		policyCalls.Add(1)
		return boundDomains[domain], nil
	}

	const processCount = 3
	managers := make([]*gateway.Manager, processCount)
	for i := range managers {
		managers[i] = gateway.NewManager(system, golog.DiscardLogger,
			gateway.WithCoordinator(coordinator),
			gateway.WithCertStore(certStore),
			gateway.WithCertIssuer(issuer),
			gateway.WithDomainPolicy(policy),
		)
	}

	type result struct {
		cert *tls.Certificate
		err  error
	}
	results := make([]result, processCount)
	var wg sync.WaitGroup
	for i, m := range managers {
		wg.Go(func() {
			cert, err := m.EnsureCertificate(ctx, boundDomain)
			results[i] = result{cert: cert, err: err}
		})
	}
	wg.Wait()

	var fingerprint [32]byte
	for i, r := range results {
		if r.err != nil {
			log.Fatalf("process %d: EnsureCertificate(%q) failed: %v", i, boundDomain, r.err)
		}
		fp := sha256.Sum256(r.cert.Certificate[0])
		if i == 0 {
			fingerprint = fp
		} else if fp != fingerprint {
			log.Fatalf("process %d served a different certificate than process 0 - sharing failed", i)
		}
	}
	log.Printf("%d processes resolved %q to the identical certificate (sha256 %x...), issuer.Issue called %d time(s)",
		processCount, boundDomain, fingerprint[:6], issuer.calls.Load())

	issuerCallsBefore := issuer.calls.Load()
	policyCallsBeforeUnbound := policyCalls.Load()
	for range 2 {
		_, err := managers[0].EnsureCertificate(ctx, unboundDomain)
		if err == nil {
			log.Fatalf("EnsureCertificate(%q) unexpectedly succeeded", unboundDomain)
		}
		log.Printf("EnsureCertificate(%q) refused as expected: %v", unboundDomain, err)
	}
	log.Printf("unbound domain requested twice: domain policy invoked %d time(s) for it (second lookup served from the negative cache), issuer.Issue still called %d time(s) total",
		policyCalls.Load()-policyCallsBeforeUnbound, issuer.calls.Load())
	if issuer.calls.Load() != issuerCallsBefore {
		log.Fatalf("an unauthorized domain reached the issuer")
	}
}

// runColdStartDemo proves the point of a persistent CertStore: a freshly built Manager
// that shares the same store but has no CertIssuer at all - the shape of a process that
// cold-starts after the domain was already issued elsewhere - still serves the certificate
// by reading it back from the store, never reaching an issuer (there is none to reach).
// With a Redis/Valkey store this survives a full deployment restart, not just a single
// in-process one, because the certificate lives on the server, not in this process's heap.
func runColdStartDemo(ctx context.Context, system actor.ActorSystem, certStore gateway.CertStore) {
	log.Println("=== cold start from the persistent cert store (no issuer) ===")

	// No WithCertIssuer: if this Manager ever tried to issue it would fail with
	// ErrNoIssuer, so a successful EnsureCertificate here can only have come from the
	// store that the shared-issuance demo above populated.
	coldManager := gateway.NewManager(system, golog.DiscardLogger,
		gateway.WithCoordinator(gateway.NewMemoryCoordinator()),
		gateway.WithCertStore(certStore),
	)

	cert, err := coldManager.EnsureCertificate(ctx, boundDomain)
	if err != nil {
		log.Fatalf("cold start EnsureCertificate(%q) failed - the persistent store did not carry the certificate: %v", boundDomain, err)
	}
	fingerprint := sha256.Sum256(cert.Certificate[0])
	log.Printf("cold-started Manager served %q from the store (sha256 %x...) with no issuer configured", boundDomain, fingerprint[:6])
}

// runOriginPullsAndFallbackDemo builds the second TLS shape README.md describes: one
// catch-all certificate (hot-reloaded from disk, the way a Kubernetes-mounted secret
// would be) served regardless of SNI, behind Authenticated Origin Pulls mTLS. It proves
// the mTLS enforcement (no client cert, and a client cert from an untrusted CA, are both
// rejected; a client cert signed by the configured CA is accepted) and the hot reload
// (a certificate swapped on disk is picked up without restarting the process).
func runOriginPullsAndFallbackDemo(ctx context.Context, system actor.ActorSystem) {
	log.Println("=== fallback certificate + Authenticated Origin Pulls ===")

	dir, err := os.MkdirTemp("", "goakt-gateway-tls-cloudflare-*")
	if err != nil {
		log.Fatalf("create temp cert directory: %v", err)
	}
	defer func() { _ = os.RemoveAll(dir) }()

	catchAllV1, err := newDemoLeaf("edge.internal", nil, true, 24*time.Hour)
	if err != nil {
		log.Fatalf("generate catch-all certificate: %v", err)
	}
	certPath, keyPath, err := writeCertFiles(dir, catchAllV1.certPEM, catchAllV1.keyPEM)
	if err != nil {
		log.Fatalf("write catch-all certificate: %v", err)
	}

	// A short poll interval keeps the hot-reload demo below fast; production deployments
	// facing a Kubernetes secret rotation every few weeks would use something like a
	// minute.
	reloadingCert, err := gateway.NewReloadingCertificate(certPath, keyPath, 150*time.Millisecond, golog.DiscardLogger)
	if err != nil {
		log.Fatalf("create reloading certificate: %v", err)
	}
	reloadCtx, cancelReload := context.WithCancel(ctx)
	defer cancelReload()
	reloadingCert.Start(reloadCtx)
	defer reloadingCert.Stop()

	// originPullCA stands in for Cloudflare's published Authenticated Origin Pulls CA
	// bundle (https://developers.cloudflare.com/ssl/static/authenticated_origin_pull_ca.pem
	// in production - see README.md). goodClient is signed by it; badClient is a wholly
	// unrelated self-signed certificate, i.e. what any client not proxied through
	// Cloudflare would present.
	originPullCA, err := newDemoCA("goakt-gateway example Authenticated Origin Pulls CA")
	if err != nil {
		log.Fatalf("generate demo Authenticated Origin Pulls CA: %v", err)
	}
	goodClient, err := newDemoLeaf("cloudflare-edge.example", originPullCA, false, time.Hour)
	if err != nil {
		log.Fatalf("generate demo client certificate: %v", err)
	}
	badClient, err := newDemoLeaf("untrusted-client.example", nil, false, time.Hour)
	if err != nil {
		log.Fatalf("generate untrusted client certificate: %v", err)
	}

	manager := gateway.NewManager(system, golog.DiscardLogger,
		gateway.WithFallbackCertificate(reloadingCert.Get),
	)

	addr, err := freeLoopbackAddr()
	if err != nil {
		log.Fatalf("reserve loopback port: %v", err)
	}

	pulls := &gateway.AuthenticatedOriginPulls{CAPEM: originPullCA.certPEM}
	srv, err := gateway.NewServer(addr, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("ok"))
	}), gateway.WithTLSManager(manager), gateway.WithAuthenticatedOriginPulls(pulls))
	if err != nil {
		log.Fatalf("create server: %v", err)
	}

	serverErrs := make(chan error, 1)
	go func() { serverErrs <- srv.ListenAndServe(ctx) }()
	if err := waitForListening(addr, 5*time.Second); err != nil {
		log.Fatalf("server did not become ready: %v", err)
	}

	dialExpecting(addr, nil, "no client certificate", false)
	dialExpecting(addr, badClient, "client certificate from an untrusted CA", false)
	dialExpecting(addr, goodClient, "client certificate signed by the configured Origin Pulls CA", true)

	shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelShutdown()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Fatalf("shutdown server: %v", err)
	}
	if err := <-serverErrs; err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatalf("server exited with error: %v", err)
	}

	// Hot reload: overwrite the on-disk certificate with a new one and confirm
	// ReloadingCertificate picks it up without the process restarting or Get() being
	// reconfigured - the same mechanism a Kubernetes secret rotation relies on.
	before, err := reloadingCert.Get()
	if err != nil {
		log.Fatalf("read initial catch-all certificate: %v", err)
	}
	beforeFP := sha256.Sum256(before.Certificate[0])

	catchAllV2, err := newDemoLeaf("edge.internal", nil, true, 48*time.Hour)
	if err != nil {
		log.Fatalf("generate rotated catch-all certificate: %v", err)
	}
	if _, _, err := writeCertFiles(dir, catchAllV2.certPEM, catchAllV2.keyPEM); err != nil {
		log.Fatalf("write rotated catch-all certificate: %v", err)
	}

	after, err := waitForCertChange(reloadingCert.Get, beforeFP, 2*time.Second)
	if err != nil {
		log.Fatalf("hot reload: %v", err)
	}
	afterFP := sha256.Sum256(after.Certificate[0])
	log.Printf("catch-all certificate hot-reloaded: sha256 %x... -> %x... (no restart)", beforeFP[:6], afterFP[:6])
}

// writeCertFiles writes certPEM/keyPEM to "<dir>/tls.crt" and "<dir>/tls.key" via a
// write-to-temp-then-rename per file, so ReloadingCertificate's poller never observes a
// half-written file - only ever the previous complete pair or the new complete pair.
func writeCertFiles(dir string, certPEM, keyPEM []byte) (certPath, keyPath string, err error) {
	certPath = filepath.Join(dir, "tls.crt")
	keyPath = filepath.Join(dir, "tls.key")
	if err := atomicWriteFile(certPath, certPEM); err != nil {
		return "", "", err
	}
	if err := atomicWriteFile(keyPath, keyPEM); err != nil {
		return "", "", err
	}
	return certPath, keyPath, nil
}

func atomicWriteFile(path string, data []byte) error {
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

// freeLoopbackAddr reserves an ephemeral loopback port by binding to it and releasing
// it immediately, so the demo does not depend on a fixed port being free.
func freeLoopbackAddr() (string, error) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return "", err
	}
	addr := ln.Addr().String()
	if err := ln.Close(); err != nil {
		return "", err
	}
	return addr, nil
}

// waitForListening polls addr until a plain TCP connection succeeds, which is enough to
// know http.Server.Serve has bound the listener (the TLS handshake itself is what the
// callers below are testing).
func waitForListening(addr string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", addr, 100*time.Millisecond)
		if err == nil {
			// A bare connect-then-close never sends a ClientHello, which the server
			// would otherwise log as a confusing, unexplained "http: TLS handshake
			// error ...: EOF". Attempting (and discarding) a real handshake instead
			// makes the server's stderr, if anything, log the same "no client
			// certificate" rejection the first real test below deliberately
			// triggers - not a bare EOF that looks like a bug.
			_ = tls.Client(conn, &tls.Config{InsecureSkipVerify: true}).Handshake() //nolint:gosec // demo readiness probe only
			_ = conn.Close()
			return nil
		}
		time.Sleep(25 * time.Millisecond)
	}
	return fmt.Errorf("no listener on %s after %s", addr, timeout)
}

// dialExpecting attempts a TLS handshake against addr, optionally presenting client's
// certificate, and fails the demo (log.Fatalf) unless the outcome matches expectAccept.
// client may be nil to dial with no client certificate at all.
func dialExpecting(addr string, client *demoCert, label string, expectAccept bool) {
	cfg := &tls.Config{InsecureSkipVerify: true} //nolint:gosec // demo only skips *server* cert trust; client-cert enforcement is what is under test
	if client != nil {
		tlsCert, err := tls.X509KeyPair(client.certPEM, client.keyPEM)
		if err != nil {
			log.Fatalf("parse demo client certificate for %s: %v", label, err)
		}
		// GetClientCertificate, not Certificates: the server's CertificateRequest
		// advertises which CAs it accepts, and crypto/tls's default certificate
		// selection (used when only Certificates is set) silently omits any
		// certificate that does not chain to one of them - which would make the
		// "untrusted CA" case send no certificate at all and be indistinguishable
		// from the "no certificate" case. GetClientCertificate always sends what it
		// is given, so the untrusted certificate actually reaches the server and is
		// rejected for the reason this test exists to demonstrate.
		cfg.GetClientCertificate = func(*tls.CertificateRequestInfo) (*tls.Certificate, error) {
			return &tlsCert, nil
		}
	}

	err := tlsDial(addr, cfg)
	switch {
	case expectAccept && err != nil:
		log.Fatalf("connection with %s was unexpectedly rejected: %v", label, err)
	case expectAccept:
		log.Printf("connection with %s correctly accepted", label)
	case err == nil:
		log.Fatalf("connection with %s was unexpectedly accepted", label)
	default:
		log.Printf("connection with %s correctly rejected: %v", label, err)
	}
}

// tlsDial performs a full HTTPS round trip rather than just a TLS handshake. A raw
// tls.Dial can return success on the client side before the server has finished
// validating (and possibly rejecting) the client certificate: in TLS 1.3 the client
// sends its Finished message and considers its side of the handshake done without
// waiting for the server's verdict, so an immediate Close after Dial can miss a
// rejection the server sends microseconds later. Driving an actual HTTP request forces
// the round trip that would surface it.
func tlsDial(addr string, cfg *tls.Config) error {
	client := &http.Client{
		Timeout:   3 * time.Second,
		Transport: &http.Transport{TLSClientConfig: cfg},
	}
	resp, err := client.Get("https://" + addr + "/")
	if err != nil {
		return err
	}
	return resp.Body.Close()
}

// waitForCertChange polls get until it returns a certificate whose fingerprint differs
// from before, or timeout elapses.
func waitForCertChange(get func() (*tls.Certificate, error), before [32]byte, timeout time.Duration) (*tls.Certificate, error) {
	deadline := time.Now().Add(timeout)
	for {
		cert, err := get()
		if err != nil {
			return nil, err
		}
		if sha256.Sum256(cert.Certificate[0]) != before {
			return cert, nil
		}
		if time.Now().After(deadline) {
			return nil, fmt.Errorf("no change observed within %s", timeout)
		}
		time.Sleep(25 * time.Millisecond)
	}
}
