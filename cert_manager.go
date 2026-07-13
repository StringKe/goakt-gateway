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
	"container/list"
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"golang.org/x/sync/singleflight"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/tochemey/goakt/v4/actor"
	"github.com/tochemey/goakt/v4/log"
)

const (
	// certRenewalActorNamePrefix prefixes the name of the internal actor that receives the
	// renewal schedule's cron tick. In cluster mode ActorSystem.Spawn requires actor names to
	// be unique across the whole cluster (checked via the cluster actor directory, not just
	// locally), so a fixed name would make every node after the first fail Manager.Start with
	// ErrActorAlreadyExists. Start therefore suffixes this prefix with a random per-call
	// identifier, giving every node its own local actor. This does not weaken the cluster-wide
	// single-fire guarantee: ScheduleWithCron arbitrates delivery by the schedule reference
	// (certRenewalReference below), not by the target actor's name, so every node's uniquely
	// named actor can still share the exact same reference and have the cluster still deliver
	// each tick to exactly one of them. It intentionally does not start with the reserved
	// "GoAkt" actor name prefix.
	certRenewalActorNamePrefix = "goaktGatewayCertRenewal"
	// certRenewalReference is the ScheduleOption reference used so the renewal schedule
	// can be canceled by Manager.Stop. In cluster mode a reference is required by
	// ActorSystem.ScheduleWithCron, and it is what the cluster keys its single-fire-per-tick
	// arbitration on; see certRenewalActorNamePrefix and Manager.Start.
	certRenewalReference = "goakt-gateway-cert-renewal"

	certCoordinatorKeyPrefix     = "gateway:cert:"
	certLockCoordinatorKeyPrefix = "gateway:cert-lock:"

	defaultRenewBefore = 30 * 24 * time.Hour
	defaultLockTTL     = 2 * time.Minute
	// go-quartz cron syntax (6 fields, seconds first): fire at the top of every hour.
	// Descriptor forms like "@every 1h" are NOT supported by go-quartz and fail to
	// parse, which would abort Manager.Start.
	defaultRenewInterval  = "0 0 * * * *"
	defaultWaitPollPeriod = 100 * time.Millisecond

	defaultNegativeCacheTTL = time.Minute
	defaultMaxCachedCerts   = 1024
)

// cachedCert is the parsed, ready-to-serve form of a Certificate kept in Manager's hot
// in-memory cache.
type cachedCert struct {
	tlsCert  *tls.Certificate
	notAfter time.Time
}

// negativeEntry records that a domain was refused (or failed issuance) so that repeated
// handshakes for it are answered from memory instead of re-consulting the DomainPolicy or
// re-calling the CertIssuer.
type negativeEntry struct {
	err       error
	expiresAt time.Time
}

// lruEntry is what lruCache threads through its recency list.
type lruEntry[V any] struct {
	key   string
	value V
}

// lruCache is a fixed-capacity, string-keyed LRU cache. Manager keys both its hot
// certificate cache and its negative cache by the SNI server name, i.e. by fully
// attacker-controlled input: an unbounded map would let a peer that opens handshakes with
// random SNI values grow the process's memory without limit.
type lruCache[V any] struct {
	mu       sync.Mutex
	capacity int
	order    *list.List
	items    map[string]*list.Element
}

func newLRUCache[V any](capacity int) *lruCache[V] {
	return &lruCache[V]{
		capacity: capacity,
		order:    list.New(),
		items:    make(map[string]*list.Element, capacity),
	}
}

// get returns the value cached under key, marking it as most recently used.
func (c *lruCache[V]) get(key string) (V, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	element, ok := c.items[key]
	if !ok {
		var zero V
		return zero, false
	}
	c.order.MoveToFront(element)
	return element.Value.(*lruEntry[V]).value, true
}

// put inserts or overwrites key, evicting the least recently used entry once the cache is
// over capacity.
func (c *lruCache[V]) put(key string, value V) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if element, ok := c.items[key]; ok {
		element.Value.(*lruEntry[V]).value = value
		c.order.MoveToFront(element)
		return
	}
	c.items[key] = c.order.PushFront(&lruEntry[V]{key: key, value: value})
	for c.order.Len() > c.capacity {
		oldest := c.order.Back()
		if oldest == nil {
			return
		}
		c.order.Remove(oldest)
		delete(c.items, oldest.Value.(*lruEntry[V]).key)
	}
}

// remove drops key from the cache, if present.
func (c *lruCache[V]) remove(key string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	element, ok := c.items[key]
	if !ok {
		return
	}
	c.order.Remove(element)
	delete(c.items, key)
}

// len returns the number of cached entries.
func (c *lruCache[V]) len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.order.Len()
}

// snapshot returns a copy of every cached entry, leaving recency order untouched.
func (c *lruCache[V]) snapshot() map[string]V {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make(map[string]V, c.order.Len())
	for element := c.order.Front(); element != nil; element = element.Next() {
		entry := element.Value.(*lruEntry[V])
		out[entry.key] = entry.value
	}
	return out
}

// DomainPolicy decides at handshake time whether domain may be served (and, if no
// certificate exists yet, issued for). It is the dynamic counterpart to
// WithAllowedDomains: multi-tenant deployments that let customers bring their own domain
// cannot enumerate a static allow list, so they plug in a policy that consults their own
// source of truth (e.g. "is this hostname bound to an active tenant?").
//
// It is called on the TLS handshake path, so it must be fast and safe for concurrent use.
// Its verdict is only consulted for domains that are neither already cached nor negatively
// cached (see WithNegativeCacheTTL), so a policy backed by a database is not queried once
// per handshake.
//
// Returning an error (as opposed to false) means the policy itself could not reach a
// verdict; the handshake fails and nothing is cached, so the next handshake asks again.
type DomainPolicy func(ctx context.Context, domain string) (bool, error)

// Manager terminates TLS for one or more domains from certificates shared across every
// process that shares its Coordinator: issuance for a given domain is arbitrated
// through Coordinator.TryLock so at most one caller invokes the configured CertIssuer,
// the result is distributed through Coordinator.Put/Get, and a cron renewal schedule
// (single-fire cluster-wide by GoAkt scheduler design when running in a GoAkt cluster)
// drives renewal ahead of expiry.
//
// The issuance lock is held for at most WithIssuanceLockTTL and is not renewed/extended
// while a CertIssuer call is outstanding: if issuance takes longer than lockTTL, the lock
// can expire mid-issuance, letting a second process acquire it and also call the issuer.
// doEnsure detects this after the fact and returns ErrIssuanceLockExpired instead of
// caching the result, but the extra CertIssuer call itself is not prevented. Set
// WithIssuanceLockTTL comfortably longer than your CertIssuer's worst-case latency.
//
// Without an explicit WithCoordinator, Manager defaults to a process-local
// MemoryCoordinator: issuance is still deduplicated (so concurrent handshakes for a cold
// domain only call the issuer once per process) but nothing is shared across processes.
// Share a cluster-backed Coordinator (e.g. coordinator/redis) to get cluster-wide
// arbitration and distribution.
//
// Because the SNI server name a Manager is asked for is entirely attacker-controlled, a
// Manager configured with a CertIssuer denies every domain by default: WithAllowedDomains,
// WithDomainPolicy, or both must be configured to declare which domains may be served or
// issued for. Without either, EnsureCertificate/GetCertificate fail with
// ErrDomainNotAllowed for every domain rather than letting an arbitrary SNI value trigger a
// real issuance against the CA (which is rate limited and quota'd). This deny-by-default
// only applies once a CertIssuer is configured; without one, Manager can only ever serve a
// pre-provisioned certificate (from CertStore or WithFallbackCertificate), so allowing every
// domain through costs nothing. Domains that are refused, or whose issuance fails, are
// negatively cached for WithNegativeCacheTTL, and the hot certificate cache is bounded by
// WithMaxCachedCerts.
//
// A Manager is safe for concurrent use. Use NewManager to construct one.
type Manager struct {
	system actor.ActorSystem
	logger log.Logger

	issuer         CertIssuer
	store          CertStore
	coordinator    Coordinator
	allowedDomains []string
	domainPolicy   DomainPolicy
	fallback       func() (*tls.Certificate, error)
	renewBefore    time.Duration
	lockTTL        time.Duration
	renewInterval  string
	negativeTTL    time.Duration
	maxCachedCerts int

	// g deduplicates concurrent EnsureCertificate/renewCertificate calls for the same
	// domain on this process, independent of (and layered underneath) the
	// Coordinator.TryLock arbitration doEnsure performs.
	g singleflight.Group

	certs     *lruCache[*cachedCert]
	negatives *lruCache[negativeEntry]

	renewalPID *actor.PID
}

// ManagerOption configures a Manager created with NewManager.
type ManagerOption func(*Manager)

// WithCertIssuer sets the CertIssuer used to obtain certificates that are not already
// cached or stored. Without one, Manager can only serve certificates that a CertStore
// already has (e.g. pre-populated via CertStore.Put).
//
// SECURITY: the SNI server name a Manager is asked for is entirely attacker-controlled, so
// configuring a CertIssuer alone denies every domain by default. WithAllowedDomains,
// WithDomainPolicy, or both must also be configured to declare which domains may be
// admitted; without either, EnsureCertificate/GetCertificate return ErrDomainNotAllowed for
// every domain. This is what stops a remote party from exhausting the CA's (rate-limited,
// quota'd) issuance budget by opening handshakes with random hostnames. Refused/failed
// domains are additionally negatively cached (WithNegativeCacheTTL) and the hot cache is
// bounded (WithMaxCachedCerts), but neither substitutes for an admission policy.
func WithCertIssuer(issuer CertIssuer) ManagerOption {
	return func(m *Manager) { m.issuer = issuer }
}

// WithCertStore overrides the persistent CertStore layer. Defaults to a MemoryCertStore.
func WithCertStore(store CertStore) ManagerOption {
	return func(m *Manager) { m.store = store }
}

// WithCoordinator overrides the Coordinator Manager uses to arbitrate issuance and
// distribute certificates. Defaults to a process-local NewMemoryCoordinator; pass a
// cluster-shared implementation (e.g. coordinator/redis) to coordinate issuance across
// every process that shares it.
func WithCoordinator(coordinator Coordinator) ManagerOption {
	return func(m *Manager) { m.coordinator = coordinator }
}

// WithAllowedDomains restricts issuance/serving to the given domains. A domain may be
// given as an exact name ("example.com") or as a single-label wildcard ("*.example.com",
// which matches "a.example.com" but neither "example.com" itself nor "a.b.example.com",
// mirroring how TLS clients match a wildcard SAN).
//
// When WithDomainPolicy is also configured, the static list is checked first and the
// policy is only consulted for domains it does not cover, so the common, statically known
// domains never pay for a policy lookup.
//
// Without either option, a Manager configured with WithCertIssuer denies every domain by
// default; see the Manager documentation.
func WithAllowedDomains(domains ...string) ManagerOption {
	return func(m *Manager) {
		m.allowedDomains = make([]string, 0, len(domains))
		for _, d := range domains {
			m.allowedDomains = append(m.allowedDomains, normalizeDomain(d))
		}
	}
}

// WithDomainPolicy sets a dynamic admission check consulted for domains not covered by
// WithAllowedDomains. Use it for multi-tenant custom domains, where the set of servable
// domains lives in a database rather than in configuration.
func WithDomainPolicy(p DomainPolicy) ManagerOption {
	return func(m *Manager) { m.domainPolicy = p }
}

// WithNegativeCacheTTL sets how long a refused domain (or one whose issuance failed) is
// remembered so that a repeated handshake for it is answered from memory. Defaults to one
// minute. This is what keeps a flood of handshakes carrying unknown SNI values from turning
// into a flood of DomainPolicy lookups and CA issuance attempts. Pass a value <= 0 to
// disable negative caching entirely (not recommended when a CertIssuer is configured).
func WithNegativeCacheTTL(d time.Duration) ManagerOption {
	return func(m *Manager) { m.negativeTTL = d }
}

// WithMaxCachedCerts bounds the number of entries Manager keeps in its hot certificate
// cache and in its negative cache, evicting the least recently used entry beyond that.
// Defaults to 1024. Both caches are keyed by the peer-supplied SNI server name, so a bound
// is what keeps memory from growing with the number of distinct names a peer chooses to
// send. Values <= 0 are ignored and keep the default.
func WithMaxCachedCerts(n int) ManagerOption {
	return func(m *Manager) { m.maxCachedCerts = n }
}

// WithFallbackCertificate sets the certificate served when the ClientHello carries no SNI,
// or when the requested domain has no certificate and no CertIssuer is configured to obtain
// one. It covers the "TLS terminated at a CDN edge, origin serves a single catch-all
// certificate" deployment, where SNI is irrelevant and the origin simply presents the same
// certificate to every connection; combine it with NewReloadingCertificate to pick up
// rotations of a certificate mounted from a Kubernetes secret.
//
// A domain that was explicitly refused (by WithAllowedDomains/WithDomainPolicy) is never
// served the fallback: the handshake fails with ErrDomainNotAllowed, because answering a
// refused name with a valid certificate would defeat the admission check.
func WithFallbackCertificate(get func() (*tls.Certificate, error)) ManagerOption {
	return func(m *Manager) { m.fallback = get }
}

// WithRenewBefore sets how far ahead of a certificate's expiry Manager renews it.
// Defaults to 30 days.
func WithRenewBefore(d time.Duration) ManagerOption {
	return func(m *Manager) { m.renewBefore = d }
}

// WithIssuanceLockTTL sets how long the coordinated issuance lock for a domain is held
// for. Defaults to 2 minutes. The lock is not renewed while the CertIssuer call is in
// flight, so set this comfortably longer than your CertIssuer's worst-case latency: if
// issuance overruns it, doEnsure returns ErrIssuanceLockExpired instead of silently
// caching a result whose single-issuer guarantee is no longer certain.
func WithIssuanceLockTTL(d time.Duration) ManagerOption {
	return func(m *Manager) { m.lockTTL = d }
}

// WithRenewInterval sets the cron expression (go-quartz syntax, 6 fields with seconds
// first) the renewal schedule uses to check for expiring certificates. Defaults to
// hourly ("0 0 * * * *"). Pass an empty string to disable automatic renewal entirely
// (EnsureCertificate/GetCertificate will still re-issue on-demand once a served
// certificate has actually expired).
func WithRenewInterval(cronExpression string) ManagerOption {
	return func(m *Manager) { m.renewInterval = cronExpression }
}

// NewManager creates a Manager backed by system. Call Start before serving traffic so
// the renewal schedule is registered, and Stop during shutdown to release it.
func NewManager(system actor.ActorSystem, logger log.Logger, opts ...ManagerOption) *Manager {
	if logger == nil {
		logger = log.DiscardLogger
	}
	m := &Manager{
		system:         system,
		logger:         logger,
		store:          NewMemoryCertStore(),
		coordinator:    NewMemoryCoordinator(),
		renewBefore:    defaultRenewBefore,
		lockTTL:        defaultLockTTL,
		renewInterval:  defaultRenewInterval,
		negativeTTL:    defaultNegativeCacheTTL,
		maxCachedCerts: defaultMaxCachedCerts,
	}
	for _, opt := range opts {
		opt(m)
	}
	if m.maxCachedCerts <= 0 {
		m.maxCachedCerts = defaultMaxCachedCerts
	}
	m.certs = newLRUCache[*cachedCert](m.maxCachedCerts)
	m.negatives = newLRUCache[negativeEntry](m.maxCachedCerts)
	return m
}

// Start registers the renewal schedule (cluster-single-fire when system is running in a
// GoAkt cluster, driven by the schedule reference rather than by actor identity - see
// certRenewalActorNamePrefix). It is a no-op if renewal was disabled via
// WithRenewInterval(""). Each call spawns its own uniquely-named local renewal actor, so
// calling Start from every node of a cluster (the normal deployment shape) does not fail
// with ErrActorAlreadyExists.
func (m *Manager) Start(ctx context.Context) error {
	if m.renewInterval == "" {
		return nil
	}

	actorName := certRenewalActorNamePrefix + "-" + uuid.NewString()
	pid, err := m.system.Spawn(ctx, actorName, &certRenewalActor{manager: m}, actor.WithLongLived())
	if err != nil {
		return fmt.Errorf("gateway: failed to start certificate renewal actor: %w", err)
	}
	m.renewalPID = pid

	if err := m.system.ScheduleWithCron(ctx, &emptypb.Empty{}, pid, m.renewInterval,
		actor.WithReference(certRenewalReference),
	); err != nil {
		return fmt.Errorf("gateway: failed to register certificate renewal schedule: %w", err)
	}
	return nil
}

// Stop cancels the renewal schedule and stops its backing actor. It is a no-op if Start
// was never called or renewal was disabled.
func (m *Manager) Stop(ctx context.Context) error {
	if m.renewalPID == nil {
		return nil
	}
	_ = m.system.CancelSchedule(certRenewalReference)
	err := m.renewalPID.Shutdown(ctx)
	m.renewalPID = nil
	return err
}

// GetCertificate implements the tls.Config.GetCertificate signature for SNI-based
// dynamic certificate lookup: assign it directly, e.g.
// tls.Config{GetCertificate: manager.GetCertificate}.
//
// A ClientHello without SNI is served the WithFallbackCertificate, if one is configured,
// and otherwise fails: there is nothing to look a certificate up by. A domain with no
// certificate and no CertIssuer to obtain one also falls back, which is what makes a
// Manager configured with nothing but WithFallbackCertificate behave as a catch-all
// certificate server.
func (m *Manager) GetCertificate(hello *tls.ClientHelloInfo) (*tls.Certificate, error) {
	ctx := hello.Context()
	if ctx == nil {
		// tls.ClientHelloInfo.Context() is nil unless the handshake actually attached one
		// (e.g. GetCertificate called directly in a test).
		ctx = context.Background()
	}

	domain := normalizeDomain(hello.ServerName)
	if domain == "" {
		if m.fallback == nil {
			return nil, fmt.Errorf("gateway: SNI server name required")
		}
		return m.fallbackCertificate()
	}

	cert, err := m.EnsureCertificate(ctx, domain)
	if err == nil {
		return cert, nil
	}
	if errors.Is(err, ErrNoIssuer) && m.fallback != nil {
		return m.fallbackCertificate()
	}
	return nil, err
}

// fallbackCertificate returns the certificate configured with WithFallbackCertificate.
func (m *Manager) fallbackCertificate() (*tls.Certificate, error) {
	cert, err := m.fallback()
	if err != nil {
		return nil, fmt.Errorf("gateway: failed to load the fallback certificate: %w", err)
	}
	if cert == nil {
		return nil, ErrCertificateNotFound
	}
	return cert, nil
}

// TLSConfig returns a *tls.Config wired to serve certificates dynamically by SNI through
// this Manager.
func (m *Manager) TLSConfig() *tls.Config {
	return &tls.Config{
		MinVersion:     tls.VersionTLS12,
		GetCertificate: m.GetCertificate,
	}
}

// EnsureCertificate returns a ready-to-serve certificate for domain, issuing (or
// fetching from another caller's issuance) one if necessary. Concurrent calls for the
// same domain on this process are deduplicated; concurrent calls across every process
// that shares this Manager's Coordinator are arbitrated so only one of them calls the
// configured CertIssuer (see Manager documentation).
//
// Admission is checked before anything reaches the CertIssuer: a domain covered by neither
// WithAllowedDomains nor WithDomainPolicy is refused with ErrDomainNotAllowed. A refusal,
// like a failed issuance, is remembered for WithNegativeCacheTTL so a repeat of the same
// request costs neither a policy lookup nor a CA call.
//
// It returns ErrNoIssuer if issuance is required but no CertIssuer was configured.
func (m *Manager) EnsureCertificate(ctx context.Context, domain string) (*tls.Certificate, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	domain = normalizeDomain(domain)
	if domain == "" {
		return nil, fmt.Errorf("gateway: certificate domain is required")
	}

	if err, ok := m.negative(domain); ok {
		return nil, err
	}

	// Serving an already-cached certificate does not re-consult the admission check: the
	// check gates issuance, and a certificate this Manager has already served is cheaper
	// to keep serving until it is evicted or expires than to pay a policy lookup per
	// handshake. Shorten WithMaxCachedCerts/rotate the process to drop a revoked domain
	// sooner.
	if cert, ok := m.hot(domain); ok {
		return cert, nil
	}

	allowed, err := m.admit(ctx, domain)
	if err != nil {
		return nil, fmt.Errorf("gateway: domain policy failed for %q: %w", domain, err)
	}
	if !allowed {
		m.deny(domain, ErrDomainNotAllowed)
		return nil, ErrDomainNotAllowed
	}

	v, err, _ := m.g.Do(domain, func() (any, error) {
		return m.doEnsure(ctx, domain, false)
	})
	if err != nil {
		m.deny(domain, err)
		return nil, err
	}
	return v.(*tls.Certificate), nil
}

// admit reports whether domain may be served. The static allow list is consulted first
// because it costs no I/O; the DomainPolicy only sees what the list does not cover. With
// neither configured, admission is deny-by-default whenever a CertIssuer is attached (see
// the Manager documentation): allow-any is only safe without an issuer, where at most a
// pre-provisioned certificate can be served.
func (m *Manager) admit(ctx context.Context, domain string) (bool, error) {
	for _, pattern := range m.allowedDomains {
		if matchAllowedDomain(pattern, domain) {
			return true, nil
		}
	}
	if m.domainPolicy != nil {
		return m.domainPolicy(ctx, domain)
	}
	if m.allowedDomains != nil {
		return false, nil // an allow list was configured; the domain matched nothing in it
	}
	// Neither an allow list nor a policy is configured. Allow-any is only safe without an
	// issuer, where at most a pre-provisioned certificate can be served. With an issuer, an
	// unrecognized SNI would trigger a real issuance and burn CA quota, so deny by default:
	// the operator must declare admitted domains explicitly via WithAllowedDomains or
	// WithDomainPolicy.
	return m.issuer == nil, nil
}

// matchAllowedDomain reports whether domain matches an allow-list pattern. A pattern
// beginning with "*." matches exactly one additional label, the same way TLS clients match
// a wildcard SAN (RFC 6125): "*.example.com" matches "a.example.com" but neither
// "example.com" nor "a.b.example.com".
func matchAllowedDomain(pattern, domain string) bool {
	if pattern == domain {
		return true
	}
	if !strings.HasPrefix(pattern, "*.") {
		return false
	}
	suffix := pattern[1:] // ".example.com"
	if !strings.HasSuffix(domain, suffix) {
		return false
	}
	label := domain[:len(domain)-len(suffix)]
	return label != "" && !strings.Contains(label, ".")
}

// normalizeDomain lower-cases domain and strips the trailing root dot a client may send,
// so that "A.Example.com." and "a.example.com" are one cache key and one allow-list match
// rather than three.
func normalizeDomain(domain string) string {
	return strings.ToLower(strings.TrimSuffix(strings.TrimSpace(domain), "."))
}

// negative reports the remembered failure for domain, if it has not expired yet.
func (m *Manager) negative(domain string) (error, bool) {
	if m.negativeTTL <= 0 {
		return nil, false
	}
	entry, ok := m.negatives.get(domain)
	if !ok {
		return nil, false
	}
	if time.Now().After(entry.expiresAt) {
		m.negatives.remove(domain)
		return nil, false
	}
	return entry.err, true
}

// deny remembers that domain was refused or failed issuance, so repeated handshakes for it
// are answered from memory. A context error is never remembered: it says the caller went
// away, not that the domain is unservable.
func (m *Manager) deny(domain string, err error) {
	if m.negativeTTL <= 0 {
		return
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return
	}
	m.negatives.put(domain, negativeEntry{err: err, expiresAt: time.Now().Add(m.negativeTTL)})
}

// renewCertificate forces a fresh issuance for domain, bypassing the hot-cache/"already
// valid" fast path EnsureCertificate takes. It is only called by the renewal schedule.
func (m *Manager) renewCertificate(ctx context.Context, domain string) (*tls.Certificate, error) {
	v, err, _ := m.g.Do(domain, func() (any, error) {
		return m.doEnsure(ctx, domain, true)
	})
	if err != nil {
		return nil, err
	}
	return v.(*tls.Certificate), nil
}

// hot returns the cached certificate for domain if present and not within renewBefore
// of expiring.
func (m *Manager) hot(domain string) (*tls.Certificate, bool) {
	cc, ok := m.certs.get(domain)
	if !ok || time.Until(cc.notAfter) < m.renewBefore {
		return nil, false
	}
	return cc.tlsCert, true
}

// cache stores cert in the hot in-memory cache and records its expiry for renewal
// bookkeeping.
func (m *Manager) cache(cert *Certificate, tlsCert *tls.Certificate) {
	m.certs.put(normalizeDomain(cert.Domain), &cachedCert{tlsCert: tlsCert, notAfter: cert.NotAfter})
}

// doEnsure performs the actual lookup/issuance for domain. It runs inside the
// per-domain singleflight group, so at most one goroutine per domain executes it on
// this process at a time. When force is true, an already-valid cached/stored certificate
// is not treated as sufficient - a fresh one is still fetched/issued (used by renewal).
func (m *Manager) doEnsure(ctx context.Context, domain string, force bool) (*tls.Certificate, error) {
	if ctx == nil {
		// context.WithoutCancel below requires a non-nil parent.
		ctx = context.Background()
	}
	if !force {
		if cert, ok := m.hot(domain); ok {
			return cert, nil
		}
		if cert, ok := m.fromStore(ctx, domain); ok {
			return cert, nil
		}
		if cert, ok := m.fromCoordinator(ctx, domain); ok {
			return cert, nil
		}
	}

	// Checked before taking the issuance lock: without an issuer no caller can ever make
	// progress under it, and the WithFallbackCertificate path (catch-all certificate, no
	// issuer at all) would otherwise pay a lock round trip on every cold domain.
	if m.issuer == nil {
		return nil, ErrNoIssuer
	}

	unlock, err := m.coordinator.TryLock(ctx, certLockCoordinatorKeyPrefix+domain, m.lockTTL)
	if err != nil {
		if errors.Is(err, ErrLockNotAcquired) {
			return m.waitForCoordinatedCert(ctx, domain)
		}
		return nil, fmt.Errorf("gateway: failed to acquire issuance lock for %q: %w", domain, err)
	}
	lockAcquiredAt := time.Now()
	defer func() { _ = unlock(context.WithoutCancel(ctx)) }()

	// Someone may have finished issuing between our pre-lock read and acquiring the
	// lock; re-check once more now that we are the exclusive issuer for this domain.
	if !force {
		if cert, ok := m.fromCoordinator(ctx, domain); ok {
			return cert, nil
		}
	}

	tlsCert, cert, err := m.issue(ctx, domain)
	if err != nil {
		return nil, err
	}

	// The Coordinator lock has no renewal/heartbeat: if the issuer call outlived
	// lockTTL, the lock may already have expired and a second process may already be
	// issuing (or have issued) concurrently. Surface that instead of silently caching a
	// result whose single-issuer guarantee is no longer certain.
	if elapsed := time.Since(lockAcquiredAt); elapsed >= m.lockTTL {
		return nil, fmt.Errorf("gateway: issuance for %q took %s, exceeding the %s issuance lock ttl: %w",
			domain, elapsed, m.lockTTL, ErrIssuanceLockExpired)
	}

	if err := m.store.Put(ctx, cert); err != nil {
		m.logger.Warnf("gateway: failed to persist certificate for %q to local store: %v", domain, err)
	}

	record, err := encodeCertRecord(cert)
	if err != nil {
		return nil, fmt.Errorf("gateway: failed to encode certificate for %q: %w", domain, err)
	}
	if err := m.coordinator.Put(ctx, certCoordinatorKeyPrefix+domain, record, 0); err != nil {
		m.logger.Warnf("gateway: failed to distribute certificate for %q via coordinator: %v", domain, err)
	}

	m.cache(cert, tlsCert)
	return tlsCert, nil
}

// fromStore attempts to serve domain from the persistent CertStore layer.
func (m *Manager) fromStore(ctx context.Context, domain string) (*tls.Certificate, bool) {
	cert, err := m.store.Get(ctx, domain)
	if err != nil {
		return nil, false
	}
	// Verify before the expiry check: parseAndVerify overwrites cert.NotAfter with the
	// leaf's real value, so the freshness check below trusts the certificate rather than
	// the store's self-reported expiry, and a store returning a nil/wrong-domain/corrupt
	// certificate is discarded instead of served or cached.
	tlsCert, err := parseAndVerify(cert, domain)
	if err != nil {
		m.logger.Warnf("gateway: stored certificate for %q is invalid: %v", domain, err)
		return nil, false
	}
	if time.Until(cert.NotAfter) < m.renewBefore {
		return nil, false
	}
	m.cache(cert, tlsCert)
	return tlsCert, true
}

// fromCoordinator attempts to serve domain from the shared Coordinator layer,
// additionally persisting a local copy so a subsequent single-process restart does not
// need the Coordinator.
func (m *Manager) fromCoordinator(ctx context.Context, domain string) (*tls.Certificate, bool) {
	data, ok, err := m.coordinator.Get(ctx, certCoordinatorKeyPrefix+domain)
	if err != nil || !ok {
		return nil, false
	}
	cert, err := decodeCertRecord(data)
	if err != nil {
		m.logger.Warnf("gateway: certificate record for %q from coordinator is invalid: %v", domain, err)
		return nil, false
	}
	// Verify before the expiry check and before persisting: parseAndVerify overwrites
	// cert.NotAfter with the leaf's real value, so the freshness check trusts the
	// certificate rather than the coordinator record's self-reported expiry, and a tampered
	// or corrupt record covering the wrong domain is never served, cached, or written back
	// to the local store.
	tlsCert, err := parseAndVerify(cert, domain)
	if err != nil {
		m.logger.Warnf("gateway: certificate for %q fetched from coordinator is invalid: %v", domain, err)
		return nil, false
	}
	if time.Until(cert.NotAfter) < m.renewBefore {
		return nil, false
	}
	if err := m.store.Put(ctx, cert); err != nil {
		m.logger.Warnf("gateway: failed to persist certificate for %q fetched from coordinator: %v", domain, err)
	}
	m.cache(cert, tlsCert)
	return tlsCert, true
}

// waitForCoordinatedCert is taken by every caller that lost the TryLock race for domain:
// it polls the Coordinator until the winner publishes a certificate, or gives up once
// the issuing caller's lock would have expired anyway.
func (m *Manager) waitForCoordinatedCert(ctx context.Context, domain string) (*tls.Certificate, error) {
	deadline := time.Now().Add(m.lockTTL)
	ticker := time.NewTicker(defaultWaitPollPeriod)
	defer ticker.Stop()

	for {
		if cert, ok := m.fromCoordinator(ctx, domain); ok {
			return cert, nil
		}
		if time.Now().After(deadline) {
			return nil, ErrIssuanceTimeout
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-ticker.C:
		}
	}
}

// issue calls the configured CertIssuer and parses the result into a *tls.Certificate.
// doEnsure guarantees m.issuer is non-nil by the time it is reached.
func (m *Manager) issue(ctx context.Context, domain string) (*tls.Certificate, *Certificate, error) {
	cert, err := m.issuer.Issue(ctx, domain)
	if err != nil {
		return nil, nil, fmt.Errorf("gateway: failed to issue certificate for %q: %w", domain, err)
	}
	tlsCert, err := parseAndVerify(cert, domain)
	if err != nil {
		return nil, nil, fmt.Errorf("gateway: issuer returned an invalid certificate for %q: %w", domain, err)
	}
	return tlsCert, cert, nil
}

// renewAll checks every domain this process has ever served for imminent expiry and
// renews it. It is invoked by certRenewalActor on the renewal schedule tick.
func (m *Manager) renewAll(ctx context.Context) {
	cached := m.certs.snapshot()
	domains := make([]string, 0, len(cached))
	for domain, cc := range cached {
		if time.Until(cc.notAfter) < m.renewBefore {
			domains = append(domains, domain)
		}
	}

	for _, domain := range domains {
		if _, err := m.renewCertificate(ctx, domain); err != nil {
			m.logger.Warnf("gateway: failed to renew certificate for %q: %v", domain, err)
		}
	}
}

// parseAndVerify parses cert's PEM material into a *tls.Certificate and enforces the trust
// boundary between what an adapter (CertIssuer/CertStore/Coordinator) claims and what the
// certificate itself actually attests. Manager keys its hot cache, negative cache and
// renewal bookkeeping by the requested domain, so a Certificate whose leaf does not actually
// cover that domain, whose self-reported Domain disagrees with the request, or whose
// self-reported NotAfter disagrees with the leaf, must never be parsed as valid: a buggy
// adapter or a corrupted/tampered record could otherwise make Manager serve one domain
// another domain's certificate, cache material under the wrong SNI key, skip a due renewal
// on a fabricated expiry, or panic on a nil/empty chain.
//
// nil, an empty chain, an unparseable leaf, a leaf that does not cover domain, or a
// self-reported Domain that does not equal domain are all rejected. On success the returned
// *tls.Certificate always has a populated Leaf, and cert.NotAfter is overwritten with the
// leaf's real NotAfter so that no caller (cache/store/coordinator) ever propagates the
// adapter's self-reported expiry.
func parseAndVerify(cert *Certificate, domain string) (*tls.Certificate, error) {
	if cert == nil {
		return nil, fmt.Errorf("gateway: nil certificate for %q", domain)
	}
	if normalizeDomain(cert.Domain) != domain {
		return nil, fmt.Errorf("gateway: certificate reports domain %q but %q was requested", cert.Domain, domain)
	}
	tlsCert, err := tls.X509KeyPair(cert.CertPEM, cert.KeyPEM)
	if err != nil {
		return nil, err
	}
	if len(tlsCert.Certificate) == 0 {
		return nil, fmt.Errorf("gateway: certificate for %q has an empty chain", domain)
	}
	leaf, err := x509.ParseCertificate(tlsCert.Certificate[0])
	if err != nil {
		return nil, fmt.Errorf("gateway: failed to parse leaf certificate for %q: %w", domain, err)
	}
	if err := leaf.VerifyHostname(domain); err != nil {
		return nil, fmt.Errorf("gateway: certificate for %q does not cover that name: %w", domain, err)
	}
	tlsCert.Leaf = leaf
	// The leaf is the only authority on expiry: overwrite the adapter's self-reported value
	// so renewal bookkeeping cannot be steered by a fabricated NotAfter.
	cert.NotAfter = leaf.NotAfter
	return &tlsCert, nil
}

// certRenewalActor receives the renewal schedule's cron tick and delegates to
// Manager.renewAll. It exists only because ScheduleWithCron delivers to an actor.
type certRenewalActor struct {
	manager *Manager
}

// enforce compilation error
var _ actor.Actor = (*certRenewalActor)(nil)

// PreStart is called before the actor starts.
func (a *certRenewalActor) PreStart(*actor.Context) error { return nil }

// Receive handles the renewal schedule's cron tick.
func (a *certRenewalActor) Receive(ctx *actor.ReceiveContext) {
	switch ctx.Message().(type) {
	case *emptypb.Empty:
		a.manager.renewAll(ctx.Context())
	default:
		ctx.Unhandled()
	}
}

// PostStop is called when the actor is stopped.
func (a *certRenewalActor) PostStop(*actor.Context) error { return nil }
