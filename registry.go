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
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/tochemey/goakt/v4/actor"
	gerrors "github.com/tochemey/goakt/v4/errors"
	"github.com/tochemey/goakt/v4/log"
	"github.com/tochemey/goakt/v4/passivation"
)

// groupTopicPrefix namespaces the internal pub/sub topic a group's cross-node fan-out rides
// on. It leads with a NUL byte, the same reserved-control-byte trick evictReasonPrefix uses:
// WebSocket/SSE topic names are human-authored, printable strings, so no realistic
// application call to Join/Broadcast produces one that starts with "\x00". The prefix must be
// identical on every node, since Broadcast on one node has to reach every other node's local
// group members through the very same wire topic (see ensureBridge/deliverFromBridge) - unlike
// a per-process random salt, this fixed constant needs no coordination to stay consistent
// across the cluster.
const groupTopicPrefix = "\x00goaktGatewayGroup\x00"

// ownerLeaseTTL is the lease length Registry asks a configured owner lease Coordinator for
// (see WithOwnerLease). It mirrors defaultPresenceTTL: renewing at a third of it keeps a live
// connection's lease from lapsing under ordinary network jitter while bounding how long a
// genuinely dead node's stale ownership lingers before a takeover (or this node's own next
// Register for the same id) can claim it.
//
// It is a var, not a const, solely so a white-box test can shrink it and observe
// renewOwnerLeases' real background ticker fire within a test timeout instead of the
// production 30-second cadence; production code never assigns to it.
var ownerLeaseTTL = 30 * time.Second

// ownerLeaseStaleEvictReason is the close reason given to a locally held connection when this
// node's background lease renewal discovers another node has taken over its owner lease -
// distinct from takeoverEvictReason, which is what the *new* owner's takeover loop hands the
// connection it evicts directly; this is what the dispossessed *old* owner uses to explain the
// forced close to its own client once it notices the takeover on its own.
const ownerLeaseStaleEvictReason = "connection owner lease was taken over by another node"

// defaultPresenceTTL is the lease length Registry asks a Presence backend for. The
// registry renews at a third of it, so two consecutive renewals may be lost before a live
// connection is wrongly considered offline.
const defaultPresenceTTL = 30 * time.Second

// defaultConfirmationTimeout is how long a confirmed cross-node delivery waits for the
// owning node's acknowledgement before failing with ErrConfirmationTimeout. See
// WithConfirmationTimeout.
const defaultConfirmationTimeout = 5 * time.Second

// takeoverEvictTimeout and takeoverEvictPoll bound a cross-node reconnect takeover.
// connActorName(id) is a cluster-unique actor name owned by whichever node currently holds
// the connection, so when a takeover lands on a different node its Spawn is rejected with
// ErrActorAlreadyExists until the owning node releases the name. The new node repeatedly
// asks the old owner to evict and retries the Spawn on this cadence until the name frees up
// or the timeout elapses, at which point it gives up with ErrTakeoverTimeout rather than
// waiting forever.
const (
	takeoverEvictTimeout = 10 * time.Second
	takeoverEvictPoll    = 150 * time.Millisecond
)

// takeoverEvictReason is the close reason handed to a connection evicted by a cross-node
// takeover, so the displaced client learns why its socket was closed.
const takeoverEvictReason = "connection replaced by a new session on another node"

// maxConcurrentConfirmAsks caps the number of in-flight confirmation Asks a single confirmed
// SendToGroup issues at once. Confirmed group delivery Asks every remote member and each Ask
// can block for the full confirmation timeout; issuing them serially made a group with many
// unreachable members block for members x timeout. Bounding the fan-out keeps a large group
// responsive without unbounded goroutine or connection pressure.
const maxConcurrentConfirmAsks = 64

// ErrTakeoverTimeout is returned by Registry.Register when a cross-node reconnect takeover
// (WithReplaceExisting) could not evict the connection's previous owner on another node
// within takeoverEvictTimeout, so the cluster-unique actor name never freed up. It lives
// here rather than in errors.go because it is internal to the registry lifecycle.
var ErrTakeoverTimeout = errors.New("gateway: cross-node connection takeover timed out")

// groupTopic derives the internal topic name that carries a group's cross-node fan-out.
func groupTopic(group string) string {
	return fmt.Sprintf("%s-%s", groupTopicPrefix, group)
}

// connEntry is the bookkeeping Registry keeps for one locally registered connection.
type connEntry struct {
	id     string
	group  string
	send   func([]byte) error
	pid    *actor.PID
	topics map[string]struct{}
	meta   map[string]string

	// generation is the owner lease generation Register acquired for this entry (see
	// WithOwnerLease), set once and fixed for the entry's lifetime same as pid. It is 0 whenever
	// no lease is configured, which every fencing check treats as "always current" - the
	// zero-cost default this package's opt-in features all share.
	//
	// It is an atomic.Uint64, not a plain uint64, because the entry is published into r.conns
	// (and so becomes visible to a concurrent SendToConnection/Ack/deliverOne, none of which
	// wait on entry.reserved) by reserve, well before register's own later critical section
	// assigns this field; a plain field would make that assignment race any such concurrent
	// reader under go test -race, even though every read that matters happens-after the
	// generation is actually current in practice.
	generation atomic.Uint64

	// replayBarrier is non-nil only while an Outbox reconnect replay (see
	// Registry.resendUnacked) is in flight for this entry, and is closed once it finishes. It
	// is set before the entry becomes reachable by any delivery path (reserve publishes the
	// entry before Register clears its reservation), so every deliverer that resolves this
	// entry - local SendToConnection, group/topic fan-out, a remote connActor write - blocks
	// behind it via awaitReplayBarrier. That is what stops a fresh, real-time message from
	// reaching the socket ahead of the unacknowledged tail a reconnecting client is owed in
	// order. It stays nil, and awaitReplayBarrier is then a no-op, whenever no Outbox is
	// configured.
	replayBarrier chan struct{}

	// closeHook, when set by the handler that owns the socket (via WithConnCloseHook),
	// force-closes the connection out of band for Registry.Disconnect. It is read and
	// invoked under r.mu-free access after being snapshotted, so it must be safe to call
	// concurrently with the connection's normal teardown; the handler makes the close frame
	// it sends idempotent against an already-closing socket.
	closeHook func(reason string)

	// reserved is true from the moment Register publishes this entry under the id
	// until its backing actor has finished spawning. It lets a concurrent Unregister
	// for the same id detect the in-flight registration instead of racing past it.
	reserved bool
	// dead is set by Unregister when it finds the id still reserved: it tells the
	// in-flight Register to roll back the spawn instead of finalizing an entry the
	// caller of Unregister already believes is gone.
	dead bool
}

// topicBridge is the cluster-wide fan-out side of a locally joined topic or group: a
// topicSubscription (see bridge.go) that receives publishes from every node in the
// cluster, including this one's own echo, and re-delivers them to this node's local
// members.
type topicBridge struct {
	subscription *topicSubscription
	members      int
}

// Registry is the local, per-node table of registered WebSocket/SSE connections. It is
// the "local connection table" the two-tier delivery model described in the gateway
// package documentation is built around: SendToConnection, SendToGroup and Broadcast
// always prefer a direct write to a locally held connection over any actor/cluster
// machinery, and only fall back to cluster addressing when the target is not held by this
// node.
//
// A Registry is safe for concurrent use.
type Registry struct {
	system actor.ActorSystem
	logger log.Logger
	origin uuid.UUID
	// nodeID identifies this process to the owner lease mechanism (see WithOwnerLease). It is
	// origin.String(): origin is already a fresh random UUID minted once per Registry, so it
	// doubles as a node identity without needing a separate configuration knob.
	nodeID string

	presence    Presence
	presenceTTL time.Duration
	observer    Observer

	// offline, outbox and the confirmation settings back the opt-in reliability features.
	// All are nil/false by default, which keeps the default delivery path allocation- and
	// latency-free: no persistence, no cross-node acknowledgement, no offline fallback.
	offline         OfflineChannel
	outbox          Outbox
	outboxEnvelope  bool
	confirmDelivery bool
	confirmTimeout  time.Duration

	// ownerLeaseCoordinator is the raw value passed to WithOwnerLease, before NewRegistry has
	// checked it implements LinearizableFencingCoordinator. lease is non-nil only once that
	// check has passed;
	// leaseUnsupported is set instead when it failed, so the first Register call can report
	// ErrOwnerLeaseUnsupported rather than NewRegistry (which has no error return) silently
	// leaving fencing off. A nil lease is also the normal, opt-out configuration: every fencing
	// check in this file treats it as "always succeeds, generation 0", which is what makes
	// WithOwnerLease a zero-cost default.
	ownerLeaseCoordinator Coordinator
	lease                 *ownerLease
	leaseUnsupported      bool

	mu     sync.RWMutex
	conns  map[string]*connEntry
	topics map[string]map[string]struct{} // topic -> set of local connection ids
	groups map[string]map[string]struct{} // group -> set of local connection ids
	closed bool

	bridgeMu sync.Mutex
	bridges  map[string]*topicBridge // wire topic -> cluster fan-out subscription
	// bridgesClosed is set by Close under bridgeMu. It stops an in-flight Register whose
	// finalize is still creating fan-out bridges from installing one after Close already
	// drained the bridge map, which would leak a subscription no one can ever release.
	bridgesClosed bool

	closeOnce sync.Once
	cancel    context.CancelFunc
	// bgWG tracks every background renewal goroutine (presence, owner lease) so Close can wait
	// for all of them to exit, however many are running.
	bgWG sync.WaitGroup

	offlineMu     sync.Mutex
	offlineCtx    context.Context
	offlineCancel context.CancelFunc
	offlineWG     sync.WaitGroup
	offlineClosed bool
}

// RegistryOption configures a Registry created with NewRegistry.
type RegistryOption func(*Registry)

// WithPresence attaches a cluster-wide presence backend. Without one a Registry only
// knows about the connections it holds itself, so IsOnline degrades to "connected to this
// node" and DeliveryResult.None cannot distinguish an offline identity from one connected
// elsewhere in the cluster.
func WithPresence(p Presence) RegistryOption {
	return func(r *Registry) { r.presence = p }
}

// WithPresenceTTL sets the lease length the Registry asks the Presence backend for.
// Presence entries are renewed every third of it, so a node that dies takes at most one
// TTL to be forgotten. Defaults to 30 seconds.
func WithPresenceTTL(d time.Duration) RegistryOption {
	return func(r *Registry) { r.presenceTTL = d }
}

// WithObserver attaches an Observer that receives the Registry's connection and delivery
// events. Its methods run inline on delivery paths and must not block.
func WithObserver(o Observer) RegistryOption {
	return func(r *Registry) { r.observer = o }
}

// WithDeliveryConfirmation makes cross-node delivery wait for the owning node to acknowledge
// that it wrote the payload to the target socket's outbound buffer, instead of the default
// fire-and-forget fan-out. With it enabled, DeliveryResult.Remote counts confirmed remote
// writes rather than fan-outs issued, and SendToConnection to a remote connection returns
// only once the write is acknowledged (or ErrConfirmationTimeout once the confirmation
// timeout elapses). It costs a round trip per remote delivery; leave it off for the
// low-latency at-most-once default.
func WithDeliveryConfirmation() RegistryOption {
	return func(r *Registry) { r.confirmDelivery = true }
}

// WithConfirmationTimeout sets how long a confirmed cross-node delivery waits for the owning
// node's acknowledgement before giving up with ErrConfirmationTimeout. It only has an effect
// alongside WithDeliveryConfirmation. Defaults to 5 seconds.
func WithConfirmationTimeout(d time.Duration) RegistryOption {
	return func(r *Registry) { r.confirmTimeout = d }
}

// WithOwnerLease turns on strict multi-instance connection ownership: Register acquires a
// CAS-arbitrated lease for each connection id from c before publishing it locally, takeovers
// (WithReplaceExisting) fence out the previous owner by bumping the lease's generation, and
// every fencing-aware operation (refresh, release, and a receiving connActor's delivery check)
// rejects a caller whose generation has been superseded.
//
// It exists because the GoAkt actor directory a Registry otherwise relies on to make a
// connection addressable cluster-wide is PA/EC eventually consistent (see the Coordinator doc
// comment): two nodes racing a takeover of the same connection id can both observe "no owner"
// and both succeed, producing two live owners for one id. A CAS primitive is what closes that
// window; the actor directory alone cannot.
//
// c must implement LinearizableFencingCoordinator. A c that does not makes every subsequent
// Register fail with ErrOwnerLeaseUnsupported, since NewRegistry itself has no error return to
// report the mismatch immediately.
//
// Without WithOwnerLease a Registry keeps its current single-instance semantics at zero cost:
// no lease acquisition, no generation fencing, no extra coordinator round trips. This mirrors
// WithDeliveryConfirmation's opt-in philosophy - pay for the guarantee only when you need it.
func WithOwnerLease(c Coordinator) RegistryOption {
	return func(r *Registry) { r.ownerLeaseCoordinator = c }
}

// NewRegistry creates a Registry backed by system. system is used to spawn the
// per-connection ephemeral actors (relocation disabled, long-lived passivation) that make
// registered connections addressable from other nodes, and to bridge topic and group
// fan-out across the cluster (see bridge.go).
//
// Joining a topic or registering into a group requires the actor system to have been
// started with pub/sub enabled (actor.WithPubSub): both ride on the topic actor.
//
// A Registry configured with a Presence backend starts a background renewal goroutine;
// call Close to stop it.
func NewRegistry(system actor.ActorSystem, logger log.Logger, opts ...RegistryOption) *Registry {
	if logger == nil {
		logger = log.DiscardLogger
	}
	origin := uuid.New()
	r := &Registry{
		system:      system,
		logger:      logger,
		origin:      origin,
		nodeID:      origin.String(),
		presenceTTL: defaultPresenceTTL,
		conns:       make(map[string]*connEntry),
		topics:      make(map[string]map[string]struct{}),
		groups:      make(map[string]map[string]struct{}),
		bridges:     make(map[string]*topicBridge),
	}
	for _, opt := range opts {
		opt(r)
	}
	if r.presenceTTL <= 0 {
		r.presenceTTL = defaultPresenceTTL
	}
	if r.confirmTimeout <= 0 {
		r.confirmTimeout = defaultConfirmationTimeout
	}
	if r.ownerLeaseCoordinator != nil {
		if fencing, ok := r.ownerLeaseCoordinator.(LinearizableFencingCoordinator); ok {
			r.lease = newOwnerLease(fencing, r.nodeID, ownerLeaseTTL)
		} else {
			r.leaseUnsupported = true
		}
	}
	if r.presence != nil || r.lease != nil {
		ctx, cancel := context.WithCancel(context.Background())
		r.cancel = cancel
		if r.presence != nil {
			r.bgWG.Add(1)
			go func() {
				defer r.bgWG.Done()
				r.renewPresence(ctx)
			}()
		}
		if r.lease != nil {
			r.bgWG.Add(1)
			go func() {
				defer r.bgWG.Done()
				r.renewOwnerLeases(ctx)
			}()
		}
	}
	if r.offline != nil {
		r.offlineCtx, r.offlineCancel = context.WithCancel(context.Background())
	}
	return r
}

// Close stops the Registry's background resources: the presence renewal goroutine and
// every cluster fan-out bridge it still holds. Registered connections are left alone -
// their handlers own them and will unregister them as their sockets die. Close is
// idempotent, and a closed Registry refuses further registrations with ErrRegistryClosed.
func (r *Registry) Close(_ context.Context) error {
	r.closeOnce.Do(func() {
		r.mu.Lock()
		r.closed = true
		r.mu.Unlock()

		if r.cancel != nil {
			r.cancel()
			r.bgWG.Wait()
		}
		r.offlineMu.Lock()
		r.offlineClosed = true
		if r.offlineCancel != nil {
			r.offlineCancel()
		}
		r.offlineMu.Unlock()
		r.offlineWG.Wait()

		r.bridgeMu.Lock()
		r.bridgesClosed = true
		bridges := r.bridges
		r.bridges = make(map[string]*topicBridge)
		r.bridgeMu.Unlock()

		for topic, bridge := range bridges {
			if err := bridge.subscription.Close(); err != nil {
				r.logger.Warnf("gateway: failed to close topic bridge for %q: %v", topic, err)
			}
		}
	})
	return nil
}

// renewPresence keeps this node's presence leases alive for as long as it holds the
// connections behind them.
func (r *Registry) renewPresence(ctx context.Context) {
	ticker := time.NewTicker(r.presenceTTL / 3)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			for _, entry := range r.groupedConnections() {
				if err := r.refreshPresence(ctx, entry); err != nil {
					if errors.Is(err, ErrStaleOwner) {
						// A takeover elsewhere already re-established this membership at a
						// higher generation (see PresenceFencer.RefreshGen): this node's own
						// renewOwnerLeases loop will notice the same takeover on its own lease
						// refresh and evict the local connection: nothing further to do here.
						continue
					}
					r.logger.Warnf("gateway: failed to refresh presence for connection %q in group %q: %v", entry.id, entry.group, err)
				}
			}
		}
	}
}

// refreshPresence renews entry's presence membership, fenced by entry.generation when the
// configured Presence backend implements PresenceFencer (see WithOwnerLease). Without a lease
// configured, generation is always 0 for every node, so the fenced call behaves exactly like the
// plain Refresh it replaces - the zero-cost default every owner-lease fencing check in this
// package shares. A backend that does not implement PresenceFencer is called through the plain,
// unfenced Refresh, unchanged from before WithOwnerLease existed.
func (r *Registry) refreshPresence(ctx context.Context, entry *connEntry) error {
	if fencer, ok := r.presence.(PresenceFencer); ok {
		return fencer.RefreshGen(ctx, entry.group, entry.id, entry.generation.Load(), r.presenceTTL)
	}
	return r.presence.Refresh(ctx, entry.group, entry.id, r.presenceTTL)
}

// leavePresence removes entry's presence membership, fenced by entry.generation when the
// configured Presence backend implements PresenceFencer - see refreshPresence for why this is
// always the zero-cost equivalent of plain Leave without a configured lease. The fencing is what
// stops a dispossessed owner's delayed teardown from deleting a takeover's already-restored
// membership (see PresenceFencer.LeaveGen).
func (r *Registry) leavePresence(ctx context.Context, entry *connEntry) error {
	if fencer, ok := r.presence.(PresenceFencer); ok {
		return fencer.LeaveGen(ctx, entry.group, entry.id, entry.generation.Load())
	}
	return r.presence.Leave(ctx, entry.group, entry.id)
}

// groupedConnections snapshots every fully registered local connection that belongs to a
// group.
func (r *Registry) groupedConnections() []*connEntry {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]*connEntry, 0, len(r.conns))
	for _, entry := range r.conns {
		if entry.reserved || entry.group == "" {
			continue
		}
		out = append(out, entry)
	}
	return out
}

// leasedConnections snapshots every fully registered local connection, regardless of group.
// Unlike groupedConnections, the owner lease applies to every connection Register hands a
// generation to - grouped or not - so this does not filter on entry.group.
func (r *Registry) leasedConnections() []*connEntry {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]*connEntry, 0, len(r.conns))
	for _, entry := range r.conns {
		if entry.reserved {
			continue
		}
		out = append(out, entry)
	}
	return out
}

// renewOwnerLeases keeps this node's owner lease current for every connection it locally
// holds, for as long as WithOwnerLease is configured. A refresh that comes back
// ErrStaleOwner means another node's takeover has already bumped the generation past what
// this node holds - this node is no longer the legitimate owner, whether or not the explicit
// cross-node evict Tell that normally accompanies a takeover has reached it yet - so the
// connection is evicted locally rather than left to keep serving traffic its owner lease no
// longer covers.
func (r *Registry) renewOwnerLeases(ctx context.Context) {
	ticker := time.NewTicker(ownerLeaseTTL / 3)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			for _, entry := range r.leasedConnections() {
				err := r.lease.refresh(ctx, entry.id, entry.generation.Load())
				if err == nil {
					continue
				}
				if errors.Is(err, ErrStaleOwner) {
					r.logger.Warnf("gateway: owner lease for connection %q was taken over by another node; evicting the local copy", entry.id)
					r.evictLocal(entry, ownerLeaseStaleEvictReason)
					continue
				}
				r.logger.Warnf("gateway: failed to refresh owner lease for connection %q: %v", entry.id, err)
			}
		}
	}
}

// registerConfig holds the per-connection settings a Register call accumulates from its
// RegisterOptions.
type registerConfig struct {
	group     string
	topics    []string
	meta      map[string]string
	replace   bool
	closeHook func(reason string)
}

// RegisterOption configures a single Registry.Register call.
type RegisterOption func(*registerConfig)

// WithConnGroup puts the connection in an identity group, e.g. "user:123". Every device
// and browser tab of one identity shares a group, which is what SendToGroup fans out to
// and what Presence tracks.
func WithConnGroup(group string) RegisterOption {
	return func(c *registerConfig) { c.group = group }
}

// WithConnTopics joins the connection to topics as part of registering it. A failure to
// bridge any of them fails the registration, since a connection silently missing
// broadcasts is worse than one that never came up.
func WithConnTopics(topics ...string) RegisterOption {
	return func(c *registerConfig) { c.topics = append(c.topics, topics...) }
}

// WithConnMeta attaches application metadata to the connection.
func WithConnMeta(meta map[string]string) RegisterOption {
	return func(c *registerConfig) { c.meta = meta }
}

// WithConnCloseHook records how to force-close this connection out of band, for
// Registry.Disconnect and Registry.DisconnectGroup. The WebSocket/SSE handlers pass it at
// registration time so the close is wired atomically with the entry, leaving no window in
// which a registered connection cannot be disconnected. hook is invoked with the disconnect
// reason from a goroutine that does not hold the Registry lock; it must be non-blocking and
// safe to call concurrently with the connection's own teardown. Applications that register
// connections directly, without a handler, simply omit it: Disconnect then has nothing to
// run and reports the connection as closable only if another node owns it.
func WithConnCloseHook(hook func(reason string)) RegisterOption {
	return func(c *registerConfig) { c.closeHook = hook }
}

// WithReplaceExisting evicts an already-registered connection with the same id instead of
// failing with ErrConnectionExists. This is the right default for a reconnecting client:
// behind a half-open TCP connection the previous socket can stay "alive" for many minutes,
// and refusing the new one would keep the client offline for exactly as long.
func WithReplaceExisting() RegisterOption {
	return func(c *registerConfig) { c.replace = true }
}

// registerSpawnBarrier, when non-nil, is invoked with id after Register has reserved id
// in the connection table and before it spawns the backing actor. It exists solely so
// tests can deterministically interleave a concurrent Unregister within that window;
// production code never sets it.
var registerSpawnBarrier func(id string)

// registerRollbackBarrier mirrors registerSpawnBarrier for the rollback path: when non-nil
// it is invoked with the connection id just before a failed registration is rolled back, so
// a test can deterministically interleave a takeover into the window between clearing the
// reservation and the rollback and prove the rollback keys on entry identity rather than id
// (the ABA guard). Production code never sets it.
var registerRollbackBarrier func(id string)

// unregisterTeardownBarrier, when non-nil, is invoked with the id inside teardownEntry after
// the entry has been removed from the connection table but before its backing actor is shut
// down. It lets a test hold a connection's actor alive - still owning its cluster-unique name -
// in the exact window a same-node reconnect racing the old socket's own death must contend
// with, so the evict-vs-reused-id ABA hazard can be driven deterministically. Production code
// never sets it.
var unregisterTeardownBarrier func(id string)

// evictLocalBarrier, when non-nil, is invoked with the id at the start of evictLocal so a test
// can observe a cross-node takeover's evict landing on this node - and thus that the buggy
// id-scoped path would already have fired - before it releases a parked teardown. Production
// code never sets it.
var evictLocalBarrier func(id string)

// ConnHandle is the entry-guarded handle Registry.RegisterHandle returns alongside a
// successful registration. It is bound to the exact connEntry that call created, the same
// entry identity removeEntryLocked already guards the internal rollback and cross-node
// eviction paths with, so UnregisterHandle can never tear down a newer connection that a
// same-id takeover has since installed.
type ConnHandle struct {
	registry *Registry
	entry    *connEntry
}

// UnregisterHandle removes exactly the connection h was issued for: the entry-guarded
// counterpart to Registry.Unregister(id). It is a safe no-op if a takeover has since replaced
// this connection under its id - h's own entry is no longer the current owner of that id, so
// there is nothing left for this specific handle to tear down. Callers that must remove
// whatever currently owns an id, regardless of which registration installed it, use the
// plain, id-scoped Unregister instead.
func (h *ConnHandle) UnregisterHandle(ctx context.Context) error {
	return h.registry.unregisterEntry(ctx, h.entry)
}

// Register adds a new connection to the local table and makes it addressable
// cluster-wide under connActorName(id). send is invoked (from any goroutine, including
// remote-delivery ones) whenever a payload must be written to the underlying socket; it
// must be non-blocking or perform its own internal buffering, since Registry never
// queues on the caller's behalf.
//
// The id is reserved in the table before the backing actor is spawned, so a concurrent
// Register for the same id fails immediately with ErrConnectionExists rather than racing
// past the reservation. If an Unregister for id arrives while the actor is still
// spawning, Register rolls back the spawn and returns ErrConnectionClosed instead of
// resurrecting a connection the Unregister caller already believes is gone.
//
// Anything that can leave the connection half-wired - a topic bridge that cannot be
// established, a presence backend that rejects the join, an owner lease held by another
// node - rolls the whole registration back and returns the error.
//
// It returns ErrConnectionExists if id is already registered and WithReplaceExisting was
// not given, and ErrRegistryClosed if the Registry has been closed. With WithOwnerLease
// configured it can also return ErrOwnerLeaseUnsupported (the configured Coordinator does
// not implement LinearizableFencingCoordinator) or ErrOwnerHeld (another node holds id's
// lease, unexpired, and WithReplaceExisting was not given).
func (r *Registry) Register(ctx context.Context, id string, send func([]byte) error, opts ...RegisterOption) error {
	_, err := r.register(ctx, id, send, opts...)
	return err
}

// RegisterHandle is Register with an entry-guarded handle for teardown (see ConnHandle). A
// caller - typically a WebSocket/SSE handler - that unregisters through
// ConnHandle.UnregisterHandle instead of the id-scoped Unregister closes the same ABA hazard
// the rollback and cross-node eviction paths already guard against for their own internal
// teardowns: a handler whose connection has since been evicted or replaced by a same-id
// takeover can never delete the newer owner's entry out from under it.
func (r *Registry) RegisterHandle(ctx context.Context, id string, send func([]byte) error, opts ...RegisterOption) (*ConnHandle, error) {
	entry, err := r.register(ctx, id, send, opts...)
	if err != nil {
		return nil, err
	}
	return &ConnHandle{registry: r, entry: entry}, nil
}

// register is the shared implementation behind Register and RegisterHandle; see Register's
// doc comment for its externally visible behaviour. It additionally returns the connEntry it
// published so RegisterHandle can bind a ConnHandle to it.
func (r *Registry) register(ctx context.Context, id string, send func([]byte) error, opts ...RegisterOption) (*connEntry, error) {
	if r.leaseUnsupported {
		return nil, ErrOwnerLeaseUnsupported
	}

	cfg := &registerConfig{}
	for _, opt := range opts {
		opt(cfg)
	}

	entry := &connEntry{
		id:    id,
		group: cfg.group,
		send:  send,
		// The caller keeps ownership of the meta map it passed; copy it so a later mutation
		// on their side cannot race the registry's reads of it (presence join, snapshots).
		meta:      copyMeta(cfg.meta),
		closeHook: cfg.closeHook,
		topics:    make(map[string]struct{}),
		reserved:  true,
	}
	if r.outbox != nil {
		entry.replayBarrier = make(chan struct{})
		defer func() {
			// The normal success path below closes this once resendUnacked's replay has
			// finished writing the unacked tail, which makes this a no-op here. Every other
			// return path (a failed lease acquire, spawn, or finalize) never reaches that
			// close, so this is what releases a deliverer that started waiting on the barrier
			// before it can know registration failed and the entry was torn down - otherwise it
			// would block forever on a channel nothing else would ever close.
			select {
			case <-entry.replayBarrier:
			default:
				close(entry.replayBarrier)
			}
		}()
	}

	replaced, err := r.reserve(ctx, id, entry, cfg.replace)
	if err != nil {
		return nil, err
	}

	if registerSpawnBarrier != nil {
		registerSpawnBarrier(id)
	}

	// The owner lease is the authoritative, CAS-arbitrated cross-node ownership decision (see
	// WithOwnerLease's doc comment): it is acquired regardless of what the local reservation
	// above just decided, because that reservation only ever sees this node's own table.
	// Without a configured lease this always succeeds with generation 0, the zero-cost default.
	acq, leaseErr := r.acquireEntryLease(ctx, id, cfg.replace)
	generation := acq.generation

	var pid *actor.PID
	var crossNodeReplaced bool
	var spawnErr error
	if leaseErr == nil {
		pid, crossNodeReplaced, spawnErr = r.spawnConnActor(ctx, id, entry, cfg.replace)
		replaced = replaced || crossNodeReplaced
	}

	r.mu.Lock()
	if entry.dead || leaseErr != nil || spawnErr != nil {
		// The lease acquire failed, the spawn failed, or a concurrent Unregister already
		// claimed id while it was still reserved: undo the reservation instead of finalizing it.
		delete(r.conns, id)
		r.mu.Unlock()
		if leaseErr == nil && r.lease != nil {
			if spawnErr != nil {
				// The lease was granted (possibly preempting a live, unexpired owner - see
				// acquire's takeover branch) but the physical takeover never actually completed:
				// restore whatever owned the connection before this call, instead of leaving our
				// own failed claim permanently fencing it out (see ownerLease.abortTakeover).
				if abortErr := r.lease.abortTakeover(ctx, id, acq); abortErr != nil {
					r.logger.Warnf("gateway: failed to restore owner lease for connection %q after a failed takeover: %v", id, abortErr)
				}
			} else if relErr := r.lease.release(ctx, id, generation); relErr != nil {
				// The spawn actually succeeded (we are the confirmed new owner) and only a
				// concurrent Unregister aborted this registration: give the lease back rather
				// than leaving it idle until the coordinator's retention window elapses.
				r.logger.Warnf("gateway: failed to release owner lease for connection %q after a failed registration: %v", id, relErr)
			}
		}
		if leaseErr != nil {
			return nil, leaseErr
		}
		if spawnErr != nil {
			return nil, spawnErr
		}
		if shutdownErr := pid.Shutdown(ctx); shutdownErr != nil {
			r.logger.Warnf("gateway: failed to shut down actor for concurrently unregistered connection %q: %v", id, shutdownErr)
		}
		return nil, ErrConnectionClosed
	}
	entry.pid = pid
	entry.generation.Store(generation)
	if entry.group != "" {
		r.addGroupMemberLocked(entry.group, id)
	}
	// The entry stays reserved across the bridge/presence setup below. A concurrent
	// Unregister that lands in this window marks the reservation dead and defers teardown
	// to us rather than racing it: if it ran now it would releaseBridge before we have
	// created the group bridge (a no-op) and then our ensureBridge would be left orphaned
	// under an id no live entry can ever release.
	r.mu.Unlock()

	// The physical takeover (or fresh spawn) is now confirmed: raise the Outbox's Ack fencing
	// floor to this generation before any traffic resumes, so a stale owner's in-flight Ack at
	// its own (lower) generation is rejected even if it arrives before this connection's first
	// real Ack would otherwise have raised the floor itself (see OutboxGenerationAdvancer). A
	// generation of 0 (no lease configured) and an Outbox that does not implement the optional
	// capability both make this a no-op, the zero-cost default every opt-in feature here shares.
	if generation != 0 && r.outbox != nil {
		if advancer, ok := r.outbox.(OutboxGenerationAdvancer); ok {
			if err := advancer.AdvanceGeneration(ctx, id, generation); err != nil {
				r.rollback(ctx, entry, err)
				return nil, err
			}
		}
	}

	if replaced {
		r.observeReplaced(id, entry.group)
	}

	setupErr := r.finalizeRegistration(ctx, entry, cfg.topics)

	r.mu.Lock()
	dead := entry.dead
	entry.reserved = false
	r.mu.Unlock()

	if setupErr != nil {
		r.rollback(ctx, entry, setupErr)
		return nil, setupErr
	}
	if dead {
		r.rollback(ctx, entry, ErrConnectionClosed)
		return nil, ErrConnectionClosed
	}

	r.observeRegistered(id, entry.group)

	// A reconnecting client catches up on the tail its previous socket never acknowledged.
	// This runs only when an Outbox is configured; it is a no-op otherwise. Every delivery
	// path that resolves this entry blocks behind entry.replayBarrier (see awaitReplayBarrier)
	// until this call returns and the deferred close above runs, so a fresh message can never
	// overtake the replay on the wire.
	r.resendUnacked(ctx, entry)
	return entry, nil
}

// acquireEntryLease obtains this node's owner lease acquisition for id, or short-circuits to
// the zero leaseAcquisition (generation 0, no error) when no lease is configured - the default,
// zero-cost single-instance semantics WithOwnerLease's doc comment promises. takeover forces a
// preemptive acquisition (WithReplaceExisting) that fences out whichever node currently holds
// the lease, including one this node's local connection table has no entry for: the lease is
// the cross-node ownership decision the eventually consistent actor directory alone cannot
// make safely. The returned leaseAcquisition also carries what it replaced, which register uses
// to abort a takeover whose physical eviction never completes (see ownerLease.abortTakeover).
func (r *Registry) acquireEntryLease(ctx context.Context, id string, takeover bool) (leaseAcquisition, error) {
	if r.lease == nil {
		return leaseAcquisition{}, nil
	}
	return r.lease.acquireDetailed(ctx, id, takeover)
}

// spawnConnActor spawns the per-connection actor addressable under connActorName(id). The
// name is unique cluster-wide, so on a reconnect takeover that lands on a different node
// than the one still holding the previous socket the Spawn is rejected with
// ErrActorAlreadyExists. When the caller opted into a takeover (WithReplaceExisting), this
// asks the current owner to evict its connection - releasing the name once its actor's
// PostStop runs - and retries the Spawn until the name frees up or takeoverEvictTimeout
// elapses. It reports whether a cross-node eviction happened so Register can observe the
// replacement.
func (r *Registry) spawnConnActor(ctx context.Context, id string, entry *connEntry, replace bool) (*actor.PID, bool, error) {
	pid, err := r.doSpawn(ctx, id, entry)
	if err == nil {
		return pid, false, nil
	}
	if !replace || !errors.Is(err, gerrors.ErrActorAlreadyExists) {
		return nil, false, err
	}

	deadline := time.Now().Add(takeoverEvictTimeout)
	for {
		// Re-issue the evict every poll: the request is a fire-and-forget Tell and is
		// idempotent on the owning node, so re-sending it makes the takeover robust against
		// a dropped message and against cluster-directory propagation lag after the owner
		// released the name.
		if evErr := r.requestRemoteEvict(ctx, id); evErr != nil {
			return nil, false, evErr
		}

		select {
		case <-ctx.Done():
			return nil, false, ctx.Err()
		case <-time.After(takeoverEvictPoll):
		}

		pid, err = r.doSpawn(ctx, id, entry)
		if err == nil {
			return pid, true, nil
		}
		if !errors.Is(err, gerrors.ErrActorAlreadyExists) {
			return nil, false, err
		}
		if time.Now().After(deadline) {
			return nil, false, ErrTakeoverTimeout
		}
	}
}

// doSpawn spawns the backing connActor with the fixed per-connection lifecycle: no
// relocation (the socket dies with its node) and long-lived passivation (only Unregister
// stops it).
func (r *Registry) doSpawn(ctx context.Context, id string, entry *connEntry) (*actor.PID, error) {
	return r.system.Spawn(ctx, connActorName(id), newConnActor(r, id, entry),
		actor.WithRelocationDisabled(),
		actor.WithPassivationStrategy(passivation.NewLongLivedStrategy()),
	)
}

// requestRemoteEvict locates the node currently holding connActorName(id) and asks it to
// evict the connection so its cluster-unique name is released. A name that is already gone
// (ErrActorNotFound) is not an error: the retrying Spawn will succeed once the cluster
// directory catches up.
func (r *Registry) requestRemoteEvict(ctx context.Context, id string) error {
	pid, err := r.system.ActorOf(ctx, connActorName(id))
	if err != nil {
		if connectionActorUnavailable(err) {
			return nil
		}
		return err
	}
	err = r.system.NoSender().Tell(ctx, pid, wrapperspb.String(evictReasonPrefix+takeoverEvictReason))
	if connectionActorUnavailable(err) {
		return nil
	}
	return err
}

// evictLocal force-closes and fully unregisters the specific connection generation entry
// backs, so its cluster-unique actor name is released for a takeover landing on another node.
// It runs the close hook (if any) so the displaced socket is torn down, then unregisters off
// the connActor's own goroutine: Unregister shuts down that very actor, which would deadlock if
// run inline from its Receive. Doing the unregister here rather than relying on the socket
// handler makes the takeover deterministic even for connections registered without a handler
// or close hook.
//
// Every action keys on entry identity, not on the bare id. A connActor whose entry is no
// longer the current owner of the id - its own teardown already removed it and a newer
// registration has since reused the id, which happens when a same-node reconnect races the old
// socket's own death - must not fire the newcomer's close hook or unregister it. In that case
// the actor name this actor still holds is released by its own PostStop once its teardown
// finishes, so a takeover waiting on the name still makes progress; evicting here would only
// force-close a freshly reconnected, legitimate connection.
func (r *Registry) evictLocal(entry *connEntry, reason string) {
	if evictLocalBarrier != nil {
		evictLocalBarrier(entry.id)
	}
	r.mu.RLock()
	current, exists := r.conns[entry.id]
	isCurrent := exists && current == entry
	var hook func(string)
	if isCurrent {
		hook = entry.closeHook
	}
	r.mu.RUnlock()
	if !isCurrent {
		return
	}
	if hook != nil {
		hook(reason)
	}
	go func() {
		if err := r.evictEntry(context.Background(), entry); err != nil {
			r.logger.Warnf("gateway: failed to evict connection %q for a cross-node takeover: %v", entry.id, err)
		}
	}()
}

// evictEntry tears down entry for a cross-node takeover eviction, keying the teardown on entry
// identity so an eviction aimed at this connActor's own generation never touches a newer
// connection that has since reused the id (the same ABA guard removeEntryLocked gives the
// rollback path). If entry's registration is still in flight it defers to that Register by
// marking the reservation dead, exactly as Unregister does, since only the registering
// goroutine may safely stop the actor it is spawning.
func (r *Registry) evictEntry(ctx context.Context, entry *connEntry) error {
	r.mu.Lock()
	current, exists := r.conns[entry.id]
	if !exists || current != entry {
		r.mu.Unlock()
		return nil
	}
	if entry.reserved {
		entry.dead = true
		r.mu.Unlock()
		return nil
	}
	topicsToLeave, _ := r.removeEntryLocked(entry)
	r.mu.Unlock()
	return r.teardownEntry(ctx, entry, topicsToLeave)
}

// finalizeRegistration performs the cluster-facing setup that cannot run under r.mu: the
// group fan-out bridge, the requested topic joins, and the presence lease. The entry is
// still reserved while this runs so a concurrent Unregister defers teardown to Register
// instead of racing this setup.
func (r *Registry) finalizeRegistration(ctx context.Context, entry *connEntry, topics []string) error {
	if entry.group != "" {
		if err := r.ensureBridge(ctx, groupTopic(entry.group), r.groupDelivery(entry.group)); err != nil {
			return err
		}
	}
	for _, topic := range topics {
		if err := r.Join(ctx, entry.id, topic); err != nil {
			return err
		}
	}
	if r.presence != nil && entry.group != "" {
		if err := r.presenceJoin(ctx, entry); err != nil {
			return err
		}
	}
	return nil
}

// presenceJoin records the connection in the Presence backend, carrying its metadata when
// the backend implements PresenceMetaJoiner so GroupMembers can return it cluster-wide, and
// falling back to the metadata-less Join otherwise.
func (r *Registry) presenceJoin(ctx context.Context, entry *connEntry) error {
	if joiner, ok := r.presence.(PresenceMetaJoiner); ok && len(entry.meta) > 0 {
		return joiner.JoinWithMeta(ctx, entry.group, entry.id, entry.meta, r.presenceTTL)
	}
	return r.presence.Join(ctx, entry.group, entry.id, r.presenceTTL)
}

// reserve publishes entry under id, evicting an existing connection first when the caller
// asked for a takeover. It reports whether an eviction happened.
func (r *Registry) reserve(ctx context.Context, id string, entry *connEntry, replace bool) (bool, error) {
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return false, ErrRegistryClosed
	}
	existing, exists := r.conns[id]
	if !exists {
		r.conns[id] = entry
		r.mu.Unlock()
		return false, nil
	}
	// A reserved entry belongs to an in-flight Register that owns the actor it is
	// spawning; only that goroutine can safely tear it down, so a takeover cannot wait
	// for it.
	if !replace || existing.reserved {
		r.mu.Unlock()
		return false, ErrConnectionExists
	}
	r.mu.Unlock()

	if err := r.Unregister(ctx, id); err != nil {
		return false, err
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return false, ErrRegistryClosed
	}
	if _, taken := r.conns[id]; taken {
		return false, ErrConnectionExists
	}
	r.conns[id] = entry
	return true, nil
}

// rollback undoes a registration that could not be completed. It tears down the specific
// entry it was given, not merely whatever currently lives under the id: between the moment
// Register clears the reservation and the moment it decides to roll back, a concurrent
// takeover can have already unregistered this entry and published a fresh owner under the
// same id. Keying the teardown on entry identity (see removeEntryLocked) stops the stale
// rollback from tearing down that newer owner (the ABA hazard).
func (r *Registry) rollback(ctx context.Context, entry *connEntry, cause error) {
	if registerRollbackBarrier != nil {
		registerRollbackBarrier(entry.id)
	}
	if err := r.unregisterEntry(ctx, entry); err != nil {
		r.logger.Warnf("gateway: failed to roll back connection %q after %v: %v", entry.id, cause, err)
	}
}

// Unregister removes a connection from the local table, leaves every topic and group it
// had joined, and shuts down its backing actor. It is a no-op if id is not registered.
//
// If id is currently reserved by an in-flight Register (its actor is still spawning),
// Unregister marks the reservation dead and returns immediately: the in-flight Register
// observes this and rolls back the spawn itself, since it is the only side that can
// safely stop the actor it just created.
func (r *Registry) Unregister(ctx context.Context, id string) error {
	r.mu.Lock()
	entry, exists := r.conns[id]
	if !exists {
		r.mu.Unlock()
		return nil
	}
	if entry.reserved {
		entry.dead = true
		r.mu.Unlock()
		return nil
	}
	topicsToLeave, _ := r.removeEntryLocked(entry)
	r.mu.Unlock()

	return r.teardownEntry(ctx, entry, topicsToLeave)
}

// unregisterEntry is the identity-scoped Unregister the rollback path uses: it tears entry
// down only while entry is still the current owner of its id, so a rollback that races a
// takeover leaves the takeover's newer entry untouched.
func (r *Registry) unregisterEntry(ctx context.Context, entry *connEntry) error {
	r.mu.Lock()
	topicsToLeave, removed := r.removeEntryLocked(entry)
	r.mu.Unlock()
	if !removed {
		return nil
	}
	return r.teardownEntry(ctx, entry, topicsToLeave)
}

// removeEntryLocked drops entry from the connection table and its group index only if entry
// is still the current owner of its id, reporting whether it did and returning the topics it
// held so the caller can detach them outside r.mu. The compare-and-act on entry identity is
// what makes a stale rollback safe against a takeover that reused the id. The caller must
// hold r.mu for writing.
func (r *Registry) removeEntryLocked(entry *connEntry) (topics []string, removed bool) {
	current, exists := r.conns[entry.id]
	if !exists || current != entry {
		return nil, false
	}
	delete(r.conns, entry.id)
	topics = make([]string, 0, len(entry.topics))
	for topic := range entry.topics {
		topics = append(topics, topic)
	}
	if entry.group != "" {
		r.removeGroupMemberLocked(entry.group, entry.id)
	}
	return topics, true
}

// teardownEntry runs the out-of-lock cleanup for an entry already removed from the table:
// leaving its topics, releasing its group bridge and presence lease, notifying the Observer,
// and shutting down its backing actor.
func (r *Registry) teardownEntry(ctx context.Context, entry *connEntry, topicsToLeave []string) error {
	for _, topic := range topicsToLeave {
		r.detachTopic(entry.id, topic)
	}

	if entry.group != "" {
		r.releaseBridge(groupTopic(entry.group))
		if r.presence != nil {
			if err := r.leavePresence(ctx, entry); err != nil && !errors.Is(err, ErrStaleOwner) {
				// ErrStaleOwner here means a takeover already re-established this membership at
				// a higher generation (see PresenceFencer.LeaveGen): this teardown's own
				// generation trails it, so leaving the newer membership alone is the whole
				// point, not a failure to log.
				r.logger.Warnf("gateway: failed to remove connection %q from presence group %q: %v", entry.id, entry.group, err)
			}
		}
	}

	if r.lease != nil {
		// A no-op, by design, if a takeover elsewhere has already superseded entry.generation
		// (see ownerLease.release): this teardown must never be able to clobber a newer
		// owner's lease, whether it runs because this connection unregistered normally or
		// because a cross-node takeover evicted it.
		if err := r.lease.release(ctx, entry.id, entry.generation.Load()); err != nil {
			r.logger.Warnf("gateway: failed to release owner lease for connection %q: %v", entry.id, err)
		}
	}

	r.observeUnregistered(entry.id, entry.group)

	if entry.pid == nil {
		return nil
	}
	if unregisterTeardownBarrier != nil {
		unregisterTeardownBarrier(entry.id)
	}
	return entry.pid.Shutdown(ctx)
}

// Join adds an already-registered connection to a topic's local membership, creating
// the cluster fan-out bridge for that topic on first use. It returns ErrConnectionNotFound
// if id is not registered.
//
// A bridge that cannot be established fails the Join and leaves the connection out of the
// topic: a connection joined to a topic it can never receive broadcasts on is a silent
// data-loss bug, not a degraded mode.
func (r *Registry) Join(ctx context.Context, id, topic string) error {
	r.mu.Lock()
	entry, exists := r.conns[id]
	if !exists {
		r.mu.Unlock()
		return ErrConnectionNotFound
	}
	if _, alreadyJoined := entry.topics[topic]; alreadyJoined {
		r.mu.Unlock()
		return nil
	}
	r.mu.Unlock()

	// Acquire the bridge reference before recording membership. detachTopic releases a
	// reference only when it sees the id in r.topics, so a concurrent Leave/Unregister that
	// races this Join can only release a reference after this Join has recorded membership,
	// which is after the reference was taken. Taking the reference first therefore makes it
	// impossible for a detach to release a reference this Join has not yet acquired and then
	// have this Join re-create the bridge - the orphaned, member-less bridge leak.
	if err := r.ensureBridge(ctx, topic, r.topicDelivery(topic)); err != nil {
		return err
	}

	r.mu.Lock()
	current, exists := r.conns[id]
	if !exists || current != entry {
		// The connection was unregistered or replaced by a takeover while we were bridging;
		// drop the reference we took and leave the id to its current owner.
		r.mu.Unlock()
		r.releaseBridge(topic)
		return ErrConnectionNotFound
	}
	if _, alreadyJoined := entry.topics[topic]; alreadyJoined {
		// A concurrent Join for the same id/topic won the record; release our duplicate
		// reference so the bridge count stays equal to the membership count.
		r.mu.Unlock()
		r.releaseBridge(topic)
		return nil
	}
	entry.topics[topic] = struct{}{}
	if r.topics[topic] == nil {
		r.topics[topic] = make(map[string]struct{})
	}
	r.topics[topic][id] = struct{}{}
	r.mu.Unlock()
	return nil
}

// Leave removes an already-registered connection from a topic's local membership,
// tearing down the cluster fan-out bridge for that topic once its last local member
// leaves.
func (r *Registry) Leave(_ context.Context, id, topic string) error {
	r.mu.Lock()
	entry, exists := r.conns[id]
	if !exists {
		r.mu.Unlock()
		return ErrConnectionNotFound
	}
	delete(entry.topics, topic)
	r.mu.Unlock()

	r.detachTopic(id, topic)
	return nil
}

// detachTopic removes id from topic's local membership set and releases one bridge
// reference. Each successful Join took exactly one reference through ensureBridge, so the
// release is per-member: releasing only when the last member leaves would strand the
// references taken by every earlier member and leak the cluster subscription.
func (r *Registry) detachTopic(id, topic string) {
	r.mu.Lock()
	members, ok := r.topics[topic]
	_, wasMember := members[id]
	if ok {
		delete(members, id)
		if len(members) == 0 {
			delete(r.topics, topic)
		}
	}
	r.mu.Unlock()

	if !wasMember {
		return
	}
	r.releaseBridge(topic)
}

// addGroupMemberLocked indexes id under group. The caller must hold r.mu for writing.
func (r *Registry) addGroupMemberLocked(group, id string) {
	if r.groups[group] == nil {
		r.groups[group] = make(map[string]struct{})
	}
	r.groups[group][id] = struct{}{}
}

// removeGroupMemberLocked drops id from group's index. The caller must hold r.mu for
// writing.
func (r *Registry) removeGroupMemberLocked(group, id string) {
	members, ok := r.groups[group]
	if !ok {
		return
	}
	delete(members, id)
	if len(members) == 0 {
		delete(r.groups, group)
	}
}

// ensureBridge lazily creates the cluster fan-out bridge for wireTopic (see bridge.go) so
// that a publish on any node in the cluster reaches this node's local members, and
// reference-counts it so the subscription only goes away with its last local member.
// deliver re-delivers a received payload to those members.
func (r *Registry) ensureBridge(ctx context.Context, wireTopic string, deliver func(payload []byte, excludes map[string]struct{})) error {
	r.bridgeMu.Lock()
	defer r.bridgeMu.Unlock()

	if r.bridgesClosed {
		return ErrRegistryClosed
	}

	if b, exists := r.bridges[wireTopic]; exists {
		b.members++
		return nil
	}

	sub, err := subscribeTopic(ctx, r.system, wireTopic, func(_ context.Context, message proto.Message) {
		r.deliverFromBridge(deliver, message)
	})
	if err != nil {
		return err
	}
	r.bridges[wireTopic] = &topicBridge{subscription: sub, members: 1}
	return nil
}

// releaseBridge decrements the bridge's reference count for wireTopic and tears it down
// once no local connection needs it anymore.
func (r *Registry) releaseBridge(wireTopic string) {
	r.bridgeMu.Lock()
	defer r.bridgeMu.Unlock()

	b, exists := r.bridges[wireTopic]
	if !exists {
		return
	}
	b.members--
	if b.members > 0 {
		return
	}
	delete(r.bridges, wireTopic)
	if err := b.subscription.Close(); err != nil {
		r.logger.Warnf("gateway: failed to close topic bridge for %q: %v", wireTopic, err)
	}
}

// hasBridge reports whether a bridge already exists for wireTopic.
func (r *Registry) hasBridge(wireTopic string) bool {
	r.bridgeMu.Lock()
	defer r.bridgeMu.Unlock()
	_, exists := r.bridges[wireTopic]
	return exists
}

// deliverFromBridge is invoked whenever a message is published to a bridged topic anywhere
// in the cluster, including by this very node. It skips deliveries that originated here,
// since the publishing call already wrote to its own local members directly, and hands the
// rest to deliver.
func (r *Registry) deliverFromBridge(deliver func(payload []byte, excludes map[string]struct{}), message proto.Message) {
	bv, ok := message.(*wrapperspb.BytesValue)
	if !ok {
		return
	}
	origin, excludes, payload, ok := decodeEnvelope(bv.GetValue())
	if !ok {
		return
	}
	if bytes.Equal(origin, r.origin[:]) {
		return
	}
	deliver(payload, excludes)
}

// topicDelivery returns the bridge callback that writes a received payload to topic's
// local members. It runs off a cluster fan-out subscription's own goroutine, which carries no
// caller request context, so it fences deliveries against context.Background() rather than
// skipping the owner-lease check the way an unfenced local write would (see deliverAll).
func (r *Registry) topicDelivery(topic string) func(payload []byte, excludes map[string]struct{}) {
	return func(payload []byte, excludes map[string]struct{}) {
		r.deliverAll(context.Background(), r.snapshotMembers(r.topics, topic, excludes), payload)
	}
}

// groupDelivery returns the bridge callback that writes a received payload to group's
// local members. See topicDelivery for why it fences against context.Background().
func (r *Registry) groupDelivery(group string) func(payload []byte, excludes map[string]struct{}) {
	return func(payload []byte, excludes map[string]struct{}) {
		r.deliverAll(context.Background(), r.snapshotMembers(r.groups, group, excludes), payload)
	}
}

// snapshotMembers copies out the local connections indexed under key, minus the excluded
// ids, so that deliveries never run a caller-supplied send function while holding r.mu.
func (r *Registry) snapshotMembers(index map[string]map[string]struct{}, key string, excludes map[string]struct{}) []*connEntry {
	r.mu.RLock()
	defer r.mu.RUnlock()

	ids := index[key]
	entries := make([]*connEntry, 0, len(ids))
	for id := range ids {
		if _, excluded := excludes[id]; excluded {
			continue
		}
		if entry, ok := r.conns[id]; ok {
			entries = append(entries, entry)
		}
	}
	return entries
}

// awaitReplayBarrier blocks until any Outbox reconnect replay in flight for entry (see
// connEntry.replayBarrier and Registry.resendUnacked) has finished writing its tail, so a
// fresh delivery can never overtake it on the wire. It is the no-op the common case needs:
// entry.replayBarrier is nil whenever no Outbox is configured, or once any past replay for
// this entry has already completed, and a receive on a nil channel is deliberately not
// attempted (it would block forever, not return immediately).
func awaitReplayBarrier(entry *connEntry) {
	if entry.replayBarrier == nil {
		return
	}
	<-entry.replayBarrier
}

// deliverAll writes payload to every entry and reports what happened. It is deliverOne run over
// a batch, so every local delivery path - not just a single-connection send - gets the same
// owner-lease fencing check (see deliverOne); before that check was centralized here, a group or
// topic fan-out reaching a not-yet-evicted, superseded local entry (see WithOwnerLease) could
// still deliver on the old owner's behalf even though the cross-node connActor path already
// rejected the same generation.
func (r *Registry) deliverAll(ctx context.Context, entries []*connEntry, payload []byte) DeliveryResult {
	var result DeliveryResult
	for _, entry := range entries {
		if err := r.deliverOne(ctx, entry, payload); err != nil {
			result.Dropped++
			continue
		}
		result.Delivered++
	}
	return result
}

// SendToConnection delivers payload to the connection identified by id anywhere in the
// cluster. If id is registered on this node, payload is written directly to the socket
// with no actor or cluster machinery involved. Otherwise, SendToConnection resolves the
// connection's owning node through the cluster-aware actor directory
// (ActorSystem.ActorOf) and delivers payload there.
//
// It returns ErrConnectionNotFound if id is not registered anywhere in the cluster.
func (r *Registry) SendToConnection(ctx context.Context, id string, payload []byte) error {
	// With an Outbox configured the payload is persisted before it is written, so a send
	// that never reaches the client is redelivered when the connection registers again. The
	// record is kept whether the target is local or remote; it is the sending node that owns
	// the at-least-once bookkeeping. Without an Outbox this branch is skipped entirely.
	if r.outbox != nil {
		msgID, seq, err := r.outbox.Append(ctx, id, payload)
		if err != nil {
			return err
		}
		payload, err = r.outboxWirePayload(msgID, seq, payload)
		if err != nil {
			return err
		}
	}

	r.mu.RLock()
	entry, ok := r.conns[id]
	r.mu.RUnlock()
	if ok {
		return r.deliverOne(ctx, entry, payload)
	}

	pid, err := r.system.ActorOf(ctx, connActorName(id))
	if err != nil {
		if connectionActorUnavailable(err) {
			return ErrConnectionNotFound
		}
		return err
	}

	return r.deliverRemote(ctx, pid, payload)
}

func (r *Registry) outboxWirePayload(msgID string, seq uint64, payload []byte) ([]byte, error) {
	if !r.outboxEnvelope {
		return payload, nil
	}
	return EncodeOutboxTextEnvelope(msgID, seq, payload)
}

// deliverRemote writes payload to a remote connActor. On the default path it is a
// fire-and-forget Tell. With WithDeliveryConfirmation it is an Ask that waits for the owning
// node to acknowledge the write, returning ErrConfirmationTimeout on timeout and
// ErrConnectionClosed when the remote node reports the socket write itself failed.
func (r *Registry) deliverRemote(ctx context.Context, pid *actor.PID, payload []byte) error {
	if !r.confirmDelivery {
		err := r.system.NoSender().Tell(ctx, pid, wrapperspb.Bytes(payload))
		if connectionActorUnavailable(err) {
			return ErrConnectionNotFound
		}
		return err
	}
	ok, err := r.askRemote(ctx, pid, payload)
	if err != nil {
		if connectionActorUnavailable(err) {
			return ErrConnectionNotFound
		}
		return err
	}
	if !ok {
		return ErrConnectionClosed
	}
	return nil
}

// connectionActorUnavailable normalizes a directory miss and a stale PID that has already
// started shutdown. Both mean the connection is no longer reachable, even though the actor
// directory can expose the stale PID briefly while its termination propagates.
func connectionActorUnavailable(err error) bool {
	return errors.Is(err, gerrors.ErrActorNotFound) || errors.Is(err, gerrors.ErrDead)
}

// askRemote sends payload to a remote connActor and waits for its ack under the confirmation
// timeout. It reports whether the remote node wrote the payload to its socket, mapping an
// unanswered Ask to ErrConfirmationTimeout.
func (r *Registry) askRemote(ctx context.Context, pid *actor.PID, payload []byte) (bool, error) {
	resp, err := r.system.NoSender().Ask(ctx, pid, wrapperspb.Bytes(payload), r.confirmTimeout)
	if err != nil {
		if errors.Is(err, gerrors.ErrRequestTimeout) {
			return false, ErrConfirmationTimeout
		}
		return false, err
	}
	ack, ok := resp.(*wrapperspb.BoolValue)
	if !ok {
		return false, nil
	}
	return ack.GetValue(), nil
}

// deliverOne writes payload to a single local connection and reports the outcome to the
// Observer.
//
// It fences the write against entry's owner-lease generation first (see Registry.staleOwner):
// without this check, a local entry a cross-node takeover has already superseded but not yet
// evicted would stay fully deliverable through every local delivery path - SendToConnection's
// fast path, and SendToGroup/Broadcast's local fan-out via deliverAll - even though the
// cross-node connActor path (connection.go's Receive) already rejects the same generation. This
// is what closes that gap for every local caller uniformly, rather than requiring each one to
// remember to check separately.
func (r *Registry) deliverOne(ctx context.Context, entry *connEntry, payload []byte) error {
	if r.staleOwner(ctx, entry.id, entry.generation.Load()) {
		return ErrStaleOwner
	}
	awaitReplayBarrier(entry)
	err := entry.send(payload)
	switch {
	case err == nil:
		return nil
	case errors.Is(err, ErrBackpressure):
		r.observeDropped(entry.id, entry.group)
	default:
		r.observeFailed(entry.id, err)
	}
	return err
}

// SendToGroup delivers payload to every connection of an identity group, cluster-wide.
// Local members are written to directly; the remaining nodes are reached through the
// group's internal fan-out topic.
//
// The returned DeliveryResult is what an application uses to decide whether the identity was
// reachable at all: DeliveryResult.None is the signal that an offline channel (web push,
// mail) is the only way left. How exact that signal is depends on configuration, and the
// precise contract - including why a stale Presence lease can suppress it and why the
// confirmed path can duplicate onto the offline channel - is documented on DeliveryResult.None
// and DeliveryResult.Remote. In short: exact reachability needs WithDeliveryConfirmation, and
// even then the offline fallback is at-least-once.
func (r *Registry) SendToGroup(ctx context.Context, group string, payload []byte) (DeliveryResult, error) {
	local := r.snapshotMembers(r.groups, group, nil)
	result := r.deliverAll(ctx, local, payload)

	if r.confirmDelivery && r.presence != nil {
		// Confirmation needs to address each remote member individually, which only a
		// Presence backend can enumerate. Remote then counts acknowledged remote writes
		// rather than a single fan-out.
		confirmed, err := r.confirmRemoteGroup(ctx, group, payload)
		if err != nil {
			return result, err
		}
		result.Remote = confirmed
	} else {
		remote, err := r.remoteGroupMembers(ctx, group)
		if err != nil {
			return result, err
		}
		if remote > 0 {
			if err := r.publish(ctx, groupTopic(group), payload, nil); err != nil {
				return result, err
			}
			result.Remote = remote
		}
	}

	// The identity is unreachable over any socket in the cluster: hand it to the offline
	// channel if one is configured. This is a no-op when the delivery reached something or
	// no OfflineChannel was set.
	r.maybeOfflineFallback(group, result, payload)
	return result, nil
}

// confirmRemoteGroup delivers payload to each of group's members that live on another node,
// waiting for each owning node to acknowledge the write, and returns how many acknowledged.
// A member the cluster can no longer resolve, or one that times out or errors, simply does
// not count; only a Presence backend failure aborts the fan-out.
func (r *Registry) confirmRemoteGroup(ctx context.Context, group string, payload []byte) (int, error) {
	members, err := r.presence.Members(ctx, group)
	if err != nil {
		return 0, err
	}

	r.mu.RLock()
	localIDs := r.groups[group]
	remoteIDs := make([]string, 0, len(members))
	for _, id := range members {
		if _, isLocal := localIDs[id]; !isLocal {
			remoteIDs = append(remoteIDs, id)
		}
	}
	r.mu.RUnlock()

	// Ask every remote member concurrently under a bounded fan-out. Each Ask can block for
	// the full confirmation timeout, so a serial loop over a group with many unreachable
	// members would stall for members x timeout; the semaphore keeps the concurrency (and
	// thus goroutine and connection pressure) capped while still overlapping the waits.
	var confirmed int64
	var wg sync.WaitGroup
	sem := make(chan struct{}, maxConcurrentConfirmAsks)
	for _, id := range remoteIDs {
		select {
		case sem <- struct{}{}:
		case <-ctx.Done():
			wg.Wait()
			return int(confirmed), ctx.Err()
		}
		wg.Add(1)
		go func(id string) {
			defer wg.Done()
			defer func() { <-sem }()

			pid, err := r.system.ActorOf(ctx, connActorName(id))
			if err != nil {
				if !connectionActorUnavailable(err) {
					r.logger.Warnf("gateway: failed to resolve remote group member %q for confirmed delivery: %v", id, err)
				}
				return
			}
			ok, err := r.askRemote(ctx, pid, payload)
			if err != nil {
				if !connectionActorUnavailable(err) {
					r.logger.Warnf("gateway: confirmed delivery to remote group member %q failed: %v", id, err)
				}
				return
			}
			if ok {
				atomic.AddInt64(&confirmed, 1)
			}
		}(id)
	}
	wg.Wait()
	return int(confirmed), nil
}

// remoteGroupMembers estimates how many members of group live on other nodes. With a Presence
// backend it returns the reported remote membership, which is a lease-based estimate that can
// count a member on a just crashed node until its lease lapses; without one, all a clustered
// node can say is "there may be members elsewhere", which counts as a single fan-out. Exact
// remote reachability needs the confirmed path (confirmRemoteGroup), not this estimate.
func (r *Registry) remoteGroupMembers(ctx context.Context, group string) (int, error) {
	if r.presence == nil {
		if r.system.InCluster() {
			return 1, nil
		}
		return 0, nil
	}

	members, err := r.presence.Members(ctx, group)
	if err != nil {
		return 0, err
	}

	r.mu.RLock()
	localIDs := r.groups[group]
	remote := 0
	for _, id := range members {
		if _, isLocal := localIDs[id]; !isLocal {
			remote++
		}
	}
	r.mu.RUnlock()
	return remote, nil
}

// broadcastConfig holds the settings a Broadcast call accumulates from its
// BroadcastOptions.
type broadcastConfig struct {
	exclude []string
}

// BroadcastOption configures a single Registry.Broadcast call.
type BroadcastOption func(*broadcastConfig)

// WithExclude keeps the given connection ids out of a broadcast, typically the sender's
// own connection. The exclusion travels with the payload, so it holds on every node, not
// just the one the Broadcast was issued from.
func WithExclude(ids ...string) BroadcastOption {
	return func(c *broadcastConfig) { c.exclude = append(c.exclude, ids...) }
}

// Broadcast delivers payload to every connection joined to topic across the cluster.
// Local members are written to directly; remote members are reached through the topic
// bridge (see bridge.go).
func (r *Registry) Broadcast(ctx context.Context, topic string, payload []byte, opts ...BroadcastOption) (DeliveryResult, error) {
	cfg := &broadcastConfig{}
	for _, opt := range opts {
		opt(cfg)
	}
	excludes := excludeSet(cfg.exclude)

	members := r.snapshotMembers(r.topics, topic, excludes)
	result := r.deliverAll(ctx, members, payload)
	r.observeFanout(topic, len(members))

	if !r.system.InCluster() && !r.hasBridge(topic) {
		// no cluster and no local bridge means there is nothing else to reach.
		return result, nil
	}

	if err := r.publish(ctx, topic, payload, cfg.exclude); err != nil {
		return result, err
	}
	if r.system.InCluster() {
		result.Remote++
	}
	return result, nil
}

// publish hands an envelope to the cluster's topic actor, which disseminates it to every
// node subscribed to wireTopic. It is a no-op when the actor system has no pub/sub.
func (r *Registry) publish(ctx context.Context, wireTopic string, payload []byte, excludes []string) error {
	topicActorPID := r.system.TopicActor()
	if topicActorPID == nil {
		return nil
	}
	envelope := encodeEnvelope(r.origin, excludes, payload)
	return r.system.NoSender().Tell(ctx, topicActorPID, actor.NewPublish(uuid.NewString(), wireTopic, wrapperspb.Bytes(envelope)))
}

// Disconnect force-closes the connection identified by id, wherever in the cluster it is
// held, sending the given reason to the client. A locally held connection is closed through
// the close hook its handler registered (see WithConnCloseHook); a connection held by
// another node is reached through its connActor, which runs its own local close hook. The
// socket's handler unregisters the connection as it tears down, so Disconnect does not
// unregister it itself.
//
// It returns ErrConnectionNotFound when id is not registered anywhere in the cluster. A
// locally held connection whose handler registered no close hook is reported as closed
// (there is nothing to force), since the caller's intent - that this node stop holding the
// socket - is already the handler's responsibility.
func (r *Registry) Disconnect(ctx context.Context, id, reason string) error {
	r.mu.RLock()
	entry, isLocal := r.conns[id]
	var hook func(string)
	if isLocal {
		hook = entry.closeHook
	}
	r.mu.RUnlock()

	if isLocal {
		if hook != nil {
			hook(reason)
		}
		return nil
	}

	pid, err := r.system.ActorOf(ctx, connActorName(id))
	if err != nil {
		if connectionActorUnavailable(err) {
			return ErrConnectionNotFound
		}
		return err
	}
	err = r.system.NoSender().Tell(ctx, pid, wrapperspb.String(reason))
	if connectionActorUnavailable(err) {
		return ErrConnectionNotFound
	}
	return err
}

// DisconnectGroup force-closes every connection of an identity group across the cluster,
// sending reason to each client, and returns how many connections it acted on. Local
// members are closed through their registered close hooks; remote members are reached
// through their connActors when a Presence backend can enumerate them. The count reflects
// the connections Disconnect was dispatched for, not a cluster-wide confirmation that every
// socket has finished closing.
func (r *Registry) DisconnectGroup(ctx context.Context, group, reason string) (int, error) {
	r.mu.RLock()
	localIDs := make([]string, 0, len(r.groups[group]))
	hooks := make([]func(string), 0, len(r.groups[group]))
	for id := range r.groups[group] {
		if entry, ok := r.conns[id]; ok {
			localIDs = append(localIDs, id)
			hooks = append(hooks, entry.closeHook)
		}
	}
	r.mu.RUnlock()

	closed := 0
	for i := range localIDs {
		if hooks[i] != nil {
			hooks[i](reason)
		}
		closed++
	}

	if r.presence == nil {
		return closed, nil
	}

	members, err := r.presence.Members(ctx, group)
	if err != nil {
		return closed, err
	}
	localSet := make(map[string]struct{}, len(localIDs))
	for _, id := range localIDs {
		localSet[id] = struct{}{}
	}
	for _, id := range members {
		if _, isLocal := localSet[id]; isLocal {
			continue
		}
		pid, err := r.system.ActorOf(ctx, connActorName(id))
		if err != nil {
			if !connectionActorUnavailable(err) {
				r.logger.Warnf("gateway: failed to resolve remote group member %q for disconnect: %v", id, err)
			}
			continue
		}
		if err := r.system.NoSender().Tell(ctx, pid, wrapperspb.String(reason)); err != nil {
			if !connectionActorUnavailable(err) {
				r.logger.Warnf("gateway: failed to signal disconnect to remote group member %q: %v", id, err)
			}
			continue
		}
		closed++
	}
	return closed, nil
}

// triggerCloseHook runs the registered close hook for a locally held connection, if any. It
// is how a connActor services a remote-initiated Disconnect. It reports whether a local
// connection with a hook was found and run.
func (r *Registry) triggerCloseHook(id, reason string) bool {
	r.mu.RLock()
	entry, ok := r.conns[id]
	var hook func(string)
	if ok {
		hook = entry.closeHook
	}
	r.mu.RUnlock()
	if hook == nil {
		return false
	}
	hook(reason)
	return true
}

// IsOnline reports whether the identity group has at least one live connection. With a
// Presence backend the answer covers the whole cluster; without one it only covers this
// node, and returns false for an identity connected to a sibling node.
func (r *Registry) IsOnline(ctx context.Context, group string) (bool, error) {
	if r.presence != nil {
		return r.presence.Online(ctx, group)
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.groups[group]) > 0, nil
}

// LocalConnectionsOf returns the ids of the connections this node holds for group.
func (r *Registry) LocalConnectionsOf(group string) []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	members := r.groups[group]
	ids := make([]string, 0, len(members))
	for id := range members {
		ids = append(ids, id)
	}
	return ids
}

// Len returns the number of connections registered on this node.
func (r *Registry) Len() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.conns)
}

// Has reports whether id is registered on this node.
func (r *Registry) Has(id string) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	_, ok := r.conns[id]
	return ok
}

// sendTo writes payload to a locally registered connection. It is how a connActor reaches
// the socket it stands for, and it deliberately re-reads the table instead of closing over
// the send function: a takeover replaces the entry under an id, and the actor must follow
// the connection that currently owns it. deliverOne's own owner-lease check fences the write
// against whichever entry that re-read resolves - the current local owner of id, which may by
// now be a fresh same-node registration with a higher generation than the connActor's caller
// was spawned to back, in which case this correctly delivers rather than rejects.
func (r *Registry) sendTo(ctx context.Context, id string, payload []byte) error {
	r.mu.RLock()
	entry, ok := r.conns[id]
	r.mu.RUnlock()
	if !ok {
		return ErrConnectionNotFound
	}
	return r.deliverOne(ctx, entry, payload)
}

// staleOwner reports whether generation no longer matches the current owner lease record for
// id (see WithOwnerLease). It is the fencing check a connActor runs before writing an inbound
// cross-node delivery to the local socket, so a node whose generation has been superseded by a
// takeover elsewhere - even one its own background refresh or an explicit evict Tell has not
// yet caught up with - cannot deliver on its behalf.
//
// Without a configured lease it always reports false: no coordinator round trip, no fencing,
// the zero-cost default every opt-in feature in this package promises.
//
// A Coordinator error is logged and treated as stale. Ownership is unknown, so strict fencing
// must fail closed rather than write a socket from an owner the coordinator cannot confirm.
func (r *Registry) staleOwner(ctx context.Context, id string, generation uint64) bool {
	if r.lease == nil {
		return false
	}
	nodeID, currentGeneration, ok, err := r.lease.ownerNode(ctx, id)
	if err != nil {
		r.logger.Warnf("gateway: failed to check owner lease for connection %q: %v", id, err)
		return true
	}
	return !ok || nodeID != r.nodeID || currentGeneration != generation
}

// copyMeta returns an independent copy of the caller-supplied metadata map so the registry
// never retains a reference the caller can mutate out from under it. It returns nil for an
// empty map, keeping the metadata-less common case allocation-free.
func copyMeta(meta map[string]string) map[string]string {
	if len(meta) == 0 {
		return nil
	}
	out := make(map[string]string, len(meta))
	for k, v := range meta {
		out[k] = v
	}
	return out
}

// excludeSet turns an exclusion list into a lookup set, returning nil for an empty list so
// the common case allocates nothing.
func excludeSet(ids []string) map[string]struct{} {
	if len(ids) == 0 {
		return nil
	}
	set := make(map[string]struct{}, len(ids))
	for _, id := range ids {
		set[id] = struct{}{}
	}
	return set
}

// encodeEnvelope frames a cluster fan-out: the publishing node's origin tag (so its own
// echo can be recognized and skipped), the excluded connection ids (so an exclusion holds
// on every node, not only where the Broadcast was issued), then the payload.
//
//	origin   [16]byte
//	count    uint32
//	repeated uint32 length + id bytes
//	payload  remaining bytes
func encodeEnvelope(origin uuid.UUID, excludes []string, payload []byte) []byte {
	size := len(origin) + 4 + len(payload)
	for _, id := range excludes {
		size += 4 + len(id)
	}

	envelope := make([]byte, 0, size)
	envelope = append(envelope, origin[:]...)
	envelope = binary.BigEndian.AppendUint32(envelope, uint32(len(excludes)))
	for _, id := range excludes {
		envelope = binary.BigEndian.AppendUint32(envelope, uint32(len(id)))
		envelope = append(envelope, id...)
	}
	return append(envelope, payload...)
}

// decodeEnvelope parses a frame written by encodeEnvelope. A malformed frame is reported
// as such rather than delivered: it can only come from a node running an incompatible
// build, and guessing at its contents would deliver garbage to sockets.
func decodeEnvelope(envelope []byte) (origin []byte, excludes map[string]struct{}, payload []byte, ok bool) {
	const originLen = len(uuid.UUID{})
	if len(envelope) < originLen+4 {
		return nil, nil, nil, false
	}
	origin = envelope[:originLen]
	rest := envelope[originLen:]

	count := binary.BigEndian.Uint32(rest[:4])
	rest = rest[4:]

	for range count {
		if len(rest) < 4 {
			return nil, nil, nil, false
		}
		length := int(binary.BigEndian.Uint32(rest[:4]))
		rest = rest[4:]
		if len(rest) < length {
			return nil, nil, nil, false
		}
		if excludes == nil {
			excludes = make(map[string]struct{}, count)
		}
		excludes[string(rest[:length])] = struct{}{}
		rest = rest[length:]
	}

	return origin, excludes, rest, true
}

// observeRegistered reports a completed registration, if an Observer is configured.
func (r *Registry) observeRegistered(id, group string) {
	if r.observer != nil {
		r.observer.ConnectionRegistered(id, group)
	}
}

// observeUnregistered reports a removed connection, if an Observer is configured.
func (r *Registry) observeUnregistered(id, group string) {
	if r.observer != nil {
		r.observer.ConnectionUnregistered(id, group)
	}
}

// observeReplaced reports a takeover, if an Observer is configured.
func (r *Registry) observeReplaced(id, group string) {
	if r.observer != nil {
		r.observer.ConnectionReplaced(id, group)
	}
}

// observeDropped reports a backpressure drop, if an Observer is configured.
func (r *Registry) observeDropped(id, group string) {
	if r.observer != nil {
		r.observer.DeliveryDropped(id, group)
	}
}

// observeFailed reports a non-backpressure delivery failure, if an Observer is configured.
func (r *Registry) observeFailed(id string, err error) {
	if r.observer != nil {
		r.observer.DeliveryFailed(id, err)
	}
}

// observeFanout reports a broadcast's local fan-out size, if an Observer is configured.
func (r *Registry) observeFanout(topic string, localMembers int) {
	if r.observer != nil {
		r.observer.BroadcastFanout(topic, localMembers)
	}
}
