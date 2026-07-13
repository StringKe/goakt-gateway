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
	"sync"
	"time"
)

// Presence is the cluster-wide answer to "is this identity connected anywhere, and
// where". A Registry without a Presence backend only ever knows about the connections it
// holds itself, which is enough for delivery (the cluster fan-out reaches the rest) but
// not enough to decide whether an identity is offline and a web push should be sent
// instead.
//
// Entries are leased rather than owned: the Registry renews them on an interval derived
// from the configured TTL (see WithPresenceTTL), so a node that dies without unregistering
// its connections stops holding them online once the lease lapses.
type Presence interface {
	// Join records connID as an online member of group for at most ttl.
	Join(ctx context.Context, group, connID string, ttl time.Duration) error

	// Leave removes connID from group. It must not fail when the member is already gone.
	Leave(ctx context.Context, group, connID string) error

	// Refresh extends the lease of connID in group by ttl.
	Refresh(ctx context.Context, group, connID string, ttl time.Duration) error

	// Members returns the connection ids currently online for group, on any node.
	Members(ctx context.Context, group string) ([]string, error)

	// Online reports whether group has at least one live member on any node.
	Online(ctx context.Context, group string) (bool, error)
}

// PresenceEventKind distinguishes a member joining a group from a member leaving it.
type PresenceEventKind int

const (
	// PresenceJoin marks a connection newly recorded as online in a group.
	PresenceJoin PresenceEventKind = iota

	// PresenceLeave marks a connection removed from a group.
	PresenceLeave
)

// PresenceEvent is a single change to a group's membership delivered over a
// PresenceWatcher subscription.
type PresenceEvent struct {
	// Group is the identity group the change happened in.
	Group string

	// ConnID is the connection the change is about.
	ConnID string

	// Kind is whether ConnID joined or left.
	Kind PresenceEventKind
}

// PresenceWatcher is an optional Presence extension: a backend that can stream membership
// changes for a group implements it, and Registry.WatchPresence surfaces the stream.
// Backends that cannot (Registry.WatchPresence returns ErrPresenceWatchUnsupported for
// them) are still valid Presence implementations.
//
// A watch is best-effort by contract: an implementation may drop events to a subscriber
// that is not keeping up, and events that occur while a subscriber is disconnected from the
// backend are not backfilled. Treat the stream as a hint to re-read authoritative state,
// not as a gap-free log.
type PresenceWatcher interface {
	// Watch subscribes to membership changes for group. It returns a receive channel of
	// events, a cancel function that unsubscribes and closes the channel, and an error. The
	// channel is closed when cancel is called or ctx is cancelled.
	Watch(ctx context.Context, group string) (events <-chan PresenceEvent, cancel func(), err error)
}

// PresenceEntry is one member of a group as reported by a PresenceDirectory: its
// connection id and the metadata recorded for it at registration time.
type PresenceEntry struct {
	// ConnID is the connection id.
	ConnID string

	// Meta is the application metadata the connection registered with, or nil if none was
	// recorded.
	Meta map[string]string
}

// PresenceDirectory is an optional Presence extension: a backend that can enumerate a
// group's members cluster-wide, with their metadata, implements it, and
// Registry.GroupMembers uses it. Without one, Registry.GroupMembers falls back to this
// node's local view only.
type PresenceDirectory interface {
	// Entries returns every online member of group across the cluster, with metadata.
	Entries(ctx context.Context, group string) ([]PresenceEntry, error)
}

// PresenceMetaJoiner is an optional Presence extension for backends that also store
// per-connection metadata. When the configured Presence implements it, Registry.Register
// records ConnInfo.Meta through JoinWithMeta so a PresenceDirectory can return it
// cluster-wide. Backends that do not implement it still work: the metadata simply is not
// stored in presence and GroupMembers falls back to the local view for it.
type PresenceMetaJoiner interface {
	// JoinWithMeta records connID as an online member of group for at most ttl, along with
	// its metadata. It has the same lease semantics as Presence.Join.
	JoinWithMeta(ctx context.Context, group, connID string, meta map[string]string, ttl time.Duration) error
}

// PresenceFencer is an optional Presence extension for backends that participate in
// owner-lease generation fencing (see WithOwnerLease and Registry.staleOwner in registry.go):
// Refresh and Leave calls carry the caller's connection generation, so a node whose lease has
// been taken over by a newer generation cannot use a delayed Refresh to keep a group
// membership alive past the takeover, nor a delayed Leave to remove a membership the new
// owner already (re)established.
//
// Join is deliberately not part of this extension: by the time a node is allowed to call Join
// (or JoinWithMeta) for a connection, the owner-lease compare-and-swap has already serialized
// who won any takeover, so a stale Join cannot happen the way a stale, in-flight Refresh or
// Leave can from a goroutine that read the old generation just before it lapsed.
//
// A Presence backend that does not implement PresenceFencer is still fully valid: Registry
// then relies solely on the connection-level generation check (Registry.staleOwner) to stop a
// superseded node from writing to the socket, at the cost of a stale Refresh potentially
// keeping a Presence entry alive for up to one more renewal interval than a fenced backend
// would allow, and a stale Leave being able to remove a membership a takeover just restored.
type PresenceFencer interface {
	// RefreshGen is Refresh fenced by generation: a call whose generation is lower than the
	// generation last recorded for connID in group is rejected with ErrStaleOwner instead of
	// extending the lease, and leaves the recorded membership untouched. A generation greater
	// than or equal to what is recorded succeeds, extends the lease, and (re)sets the recorded
	// generation to it.
	RefreshGen(ctx context.Context, group, connID string, generation uint64, ttl time.Duration) error

	// LeaveGen is Leave fenced by generation: a call whose generation is lower than the
	// generation last recorded for connID in group is rejected with ErrStaleOwner and the
	// member is not removed, so a takeover's (re)join cannot be undone by the previous owner's
	// delayed Leave. Like Leave, it is a no-op, not an error, when the member is already gone.
	LeaveGen(ctx context.Context, group, connID string, generation uint64) error
}

// WatchPresence subscribes to membership changes for group. It requires the configured
// Presence backend to implement PresenceWatcher and returns ErrPresenceWatchUnsupported
// when none is configured or the configured one does not. See PresenceWatcher for the
// best-effort delivery contract.
func (r *Registry) WatchPresence(ctx context.Context, group string) (<-chan PresenceEvent, func(), error) {
	watcher, ok := r.presence.(PresenceWatcher)
	if !ok {
		return nil, nil, ErrPresenceWatchUnsupported
	}
	return watcher.Watch(ctx, group)
}

// GroupMembers enumerates the connections of an identity group. With a Presence backend
// that implements PresenceDirectory the answer covers the whole cluster and carries each
// member's metadata; otherwise it falls back to the connections this node holds for group,
// with the metadata they registered with.
func (r *Registry) GroupMembers(ctx context.Context, group string) ([]PresenceEntry, error) {
	if dir, ok := r.presence.(PresenceDirectory); ok {
		return dir.Entries(ctx, group)
	}

	r.mu.RLock()
	defer r.mu.RUnlock()
	ids := r.groups[group]
	entries := make([]PresenceEntry, 0, len(ids))
	for id := range ids {
		entry, ok := r.conns[id]
		if !ok {
			continue
		}
		entries = append(entries, PresenceEntry{ConnID: id, Meta: cloneMeta(entry.meta)})
	}
	return entries, nil
}

// memberState is the per-connection bookkeeping MemoryPresence keeps: the lease expiry, the
// metadata the connection registered with, the last generation recorded for it (see
// PresenceFencer; zero when the caller never used a Gen call), and how long the record itself
// survives in the map once it stops being live.
//
// expiry and retainTo are deliberately separate deadlines. expiry is "is this member online",
// which is what liveMembers/Members/Online/Entries honor and what a zero value ("never
// expires") means for a permanently-joined member. retainTo is "how long do we still remember
// this connID's generation after it stops being online" - it is always a concrete, bounded
// deadline (never permanent, even for a permanent member), because its only job is to let
// RefreshGen/LeaveGen tell a stale, delayed call from a superseded generation apart from a
// genuinely fresh join with no prior record: without it, a Leave/expiry would erase the
// generation memory along with the membership, and a straggling Refresh/Leave carrying an old
// generation would be indistinguishable from a brand new one and be allowed to resurrect or
// recreate a membership a newer generation already superseded. Bounding it keeps a
// high-churn group from accumulating tombstones forever.
type memberState struct {
	expiry     time.Time
	retainTo   time.Time
	generation uint64
	meta       map[string]string
}

// MemoryPresence is an in-process Presence backend. It is the right choice for a
// single-node deployment and for tests, and the wrong choice for anything clustered: its
// view of "online" stops at the process boundary. It also implements PresenceWatcher and
// PresenceDirectory, so WatchPresence and GroupMembers are fully functional against it on a
// single node.
type MemoryPresence struct {
	mu sync.Mutex
	// groups maps group -> connID -> member state.
	groups map[string]map[string]memberState
	// watchers maps group -> the event channels subscribed to it.
	watchers map[string][]chan PresenceEvent
}

// enforce compilation error
var (
	_ Presence           = (*MemoryPresence)(nil)
	_ PresenceWatcher    = (*MemoryPresence)(nil)
	_ PresenceDirectory  = (*MemoryPresence)(nil)
	_ PresenceMetaJoiner = (*MemoryPresence)(nil)
	_ PresenceFencer     = (*MemoryPresence)(nil)
)

// memoryPresenceWatchBuffer sizes each watcher channel. Events are delivered with a
// non-blocking send, so a subscriber that falls this far behind loses the overflow rather
// than stalling the membership operations that produce events.
const memoryPresenceWatchBuffer = 16

// NewMemoryPresence creates an in-process Presence backend.
func NewMemoryPresence() *MemoryPresence {
	return &MemoryPresence{
		groups:   make(map[string]map[string]memberState),
		watchers: make(map[string][]chan PresenceEvent),
	}
}

// Join records connID as an online member of group for at most ttl. A non-positive ttl
// records a member that never expires. It emits a PresenceJoin event only when connID was
// not already a member, so a re-join of a live member does not double-count.
func (p *MemoryPresence) Join(_ context.Context, group, connID string, ttl time.Duration) error {
	p.upsert(group, connID, nil, false, ttl)
	return nil
}

// JoinWithMeta is Join with metadata recorded for the connection, used by Registry.Register
// so GroupMembers can return it.
func (p *MemoryPresence) JoinWithMeta(_ context.Context, group, connID string, meta map[string]string, ttl time.Duration) error {
	p.upsert(group, connID, meta, true, ttl)
	return nil
}

// upsert records or refreshes a member, emitting a PresenceJoin event only when the member
// was not already live - which covers both a brand new connID and one whose previous record
// is a lapsed lease or a post-Leave tombstone still being retained for generation memory (see
// memberState), so a rejoin after either is correctly reported as a join, not silently
// swallowed because a record happened to still be sitting in the map. updateMeta controls
// whether meta replaces any previously recorded metadata, so Refresh (which passes it false)
// keeps the metadata a JoinWithMeta recorded. generation is carried over from any previous
// record unconditionally: a plain (non-Gen) call must never reset a connID's fencing state
// back to zero, or a call racing a RefreshGen/LeaveGen could undo the fencing they establish.
func (p *MemoryPresence) upsert(group, connID string, meta map[string]string, updateMeta bool, ttl time.Duration) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.groups[group] == nil {
		p.groups[group] = make(map[string]memberState)
	}
	prev, existed := p.groups[group][connID]
	state := memberState{
		expiry:     expiryOf(ttl),
		retainTo:   retainToOf(ttl),
		meta:       prev.meta,
		generation: prev.generation,
	}
	if updateMeta {
		// The caller keeps ownership of meta and may mutate it after this returns, so store an
		// independent copy: sharing the reference would let a later external write corrupt the
		// member state under p.mu without ever taking the lock.
		state.meta = cloneMeta(meta)
	}
	p.groups[group][connID] = state
	if !memberLive(prev, existed) {
		p.emitLocked(PresenceEvent{Group: group, ConnID: connID, Kind: PresenceJoin})
	}
}

// RefreshGen is Refresh fenced by generation: see PresenceFencer.
func (p *MemoryPresence) RefreshGen(_ context.Context, group, connID string, generation uint64, ttl time.Duration) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.groups[group] == nil {
		p.groups[group] = make(map[string]memberState)
	}
	prev, existed := p.groups[group][connID]
	if existed && generation < prev.generation {
		return ErrStaleOwner
	}
	state := memberState{
		expiry:     expiryOf(ttl),
		retainTo:   retainToOf(ttl),
		meta:       prev.meta,
		generation: generation,
	}
	p.groups[group][connID] = state
	if !memberLive(prev, existed) {
		p.emitLocked(PresenceEvent{Group: group, ConnID: connID, Kind: PresenceJoin})
	}
	return nil
}

// Leave removes connID from group. It is a no-op when the member is already gone, and emits
// a PresenceLeave event only when a live member was actually removed.
func (p *MemoryPresence) Leave(_ context.Context, group, connID string) error {
	return p.leave(group, connID, nil)
}

// LeaveGen is Leave fenced by generation: see PresenceFencer.
func (p *MemoryPresence) LeaveGen(_ context.Context, group, connID string, generation uint64) error {
	return p.leave(group, connID, &generation)
}

// leave is the shared implementation behind Leave and LeaveGen. gen == nil is the plain,
// unfenced Leave: it always succeeds against whatever is on record, matching Leave's existing
// "must not fail when the member is already gone" contract. A non-nil gen rejects a call whose
// value trails the recorded generation with ErrStaleOwner and leaves the record untouched, so
// a superseded node's delayed Leave cannot undo a takeover's (re)join.
//
// A live member is not deleted from the map on departure, it is tombstoned: expiry is set to
// now (so it stops counting as live) while retainTo and generation are preserved, keeping the
// generation on record for RefreshGen/LeaveGen to fence a later stale call against instead of
// treating "no record" as license to resurrect the membership. liveMembers purges the
// tombstone once retainTo passes.
func (p *MemoryPresence) leave(group, connID string, gen *uint64) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	members, ok := p.groups[group]
	if !ok {
		return nil
	}
	prev, existed := members[connID]
	if !existed {
		return nil
	}
	generation := prev.generation
	if gen != nil {
		if *gen < prev.generation {
			return ErrStaleOwner
		}
		generation = *gen
	}
	if !memberLive(prev, existed) {
		// Already a tombstone or a lapsed lease: nothing online to remove and no event to
		// emit, but still record the (possibly higher) generation this call carried.
		members[connID] = memberState{expiry: prev.expiry, retainTo: prev.retainTo, meta: prev.meta, generation: generation}
		return nil
	}
	members[connID] = memberState{expiry: time.Now(), retainTo: prev.retainTo, meta: prev.meta, generation: generation}
	p.emitLocked(PresenceEvent{Group: group, ConnID: connID, Kind: PresenceLeave})
	return nil
}

// Refresh extends the lease of connID in group by ttl. A member whose lease already
// lapsed is re-recorded rather than rejected: the caller is the node that actually holds
// the socket, so its view wins over an expired lease. Refresh keeps any recorded metadata
// and, being an ongoing renewal rather than a fresh join, emits no event for a member that
// was already present.
func (p *MemoryPresence) Refresh(_ context.Context, group, connID string, ttl time.Duration) error {
	p.upsert(group, connID, nil, false, ttl)
	return nil
}

// Members returns the connection ids currently online for group, dropping and reclaiming
// lapsed leases on the way.
func (p *MemoryPresence) Members(_ context.Context, group string) ([]string, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	live := p.liveMembers(group)
	ids := make([]string, 0, len(live))
	for id := range live {
		ids = append(ids, id)
	}
	return ids, nil
}

// Online reports whether group has at least one live member.
func (p *MemoryPresence) Online(_ context.Context, group string) (bool, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.liveMembers(group)) > 0, nil
}

// Entries returns every live member of group with its recorded metadata.
func (p *MemoryPresence) Entries(_ context.Context, group string) ([]PresenceEntry, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	live := p.liveMembers(group)
	entries := make([]PresenceEntry, 0, len(live))
	for id, state := range live {
		// Return an independent copy so a caller mutating PresenceEntry.Meta cannot reach back
		// into the member state that stays live under p.mu, which would be a lock-free write to
		// shared map and a data race against concurrent Entries/upsert.
		entries = append(entries, PresenceEntry{ConnID: id, Meta: cloneMeta(state.meta)})
	}
	return entries, nil
}

// Watch subscribes to membership changes for group. The returned cancel unsubscribes and
// closes the channel and is safe to call more than once.
func (p *MemoryPresence) Watch(ctx context.Context, group string) (<-chan PresenceEvent, func(), error) {
	ch := make(chan PresenceEvent, memoryPresenceWatchBuffer)

	p.mu.Lock()
	p.watchers[group] = append(p.watchers[group], ch)
	p.mu.Unlock()

	var once sync.Once
	done := make(chan struct{})
	cancel := func() {
		once.Do(func() {
			p.mu.Lock()
			subs := p.watchers[group]
			for i, sub := range subs {
				if sub == ch {
					p.watchers[group] = append(subs[:i], subs[i+1:]...)
					break
				}
			}
			if len(p.watchers[group]) == 0 {
				delete(p.watchers, group)
			}
			p.mu.Unlock()
			close(ch)
			close(done)
		})
	}

	// A cancelled context must release the subscription even if the caller never calls
	// cancel, so a watch does not outlive the scope it was created for. The done channel
	// lets this goroutine exit when cancel is called first, so an explicit cancel with a
	// never-cancelled context does not leak it.
	go func() {
		select {
		case <-ctx.Done():
			cancel()
		case <-done:
		}
	}()

	return ch, cancel, nil
}

// emitLocked fans an event out to every watcher of its group with a non-blocking send. The
// caller must hold p.mu. A watcher whose buffer is full loses the event, keeping the
// membership operation that produced it from blocking on a slow subscriber.
func (p *MemoryPresence) emitLocked(event PresenceEvent) {
	for _, ch := range p.watchers[event.Group] {
		select {
		case ch <- event:
		default:
		}
	}
}

// liveMembers purges every tombstone in group whose retention window (retainTo) has passed
// and returns a fresh snapshot of the survivors that are currently live, keyed by connID.
// Purging is lazy: nothing sweeps in the background, so a group nobody ever asks about simply
// keeps its dead entries until it is read or the process ends. Lazy purging emits no
// PresenceLeave events, which is why a watch is documented as best-effort.
//
// The snapshot is a new map, not the underlying storage: an expired-but-still-retained
// tombstone (see memberState) stays in p.groups[group] for RefreshGen/LeaveGen fencing but
// must never be visible to Members/Online/Entries, which only report live members.
func (p *MemoryPresence) liveMembers(group string) map[string]memberState {
	members, ok := p.groups[group]
	if !ok {
		return nil
	}
	now := time.Now()
	live := make(map[string]memberState, len(members))
	for connID, state := range members {
		if !state.retainTo.IsZero() && now.After(state.retainTo) {
			delete(members, connID)
			continue
		}
		if state.expiry.IsZero() || now.Before(state.expiry) {
			live[connID] = state
		}
	}
	if len(members) == 0 {
		delete(p.groups, group)
	}
	return live
}

// memberLive reports whether prev is a currently-online member, given whether a record for it
// existed at all. A record that exists but has lapsed - a tombstone left by Leave/LeaveGen, or
// a lease nobody renewed in time - is not live, matching what liveMembers itself would report.
// Both upsert-family writers (Join/JoinWithMeta/Refresh/RefreshGen) and leave use this to
// decide whether a rejoin is a genuine PresenceJoin (nothing was online) rather than treating
// "a record still sits in the map" as if the member had never left.
func memberLive(prev memberState, existed bool) bool {
	return existed && (prev.expiry.IsZero() || time.Now().Before(prev.expiry))
}

// cloneMeta returns an independent copy of meta. A nil input stays nil rather than becoming
// an empty map so PresenceEntry.Meta keeps its documented "nil if none was recorded" meaning,
// letting callers distinguish "no metadata" from "empty metadata".
func cloneMeta(meta map[string]string) map[string]string {
	if meta == nil {
		return nil
	}
	out := make(map[string]string, len(meta))
	for k, v := range meta {
		out[k] = v
	}
	return out
}

// expiryOf turns a lease duration into an absolute deadline, with a non-positive duration
// meaning "no deadline".
func expiryOf(ttl time.Duration) time.Time {
	if ttl <= 0 {
		return time.Time{}
	}
	return time.Now().Add(ttl)
}

// presenceGenerationRetention multiplies a member's ttl to compute retainTo: how much longer
// its record survives, as a tombstone, past the point it stops being live. See memberState for
// why this window needs to exist at all; it is bounded (rather than kept forever) so a group
// with high churn does not accumulate tombstones without limit. The multiplier only needs to
// comfortably outlast realistic message delay/retry windows, not match any other package's
// retention constant - matching ownership.go's generationRetentionMultiplier here would wire
// two independently-tunable mechanisms together for no reason, since MemoryPresence's map has
// nothing to do with a CASCoordinator's storage-layer TTL.
const presenceGenerationRetention = 4

// presenceGenerationDefaultRetention bounds the tombstone window for a member that joined with
// a non-positive (permanent) ttl. expiry itself may legitimately never lapse for such a
// member, but its post-Leave tombstone still must have a bound, or an application that
// repeatedly adds and removes permanent members would leak one retained record per removed
// member forever.
const presenceGenerationDefaultRetention = 5 * time.Minute

// retainToOf computes how long a member record (live or, later, tombstoned) is kept in the map
// once it stops being live, given the ttl it was last (re)joined or refreshed with. Unlike
// expiryOf, it never returns the zero value: a permanent member's liveness can be unbounded,
// but the memory of its generation after it leaves cannot be.
func retainToOf(ttl time.Duration) time.Time {
	if ttl <= 0 {
		return time.Now().Add(presenceGenerationDefaultRetention)
	}
	return time.Now().Add(ttl * presenceGenerationRetention)
}
