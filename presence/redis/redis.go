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

// Package redis provides a Redis-backed gateway.Presence, so every process in a
// deployment shares one view of which identities are connected and where. It is a
// separate package specifically so that importing the root gateway package never pulls
// in github.com/redis/go-redis/v9 for applications that do not want it (e.g. those using
// gateway.MemoryPresence, or no presence backend at all).
//
// # Data model
//
// A group is one Redis sorted set whose members are connection ids and whose scores are
// absolute expiry timestamps in Unix milliseconds (+inf for a lease that never expires).
// The alternative shape - a SET of connection ids plus one TTL key per member - was
// rejected for three reasons:
//
//   - Consistency of reads. With per-member TTL keys, Members has to read the set and
//     then probe every member key, and the two reads are not atomic: a member can expire
//     between them, so the caller sees an id whose lease has already lapsed. Here a single
//     Lua script sweeps the lapsed scores and returns the survivors, so a Members result
//     is a consistent snapshot as of one point in time.
//   - Cleanup cost. Per-member TTL keys expire themselves, but the SET still holds the
//     dead ids, so it needs the exact same lazy SREM sweep on read - the extra keys buy
//     nothing and multiply the key count by the connection count.
//   - Redis Cluster. One key per group means every operation is single-key and therefore
//     routable to one slot; the per-member layout spreads a group's keys across slots and
//     cannot be swept atomically at all.
//
// The cost of this shape is that expiry is lazy: nothing removes a lapsed member until a
// write or a read touches its group. That is bounded, because the group key itself carries
// a Redis TTL derived from its longest-lived member (see keyTTLGrace), so a group whose
// nodes all die is reclaimed by Redis even if nobody ever reads it again - including after
// a Leave changes which member is longest-lived (see leaveScript's TTL recompute).
//
// # Atomicity
//
// Join, Refresh, Leave, Members and Online each run as one Lua script (a single round
// trip, executed atomically by Redis). Concurrent callers on different nodes therefore
// never interleave a sweep with a join.
//
// Every script determines "now" from redis.call("TIME") rather than a timestamp the Go
// caller computed locally. Two callers running on different nodes can have skewed system
// clocks; if expiry were judged against a caller-supplied "now", a node whose clock runs
// ahead of the others could sweep out another node's still-valid member the moment it
// happened to call Members or Refresh. Reading the clock inside the script instead makes
// every expiry judgement (and every freshly recorded lease's absolute deadline) agree with
// the single Redis primary all callers already serialize through, which is the only clock
// that matters for who wins a race on this data. This relies on Redis 5.0+ replicating a
// script's effects (the writes it issued) rather than replaying the script's source on a
// replica/AOF reload, which is the default and only supported replication mode as of the
// Redis 7.2 / Valkey 8 baseline this package targets: TIME's non-determinism therefore
// never reaches a replica, unlike on the very old (pre-5.0), verbatim-script-replication
// servers where consulting TIME inside a script used to be unsafe.
//
// # Membership events and metadata
//
// This Presence also implements gateway.PresenceWatcher, gateway.PresenceDirectory and
// gateway.PresenceMetaJoiner (see watch.go). Membership events are published on a per-group
// Redis Pub/Sub channel from inside the same Lua scripts that mutate the sorted set, so a
// join or leave event is atomic with the state change it announces and costs no extra round
// trip. The watch contract is best-effort: Pub/Sub does not backfill events that occur while
// a subscriber is disconnected, and a slow subscriber loses overflow.
//
// Per-connection metadata is stored in a per-group Redis HASH held on the same Redis Cluster
// slot as the member sorted set (via a shared hash tag), so the join and leave scripts
// re-arm and reclaim the metadata key's lease atomically with the member set in the same
// call - including when JoinWithMeta first records it (see watch.go: the metadata write and
// the membership write happen inside the same script call, so a failure between them can
// never leave metadata recorded for a connection that never actually joined). The metadata
// lifecycle is therefore driven entirely by Redis state, not by any process-local flag: a
// Refresh or Leave on any node keeps alive or reclaims metadata that another node recorded.
// A deployment that never records metadata never creates the metadata key, and the extra
// in-script HSET/PEXPIRE/PERSIST/HDEL calls on the absent key are no-ops, so pure-presence
// deployments pay no extra round trip or key for the directory feature. Entries still reads
// authoritative membership from the sorted set (a separate call) and consults the HASH only
// to decorate the live members, so a stale metadata entry can never be returned. A member
// swept lazily by Members/Online (its lease lapsed without an explicit Leave, e.g. a crashed
// node) has its metadata HASH field reclaimed by that same sweep, so it cannot outlive the
// membership it described - see membersScript/onlineScript.
//
// # Redis Cluster hash tags
//
// A group's member key and metadata key share a "{tag}" hash tag so both land on the same
// slot, which is what lets the multi-key join/leave scripts run at all under Redis Cluster.
// The tag body is never the group name verbatim: it is a fixed one-byte prefix followed by
// the group name's hex encoding (see hashTag). Two group names that are byte-identical still
// produce byte-identical tags (so a group's own keys keep colliding onto one slot as
// intended), but the encoded tag can never itself contain '{' or '}' and is never empty
// (even for group == ""). Both properties matter: Redis Cluster's hash tag rule is "the
// substring between the first '{' and the next '}' after it", and if a group name were used
// verbatim, a group named "" or one starting with '}' would make that substring empty -
// which Redis Cluster's own key-hashing code (see redis-py/go-redis's Key()/Slot()) then
// treats as "no hash tag present" and falls back to hashing the whole key. Since the member
// key and metadata key differ in their infix ("m:" vs "h:"), their whole-key hashes would
// then very likely land on different slots, and any multi-key script touching both would
// fail with CROSSSLOT.
package redis

import (
	"context"
	"encoding/hex"
	"errors"
	"strconv"
	"time"

	goredis "github.com/redis/go-redis/v9"

	gateway "github.com/StringKe/goakt-gateway"
)

// DefaultKeyPrefix namespaces presence keys away from every other kind of key a gateway
// deployment may keep in the same Redis database (notably the coordinator's).
const DefaultKeyPrefix = "gateway:presence:"

// keyTTLGrace pads the Redis TTL put on a group key beyond its longest-lived member's
// lease. Without the pad, clock skew between this process and the Redis server could drop
// the whole group key a moment before its members were actually due to expire, losing live
// members. With it, the key outlives its last lease by a margin and is reclaimed shortly
// after - which only ever matters for groups nobody reads again, since a read sweeps them.
//
// It also bounds how long a fully emptied group's generation-fencing record (see
// generation.go) is retained: leaveScript re-arms it for this long rather than either
// deleting it immediately (which would reopen the exact ABA window generation fencing
// exists to close) or leaving it persisted forever (a leak once every member the record
// ever named has left for good).
const keyTTLGrace = 30 * time.Second

// memberInfix, metaInfix and genInfix place a group's member sorted set, its metadata HASH
// and its generation-fencing HASH (see generation.go) in disjoint key namespaces under the
// same prefix. They differ in their first byte and the group's hash tag is enclosed after
// them, so no group name can make one key collide with another key's namespace (a group
// literally named "meta:x" no longer aliases the metadata of group "x"). The "{" ... "}"
// is a Redis Cluster hash tag: it forces a group's three keys onto the same slot, which is
// what lets joinScript and leaveScript touch all of them in one atomic, cluster-routable
// call. See the package doc for why the tag body is hashTag(group), not group itself.
const (
	memberInfix = "m:{"
	metaInfix   = "h:{"
	genInfix    = "s:{"
	hashTagEnd  = "}"
)

// hashTagPrefix is prepended to a group's hex encoding before it is wrapped in the Redis
// Cluster hash tag braces (see hashTag and the package doc's "Redis Cluster hash tags"
// section). It exists purely so the tag body can never be empty, including for group == "":
// hex.EncodeToString of an empty group name is itself the empty string, and an empty tag
// body is exactly the degenerate case Redis Cluster's hash tag rule treats as "no tag".
const hashTagPrefix = "g"

// writeMemberScript records or refreshes a member with an absolute expiry score, optionally
// checks and records a generation-fencing watermark, optionally records metadata, sweeps the
// *other* members whose leases have lapsed (reclaiming their metadata as it goes, so a
// lazily-swept member's metadata cannot outlive it - ARGV[1] itself is exempted from this
// cleanup even if its own previous lease had lapsed, so a Refresh that is itself the first
// operation to notice its member lapsed still keeps whatever metadata a prior JoinWithMeta
// recorded, matching Refresh's documented contract; if some other node's Members/Online call
// swept the same lapse first, that metadata is already correctly gone by then - this
// exclusion only stops writeMemberScript from destroying, in the same call, the very
// metadata it and the record it is about to re-add both describe), and re-arms the group's
// three co-located keys' TTL from the longest-lived remaining lease. Refresh runs this
// script too (with emit off and no metadata): a lease that already lapsed is re-recorded
// rather than rejected, because the caller is the node that actually holds the socket and
// its view wins over an expired lease.
//
// The lapsed sweep runs before the ZADD so that ZADD's return value is a truthful "was this
// a new member" signal: a member whose lease had lapsed is swept first and then re-added as
// new, which is exactly when a PresenceJoin event is due. When ARGV[4] is "1" and the member
// was newly added, the script PUBLISHes the join event atomically with the write. PUBLISH is
// issued inside the script so the event never races ahead of or lags behind the state change
// and costs no extra round trip; a channel name is not a key, so it does not affect the
// script's key routing.
//
// Generation fencing (ARGV[9]/ARGV[10], see generation.go) is independent of the metadata and
// sweep logic above: when ARGV[9] is "1", the script first compares ARGV[10] against the
// watermark recorded in KEYS[3] for this connID (absent reads as "no watermark yet", which
// always passes) and aborts the whole call - no sweep, no write, no publish - returning -1 if
// the caller's generation is stale. This is what stops a delayed refresh from a node whose
// owner lease a takeover has already superseded from resurrecting membership state a newer
// owner has moved past. Plain Join/Refresh/JoinWithMeta pass ARGV[9] "0" and pay only the one
// extra HGET-shaped no-op the flag check costs; nothing they do changes.
//
// The same TTL (or PERSIST) computed for the member key is mirrored onto the co-located
// metadata key KEYS[2] and generation key KEYS[3] in the same call, so their leases are
// driven by Redis state rather than any process-local flag: whichever node last wrote the
// group re-arms every co-located key, which is what makes cross-node Refresh keep another
// node's recorded metadata (and generation watermark) alive. PERSIST/PEXPIRE/HSET on a key
// that does not exist are no-ops, so a deployment that never records metadata or uses
// generation fencing pays only a few extra in-script calls and no extra round trip or key.
//
// KEYS[1] group member key, KEYS[2] group metadata key, KEYS[3] group generation key.
// ARGV[1] connection id, ARGV[2] ttl in milliseconds ("0" means never expires), ARGV[3]
// grace in milliseconds, ARGV[4] emit flag ("1"/"0"), ARGV[5] event channel, ARGV[6] event
// payload, ARGV[7] has-metadata flag ("1"/"0"), ARGV[8] metadata payload, ARGV[9]
// has-generation flag ("1"/"0"), ARGV[10] generation.
var writeMemberScript = goredis.NewScript(`
local time = redis.call("TIME")
local now = tonumber(time[1]) * 1000 + math.floor(tonumber(time[2]) / 1000)

if ARGV[9] == "1" then
	local watermark = redis.call("HGET", KEYS[3], ARGV[1])
	if watermark and tonumber(watermark) > tonumber(ARGV[10]) then
		return -1
	end
	redis.call("HSET", KEYS[3], ARGV[1], ARGV[10])
end

local expired = redis.call("ZRANGEBYSCORE", KEYS[1], "-inf", "(" .. now)
if #expired > 0 then
	redis.call("ZREMRANGEBYSCORE", KEYS[1], "-inf", "(" .. now)
	-- ARGV[1] itself is excluded from the metadata cleanup even when it is among the
	-- expired ids: this call is about to re-add ARGV[1] as a live member (that is the
	-- whole point of "Refresh of a lapsed member re-records it"), and Refresh's documented
	-- contract is that it keeps whatever metadata a prior JoinWithMeta recorded alive. Only
	-- some *other* member's metadata reaching this branch is truly orphaned - ARGV[1]'s own
	-- metadata survives here and is then either left untouched (ARGV[7] "0") or overwritten
	-- with fresh metadata (ARGV[7] "1") by the HSET below, never silently dropped.
	local toReclaim = {}
	for i = 1, #expired do
		if expired[i] ~= ARGV[1] then
			toReclaim[#toReclaim + 1] = expired[i]
		end
	end
	if #toReclaim > 0 then
		redis.call("HDEL", KEYS[2], unpack(toReclaim))
	end
end

local score = "+inf"
if ARGV[2] ~= "0" then
	score = now + tonumber(ARGV[2])
end
local added = redis.call("ZADD", KEYS[1], score, ARGV[1])

if ARGV[7] == "1" then
	redis.call("HSET", KEYS[2], ARGV[1], ARGV[8])
end

local top = redis.call("ZRANGE", KEYS[1], -1, -1, "WITHSCORES")
local highest = tonumber(top[2])
if highest == nil or highest == math.huge then
	redis.call("PERSIST", KEYS[1])
	redis.call("PERSIST", KEYS[2])
	redis.call("PERSIST", KEYS[3])
else
	local ttl = math.ceil(highest - now + tonumber(ARGV[3]))
	if ttl < 1 then
		ttl = 1
	end
	redis.call("PEXPIRE", KEYS[1], ttl)
	redis.call("PEXPIRE", KEYS[2], ttl)
	redis.call("PEXPIRE", KEYS[3], ttl)
end

if added == 1 and ARGV[4] == "1" then
	redis.call("PUBLISH", ARGV[5], ARGV[6])
end
return added
`)

// leaveScript removes a member and, only when a member was actually removed, PUBLISHes the
// leave event atomically with the removal. Publishing only on an effective removal keeps a
// repeated Leave (or a Leave of an absent member) from emitting a phantom event, matching the
// MemoryPresence semantics.
//
// Generation fencing (ARGV[5]/ARGV[6], see generation.go) is checked before anything else,
// exactly as in writeMemberScript: a stale generation aborts the whole call - the member is
// not removed, its metadata is not touched - and returns -1, so a delayed leave from a node a
// takeover has already superseded cannot destroy membership state a newer owner established
// since. A leave that passes the check (or ARGV[5] "0", the plain unfenced path) records its
// own generation as the new watermark, so a duplicate or reordered delivery of that same stale
// leave cannot slip through on a second attempt either.
//
// The member's metadata is dropped from the co-located metadata key KEYS[2] in the same
// call, unconditionally: a departing connection's metadata must be reclaimed no matter
// which node observes the leave, so the delete is driven by Redis state rather than a
// process-local flag. HDEL of a field (or key) that does not exist is a no-op, so a
// deployment that never records metadata pays only one extra in-script call and no extra
// round trip - the directory feature stays opt-in and cost-free for pure presence.
//
// If the group still has members after the removal, the key TTLs are recomputed exactly as
// writeMemberScript computes them for a fresh join - otherwise a permanent (never-expiring)
// member leaving would strand the group key PERSISTed (no TTL) forever even once every
// remaining member is a normal, finite lease, breaking the "a group nobody reads again is
// still reclaimed by Redis" invariant the package doc promises. If the group is now empty,
// KEYS[1] and KEYS[2] have already self-deleted (Redis drops a sorted set/hash once its last
// member/field is removed), but KEYS[3] has no such member-driven lifecycle of its own (it
// exists purely to persist watermarks across leaves and rejoins), so it is explicitly given
// a bounded keyTTLGrace-length lease rather than being left however a much earlier join last
// set it (unboundedly long, or - if the group's last member was ever permanent - PERSISTed
// with no TTL at all, which would otherwise leak forever once nothing else references it).
//
// KEYS[1] group member key, KEYS[2] group metadata key, KEYS[3] group generation key.
// ARGV[1] connection id, ARGV[2] event channel, ARGV[3] event payload, ARGV[4] grace in
// milliseconds, ARGV[5] has-generation flag ("1"/"0"), ARGV[6] generation.
var leaveScript = goredis.NewScript(`
if ARGV[5] == "1" then
	local watermark = redis.call("HGET", KEYS[3], ARGV[1])
	if watermark and tonumber(watermark) > tonumber(ARGV[6]) then
		return -1
	end
	redis.call("HSET", KEYS[3], ARGV[1], ARGV[6])
end

local removed = redis.call("ZREM", KEYS[1], ARGV[1])
if removed > 0 then
	redis.call("PUBLISH", ARGV[2], ARGV[3])
end
redis.call("HDEL", KEYS[2], ARGV[1])

if redis.call("EXISTS", KEYS[1]) == 1 then
	local time = redis.call("TIME")
	local now = tonumber(time[1]) * 1000 + math.floor(tonumber(time[2]) / 1000)
	local top = redis.call("ZRANGE", KEYS[1], -1, -1, "WITHSCORES")
	local highest = tonumber(top[2])
	if highest == nil or highest == math.huge then
		redis.call("PERSIST", KEYS[1])
		redis.call("PERSIST", KEYS[2])
		redis.call("PERSIST", KEYS[3])
	else
		local ttl = math.ceil(highest - now + tonumber(ARGV[4]))
		if ttl < 1 then
			ttl = 1
		end
		redis.call("PEXPIRE", KEYS[1], ttl)
		redis.call("PEXPIRE", KEYS[2], ttl)
		redis.call("PEXPIRE", KEYS[3], ttl)
	end
else
	redis.call("PEXPIRE", KEYS[3], ARGV[4])
end
return removed
`)

// membersScript sweeps the lapsed leases - reclaiming their metadata as it goes, so a member
// that simply times out without anyone calling Leave (the common case for a crashed node)
// cannot leave its metadata behind forever - and returns the survivors, so a caller never
// sees a member whose lease expired but whose entry had not been reclaimed yet.
//
// KEYS[1] group key, KEYS[2] group metadata key.
var membersScript = goredis.NewScript(`
local time = redis.call("TIME")
local now = tonumber(time[1]) * 1000 + math.floor(tonumber(time[2]) / 1000)
local expired = redis.call("ZRANGEBYSCORE", KEYS[1], "-inf", "(" .. now)
if #expired > 0 then
	redis.call("ZREMRANGEBYSCORE", KEYS[1], "-inf", "(" .. now)
	redis.call("HDEL", KEYS[2], unpack(expired))
end
return redis.call("ZRANGEBYSCORE", KEYS[1], "-inf", "+inf")
`)

// onlineScript is membersScript that returns only the surviving cardinality, so an
// online check never ships a large group's ids over the wire.
//
// KEYS[1] group key, KEYS[2] group metadata key.
var onlineScript = goredis.NewScript(`
local time = redis.call("TIME")
local now = tonumber(time[1]) * 1000 + math.floor(tonumber(time[2]) / 1000)
local expired = redis.call("ZRANGEBYSCORE", KEYS[1], "-inf", "(" .. now)
if #expired > 0 then
	redis.call("ZREMRANGEBYSCORE", KEYS[1], "-inf", "(" .. now)
	redis.call("HDEL", KEYS[2], unpack(expired))
end
return redis.call("ZCARD", KEYS[1])
`)

// Presence is a gateway.Presence backed by a Redis client. It is safe for concurrent use.
type Presence struct {
	client goredis.UniversalClient
	prefix string
}

// Option configures a Presence created with NewPresence.
type Option func(*Presence)

// WithKeyPrefix namespaces every key this Presence reads or writes, so multiple gateway
// deployments (or unrelated applications) can share one Redis instance/database without
// colliding. Defaults to DefaultKeyPrefix.
func WithKeyPrefix(prefix string) Option {
	return func(p *Presence) { p.prefix = prefix }
}

// NewPresence creates a Presence backed by client. client may be a *redis.Client, a
// *redis.ClusterClient, or any other goredis.UniversalClient implementation; a group's
// three co-located keys carry a shared hash tag so the multi-key join and leave scripts
// route to a single slot on Redis Cluster (see the package doc).
func NewPresence(client goredis.UniversalClient, opts ...Option) *Presence {
	p := &Presence{client: client, prefix: DefaultKeyPrefix}
	for _, opt := range opts {
		opt(p)
	}
	return p
}

// enforce compilation error
var _ gateway.Presence = (*Presence)(nil)

// ErrStaleGeneration is returned by RefreshGeneration and LeaveGeneration (see generation.go)
// when the caller's generation has already been superseded by a later one recorded for the
// same connection id in the same group - either a later writeMemberScript/leaveScript call
// with a higher generation, or a takeover the caller has not learned about yet.
var ErrStaleGeneration = errors.New("gateway: presence operation rejected: a newer generation has superseded this connection")

// hashTag returns the Redis Cluster hash tag body for group: see the package doc's "Redis
// Cluster hash tags" section for why this cannot be group verbatim.
func hashTag(group string) string {
	return hashTagPrefix + hex.EncodeToString([]byte(group))
}

func (p *Presence) key(group string) string {
	return p.prefix + memberInfix + hashTag(group) + hashTagEnd
}

func (p *Presence) generationKey(group string) string {
	return p.prefix + genInfix + hashTag(group) + hashTagEnd
}

// Join records connID as an online member of group for at most ttl. A non-positive ttl
// records a member that never expires. Joining a connection id that is already a member
// simply replaces its lease. A first join of a member publishes a PresenceJoin event.
func (p *Presence) Join(ctx context.Context, group, connID string, ttl time.Duration) error {
	return p.writeMember(ctx, group, connID, ttl, true, nil, false, 0)
}

// Refresh extends the lease of connID in group by ttl. It is a single sorted-set write
// that never rebuilds the group, and it re-records a member whose lease already lapsed
// rather than failing: the caller is the node holding the socket. Refresh never emits a
// PresenceJoin event, and it keeps any metadata a prior JoinWithMeta recorded (see watch.go)
// alive by re-arming the metadata key's lease inside the same script that re-arms the
// member lease, so a Refresh on any node keeps another node's recorded metadata alive.
func (p *Presence) Refresh(ctx context.Context, group, connID string, ttl time.Duration) error {
	return p.writeMember(ctx, group, connID, ttl, false, nil, false, 0)
}

// Leave removes connID from group. It is a no-op when the member is already gone. Redis
// deletes the group key by itself once its last member is removed. An effective removal
// publishes a PresenceLeave event, and the member's metadata is dropped inside the same
// script, so a Leave on any node reclaims metadata another node recorded.
func (p *Presence) Leave(ctx context.Context, group, connID string) error {
	return p.leaveMember(ctx, group, connID, false, 0)
}

// writeMember runs writeMemberScript to record or refresh connID's lease in group, optionally
// writing metadata (meta non-nil, used by JoinWithMeta) and optionally checking/recording a
// generation-fencing watermark (hasGeneration, used by RefreshGeneration in generation.go).
// emit controls whether a first insertion publishes a PresenceJoin event: Join and
// JoinWithMeta pass true, Refresh and RefreshGeneration pass false.
func (p *Presence) writeMember(ctx context.Context, group, connID string, ttl time.Duration, emit bool, meta []byte, hasGeneration bool, generation uint64) error {
	emitFlag := "0"
	if emit {
		emitFlag = "1"
	}
	hasMetaFlag := "0"
	if meta != nil {
		hasMetaFlag = "1"
	}
	hasGenerationFlag := "0"
	if hasGeneration {
		hasGenerationFlag = "1"
	}

	res, err := writeMemberScript.Run(ctx, p.client,
		[]string{p.key(group), p.metaKey(group), p.generationKey(group)},
		connID,
		ttlMillis(ttl),
		strconv.FormatInt(keyTTLGrace.Milliseconds(), 10),
		emitFlag,
		p.eventChannel(group),
		p.encodeEvent(connID, gateway.PresenceJoin),
		hasMetaFlag,
		meta,
		hasGenerationFlag,
		strconv.FormatUint(generation, 10),
	).Int64()
	if err != nil {
		return err
	}
	if res < 0 {
		return ErrStaleGeneration
	}
	return nil
}

// leaveMember runs leaveScript to remove connID from group, optionally checking/recording a
// generation-fencing watermark (hasGeneration, used by LeaveGeneration in generation.go).
func (p *Presence) leaveMember(ctx context.Context, group, connID string, hasGeneration bool, generation uint64) error {
	hasGenerationFlag := "0"
	if hasGeneration {
		hasGenerationFlag = "1"
	}

	res, err := leaveScript.Run(ctx, p.client,
		[]string{p.key(group), p.metaKey(group), p.generationKey(group)},
		connID,
		p.eventChannel(group),
		p.encodeEvent(connID, gateway.PresenceLeave),
		strconv.FormatInt(keyTTLGrace.Milliseconds(), 10),
		hasGenerationFlag,
		strconv.FormatUint(generation, 10),
	).Int64()
	if err != nil {
		return err
	}
	if res < 0 {
		return ErrStaleGeneration
	}
	return nil
}

// Members returns the connection ids currently online for group on any node, dropping the
// lapsed leases (and their metadata) on the way.
func (p *Presence) Members(ctx context.Context, group string) ([]string, error) {
	members, err := membersScript.Run(ctx, p.client,
		[]string{p.key(group), p.metaKey(group)},
	).StringSlice()
	if err != nil {
		return nil, err
	}
	if len(members) == 0 {
		return nil, nil
	}
	return members, nil
}

// Online reports whether group has at least one live member on any node.
func (p *Presence) Online(ctx context.Context, group string) (bool, error) {
	count, err := onlineScript.Run(ctx, p.client,
		[]string{p.key(group), p.metaKey(group)},
	).Int64()
	if err != nil {
		return false, err
	}
	return count > 0, nil
}

// ttlMillis turns a lease duration into the millisecond argument writeMemberScript expects:
// "0" for a non-positive ttl ("never expires"), otherwise the duration floored to at least
// 1ms so a positive sub-millisecond ttl cannot round down to 0 and be misread as "never
// expires" - the opposite of what a caller who passed ttl > 0 asked for.
func ttlMillis(ttl time.Duration) string {
	if ttl <= 0 {
		return "0"
	}
	return strconv.FormatInt(max(ttl.Milliseconds(), 1), 10)
}
