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
	"encoding/binary"
	"errors"
	"time"
)

// ownerLeaseKeyPrefix namespaces owner lease keys within whatever CASCoordinator the
// caller supplied to WithOwnerLease - which may be the very same Coordinator instance used
// for certificate issuance arbitration (see Manager). certCoordinatorKeyPrefix ("gateway:
// cert:") and certLockCoordinatorKeyPrefix ("gateway:cert-lock:") key domain names, which
// can never start with this prefix, so an owner lease key can never alias a certificate
// record or its issuance lock.
const ownerLeaseKeyPrefix = "gateway:conn-owner:"

// maxOwnerLeaseAcquireAttempts bounds the optimistic-concurrency retry loop in
// ownerLease.acquire. Each failed CompareAndSwap means another caller's write landed
// between this call's Get and its CompareAndSwap; retrying against the freshly observed
// value converges quickly under normal contention, but bounding the loop keeps a
// pathological hot-key race from spinning forever instead of surfacing ErrOwnerHeld.
const maxOwnerLeaseAcquireAttempts = 64

// generationRetentionMultiplier keeps a lease record in the CASCoordinator for this many
// multiples of the lease ttl past the point it stops being an actively-held, unexpired
// lease. Retaining it (rather than letting the record's storage-layer ttl equal the lease
// ttl and vanish exactly when the lease goes logically stale) is what makes generation
// numbers monotonic across a reconnect that lands after the old lease expired: acquire's
// "exists but expired" branch reads the last known generation and increments it, so a
// straggling message or fencing check still carrying that old generation can never collide
// with a freshly issued one. Without retention, the record would be physically gone by the
// time a later acquire ran, generation would restart at 1, and a same-node reconnect could
// coincidentally reuse the exact (nodeID, generation) pair a stale in-flight caller from the
// previous epoch still holds - the ABA hazard this whole mechanism exists to close.
//
// The record is not retained forever: that would leak one CASCoordinator entry per
// connection id for the life of the process. Bounding retention to a small multiple of the
// lease ttl keeps generation continuity across realistic reconnect gaps while still letting
// truly abandoned connection ids fall out of storage on their own.
const generationRetentionMultiplier = 8

// errCorruptOwnerLease is returned when a value read back from the owner lease key does
// not decode as a lease record. It is deliberately not treated the same as an absent key:
// silently proceeding as if unowned would let two callers both believe they safely
// acquired the lease.
var errCorruptOwnerLease = errors.New("gateway: corrupt owner lease value")

// ownerLease provides an atomic, generation-fenced owner lease per connection id, backed
// by a CASCoordinator. It does not use the GoAkt actor directory for ownership: that
// directory is PA/EC eventually-consistent (see the Coordinator doc comment), so two nodes
// racing a takeover could both observe "no owner" and both proceed. CompareAndSwap is an
// atomic primitive against a single coordinator backend, so at most one of two racing
// acquire calls for the same connection id ever succeeds.
//
// A lease value is nodeID + generation + an expiry timestamp. Every fencing check compares
// the caller's generation against the coordinator's current record: a caller whose
// generation has been superseded by a later acquire is stale and must be rejected, even if
// its own local ttl/clock has not yet caught up.
type ownerLease struct {
	coord  CASCoordinator
	nodeID string
	ttl    time.Duration
}

// newOwnerLease creates an ownerLease that identifies this process as nodeID and leases
// connection ownership for ttl at a time.
func newOwnerLease(coord CASCoordinator, nodeID string, ttl time.Duration) *ownerLease {
	return &ownerLease{coord: coord, nodeID: nodeID, ttl: ttl}
}

// encodeLeaseValue packs a lease record as generation (8 bytes, big-endian) + expiry as a
// Unix millisecond timestamp (8 bytes, big-endian) + the owning node id (remaining bytes,
// UTF-8). generation and expiry are fixed-width and come first so decodeLeaseValue never
// has to guess where nodeID ends, however nodeID is spelled.
func encodeLeaseValue(nodeID string, generation uint64, expiresAtUnixMs int64) []byte {
	buf := make([]byte, 0, 16+len(nodeID))
	buf = binary.BigEndian.AppendUint64(buf, generation)
	buf = binary.BigEndian.AppendUint64(buf, uint64(expiresAtUnixMs))
	buf = append(buf, nodeID...)
	return buf
}

// decodeLeaseValue reverses encodeLeaseValue. ok is false if b is too short to be a lease
// record this package wrote.
func decodeLeaseValue(b []byte) (nodeID string, generation uint64, expiresAtUnixMs int64, ok bool) {
	if len(b) < 16 {
		return "", 0, 0, false
	}
	generation = binary.BigEndian.Uint64(b[0:8])
	expiresAtUnixMs = int64(binary.BigEndian.Uint64(b[8:16]))
	nodeID = string(b[16:])
	return nodeID, generation, expiresAtUnixMs, true
}

func (l *ownerLease) key(connID string) string {
	return ownerLeaseKeyPrefix + connID
}

// retentionTTL is the storage-layer ttl passed to CompareAndSwap for every lease write. See
// generationRetentionMultiplier for why it deliberately outlives the lease's own logical
// expiry (embedded in the value as expiresAtUnixMs), which is what actually governs whether
// a lease reads as actively held.
func (l *ownerLease) retentionTTL() time.Duration {
	return l.ttl * generationRetentionMultiplier
}

// acquire obtains ownership of connID and returns the generation this call was granted. It is
// a thin wrapper over acquireDetailed for callers (and white-box tests) that only need the
// granted generation, discarding the prior-owner bookkeeping a takeover caller needs to abort
// cleanly (see acquireDetailed and abortTakeover).
//
// If no lease record exists, or the existing one has expired, acquire always succeeds
// (regardless of takeover) and is granted the next generation after whatever was there
// (or generation 1 for a brand new connection id).
//
// If an unexpired lease is currently held by another owner, acquire fails with
// ErrOwnerHeld unless takeover is true, in which case it forcibly bumps the generation and
// overwrites the record - the previous owner's generation is thereby fenced out and any
// fencing check it performs from that point on (refresh, release, or a delivery/presence
// operation carrying its old generation) must observe ErrStaleOwner.
func (l *ownerLease) acquire(ctx context.Context, connID string, takeover bool) (uint64, error) {
	acq, err := l.acquireDetailed(ctx, connID, takeover)
	return acq.generation, err
}

// leaseAcquisition is the outcome of acquireDetailed: the generation granted, plus whatever the
// coordinator record held immediately before this call overwrote it (if anything). A takeover
// caller whose subsequent physical eviction never completes (see Registry.spawnConnActor and
// ErrTakeoverTimeout) uses the prior fields to restore that record with abortTakeover, instead
// of leaving this call's own claim - or a bare tombstone - permanently fencing out an owner that
// was never actually dislodged.
type leaseAcquisition struct {
	generation uint64

	hadPrior         bool
	priorNodeID      string
	priorGeneration  uint64
	priorExpiresAtMs int64
}

// acquireDetailed is acquire's full implementation; see acquire's doc comment for the externally
// visible acquisition rules it follows. It additionally reports the record it replaced so a
// takeover caller can undo this acquisition later (abortTakeover) without a second round trip to
// rediscover what was there before.
func (l *ownerLease) acquireDetailed(ctx context.Context, connID string, takeover bool) (leaseAcquisition, error) {
	key := l.key(connID)

	for attempt := 0; attempt < maxOwnerLeaseAcquireAttempts; attempt++ {
		if err := ctx.Err(); err != nil {
			return leaseAcquisition{}, err
		}

		raw, exists, err := l.coord.Get(ctx, key)
		if err != nil {
			return leaseAcquisition{}, err
		}

		var expected []byte
		acq := leaseAcquisition{generation: 1}
		if exists {
			priorNodeID, generation, expiresAtMs, decodeOK := decodeLeaseValue(raw)
			if !decodeOK {
				return leaseAcquisition{}, errCorruptOwnerLease
			}
			if time.Now().UnixMilli() < expiresAtMs && !takeover {
				return leaseAcquisition{}, ErrOwnerHeld
			}
			expected = raw
			acq.generation = generation + 1
			acq.hadPrior = true
			acq.priorNodeID = priorNodeID
			acq.priorGeneration = generation
			acq.priorExpiresAtMs = expiresAtMs
		}

		newValue := encodeLeaseValue(l.nodeID, acq.generation, time.Now().Add(l.ttl).UnixMilli())
		swapped, err := l.coord.CompareAndSwap(ctx, key, expected, newValue, l.retentionTTL())
		if err != nil {
			return leaseAcquisition{}, err
		}
		if swapped {
			return acq, nil
		}
		// Another acquire, refresh, or takeover landed between our Get and this
		// CompareAndSwap. Re-read the now-current record and re-evaluate against it.
	}

	return leaseAcquisition{}, ErrOwnerHeld
}

// abortTakeover reverts a takeover acquisition that acq recorded, restoring the coordinator
// record to whatever it held immediately before that acquisition - if that record is still the
// exact one this acquisition wrote. It exists for a takeover whose physical eviction never
// completes (spawnConnActor times out, errors, or its context is cancelled): the lease was
// preempted to fence the old owner the instant the takeover was attempted (see acquire's
// takeover branch), but if the new owner never actually took over the connection, leaving that
// preemption in place would permanently reject the old owner's still-legitimate refresh/delivery
// calls with ErrStaleOwner - a connection that was never really dislodged, killed anyway.
//
// It is a no-op, not an error, when the coordinator's current record no longer matches what this
// acquisition wrote: a real subsequent takeover (or this connection's own normal release) has
// already superseded it, and clobbering that would reopen the very fencing hole this whole
// mechanism exists to close.
//
// When acq had no prior owner (a brand new connection id, or one whose previous lease had
// already lapsed), there is nothing to restore: the record is tombstoned exactly as a normal
// release would, so a later acquire sees it as absent/expired rather than stuck on this node's
// abandoned claim.
func (l *ownerLease) abortTakeover(ctx context.Context, connID string, acq leaseAcquisition) error {
	key := l.key(connID)

	raw, exists, err := l.coord.Get(ctx, key)
	if err != nil {
		return err
	}
	if !exists {
		return nil
	}

	nodeID, generation, _, decodeOK := decodeLeaseValue(raw)
	if !decodeOK || nodeID != l.nodeID || generation != acq.generation {
		return nil
	}

	var restored []byte
	if acq.hadPrior {
		// The failed takeover never actually dislodged this owner, so give back exactly the
		// ownership it held, with a freshly extended expiry: its own refresh loop, not this
		// aborted attempt, is what should decide when it next needs renewing.
		restored = encodeLeaseValue(acq.priorNodeID, acq.priorGeneration, time.Now().Add(l.ttl).UnixMilli())
	} else {
		restored = encodeLeaseValue(l.nodeID, acq.generation, 0)
	}
	_, err = l.coord.CompareAndSwap(ctx, key, raw, restored, l.retentionTTL())
	return err
}

// refresh extends the ttl of the lease this caller holds at generation, without changing
// the generation. It fails with ErrStaleOwner if the coordinator's current record no
// longer names this node and generation as the owner - either because a takeover already
// bumped the generation, or because the record is gone entirely (e.g. its ttl elapsed and
// nothing has re-acquired it yet).
func (l *ownerLease) refresh(ctx context.Context, connID string, generation uint64) error {
	key := l.key(connID)

	raw, exists, err := l.coord.Get(ctx, key)
	if err != nil {
		return err
	}
	if !exists {
		return ErrStaleOwner
	}

	nodeID, currentGeneration, _, decodeOK := decodeLeaseValue(raw)
	if !decodeOK {
		return errCorruptOwnerLease
	}
	if nodeID != l.nodeID || currentGeneration != generation {
		return ErrStaleOwner
	}

	newValue := encodeLeaseValue(l.nodeID, generation, time.Now().Add(l.ttl).UnixMilli())
	swapped, err := l.coord.CompareAndSwap(ctx, key, raw, newValue, l.retentionTTL())
	if err != nil {
		return err
	}
	if !swapped {
		// A takeover's CompareAndSwap landed between our Get and this one: our
		// generation is no longer current even though it matched a moment ago.
		return ErrStaleOwner
	}
	return nil
}

// release gives up ownership of connID at generation. It is a deliberate no-op, not an
// error, when the coordinator's current record no longer names this node and generation as
// the owner: a takeover has already superseded it, and releasing must never be able to
// clear a later owner's lease.
//
// release does not delete the coordinator key. It overwrites it with a tombstone record
// carrying an already-elapsed expiry (Unix millisecond 0), so an immediately following
// acquire for the same connID sees it as expired - exactly as if the record were absent -
// without requiring a delete primitive from CASCoordinator.
func (l *ownerLease) release(ctx context.Context, connID string, generation uint64) error {
	key := l.key(connID)

	raw, exists, err := l.coord.Get(ctx, key)
	if err != nil {
		return err
	}
	if !exists {
		return nil
	}

	nodeID, currentGeneration, _, decodeOK := decodeLeaseValue(raw)
	if !decodeOK || nodeID != l.nodeID || currentGeneration != generation {
		return nil
	}

	tombstone := encodeLeaseValue(l.nodeID, currentGeneration, 0)
	_, err = l.coord.CompareAndSwap(ctx, key, raw, tombstone, l.retentionTTL())
	return err
}

// ownerNode reports the node and generation currently recorded as owning connID. ok is
// false when there is no record, the record failed to decode, or the record has expired -
// in every case there is no live owner to route delivery to or fence a stale one against.
func (l *ownerLease) ownerNode(ctx context.Context, connID string) (nodeID string, generation uint64, ok bool, err error) {
	raw, exists, err := l.coord.Get(ctx, l.key(connID))
	if err != nil {
		return "", 0, false, err
	}
	if !exists {
		return "", 0, false, nil
	}

	decodedNodeID, decodedGeneration, expiresAtMs, decodeOK := decodeLeaseValue(raw)
	if !decodeOK || time.Now().UnixMilli() >= expiresAtMs {
		return "", 0, false, nil
	}
	return decodedNodeID, decodedGeneration, true, nil
}
