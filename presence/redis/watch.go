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

package redis

import (
	"context"
	"encoding/json"
	"sync"
	"time"

	gateway "github.com/StringKe/goakt-gateway"
)

// This Presence implements the optional Presence extensions so a Registry backed by it gets
// cluster-wide membership watching (WatchPresence) and cluster-wide member enumeration with
// metadata (GroupMembers).
var (
	_ gateway.PresenceWatcher    = (*Presence)(nil)
	_ gateway.PresenceDirectory  = (*Presence)(nil)
	_ gateway.PresenceMetaJoiner = (*Presence)(nil)
)

// watchChannelBuffer sizes a watcher's delivery channel. Events are handed off with a
// non-blocking send, so a subscriber that falls this far behind loses the overflow rather
// than stalling the reader goroutine. This mirrors the best-effort contract of Pub/Sub
// itself: the stream is a hint to re-read authoritative state, not a gap-free log.
const watchChannelBuffer = 64

// wireEvent is the on-the-wire form of a PresenceEvent. The group is carried by the channel
// name a subscriber is listening on, so only the connection id and kind travel in the
// payload. Field names are single letters to keep the published bytes small.
type wireEvent struct {
	ConnID string `json:"c"`
	Kind   int    `json:"k"`
}

// metaKey is the per-group HASH that maps connection id to its JSON-encoded metadata. Its
// namespace infix differs in its first byte from the member key's, so a group named after
// the metadata infix cannot alias another group's member set, and it carries the same hash
// tag (see hashTag) so it shares a Redis Cluster slot with that group's member set - which
// is what lets the join and leave scripts maintain both keys in one atomic call.
func (p *Presence) metaKey(group string) string {
	return p.prefix + metaInfix + hashTag(group) + hashTagEnd
}

// eventChannel is the Pub/Sub channel a group's membership events are published on. A
// channel name lives in a namespace separate from keys, so it never collides with a group's
// sorted set even for a group literally named "events:...".
func (p *Presence) eventChannel(group string) string {
	return p.prefix + "events:" + group
}

// encodeEvent renders a membership event as its Pub/Sub payload. Marshalling a struct of a
// string and an int cannot fail, so the error is intentionally dropped rather than propagated
// into the hot membership path.
func (p *Presence) encodeEvent(connID string, kind gateway.PresenceEventKind) string {
	payload, _ := json.Marshal(wireEvent{ConnID: connID, Kind: int(kind)})
	return string(payload)
}

// decodeEvent parses a Pub/Sub payload back into a PresenceEvent for group. A payload that
// does not parse is reported as invalid so the reader can skip it rather than deliver a
// zero-valued event.
func decodeEvent(group, payload string) (gateway.PresenceEvent, bool) {
	var wire wireEvent
	if err := json.Unmarshal([]byte(payload), &wire); err != nil {
		return gateway.PresenceEvent{}, false
	}
	kind := gateway.PresenceJoin
	if wire.Kind == int(gateway.PresenceLeave) {
		kind = gateway.PresenceLeave
	}
	return gateway.PresenceEvent{Group: group, ConnID: wire.ConnID, Kind: kind}, true
}

// JoinWithMeta records connID as an online member of group for at most ttl along with its
// metadata, so Entries (and therefore Registry.GroupMembers) can return it cluster-wide. It
// has the same lease and event semantics as Join. The metadata write and the membership
// write happen inside the same writeMemberScript call (see redis.go), so a failure partway
// through can never leave metadata recorded for a connection that never actually joined, or
// a join whose metadata never got written: the two either both land or neither does.
func (p *Presence) JoinWithMeta(ctx context.Context, group, connID string, meta map[string]string, ttl time.Duration) error {
	payload, err := json.Marshal(meta)
	if err != nil {
		return err
	}
	return p.writeMember(ctx, group, connID, ttl, true, payload, false, 0)
}

// Entries returns every online member of group across the cluster with its recorded
// metadata. Membership is read authoritatively from the sorted set (which sweeps lapsed
// leases), and the metadata HASH is consulted only to decorate the live members, so a stale
// metadata entry for a member that has since left is never returned.
func (p *Presence) Entries(ctx context.Context, group string) ([]gateway.PresenceEntry, error) {
	members, err := p.Members(ctx, group)
	if err != nil {
		return nil, err
	}
	if len(members) == 0 {
		return nil, nil
	}
	raw, err := p.client.HMGet(ctx, p.metaKey(group), members...).Result()
	if err != nil {
		return nil, err
	}
	entries := make([]gateway.PresenceEntry, 0, len(members))
	for i, id := range members {
		entry := gateway.PresenceEntry{ConnID: id}
		if i < len(raw) {
			if encoded, ok := raw[i].(string); ok && encoded != "" {
				var meta map[string]string
				if err := json.Unmarshal([]byte(encoded), &meta); err == nil && len(meta) > 0 {
					entry.Meta = meta
				}
			}
		}
		entries = append(entries, entry)
	}
	return entries, nil
}

// Watch subscribes to membership changes for group over Redis Pub/Sub. It returns a receive
// channel of events, a cancel that unsubscribes and closes the channel, and an error. The
// channel is closed when cancel is called or ctx is cancelled. Delivery is best-effort:
// events that occur while the subscriber is disconnected from Redis are not backfilled, and a
// subscriber that does not keep up loses overflow.
func (p *Presence) Watch(ctx context.Context, group string) (<-chan gateway.PresenceEvent, func(), error) {
	pubsub := p.client.Subscribe(ctx, p.eventChannel(group))
	// Block until the subscription is confirmed by Redis so that a caller which subscribes and
	// then triggers a join observes it: any PUBLISH after this point reaches this subscriber.
	if _, err := pubsub.Receive(ctx); err != nil {
		_ = pubsub.Close()
		return nil, nil, err
	}

	out := make(chan gateway.PresenceEvent, watchChannelBuffer)
	messages := pubsub.Channel()
	done := make(chan struct{})
	var once sync.Once
	cancel := func() {
		once.Do(func() {
			close(done)
			_ = pubsub.Close()
		})
	}

	go func() {
		defer close(out)
		for {
			select {
			case <-ctx.Done():
				// A cancelled context must release the subscription even if the caller never
				// calls cancel, so a watch does not outlive the scope it was created for.
				cancel()
				return
			case <-done:
				return
			case msg, ok := <-messages:
				if !ok {
					return
				}
				event, valid := decodeEvent(group, msg.Payload)
				if !valid {
					continue
				}
				select {
				case out <- event:
				default:
				}
			}
		}
	}()

	return out, cancel, nil
}
