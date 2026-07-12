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
	"errors"
	"sync"

	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/tochemey/goakt/v4/actor"
	gerrors "github.com/tochemey/goakt/v4/errors"
	"github.com/tochemey/goakt/v4/log"
	"github.com/tochemey/goakt/v4/passivation"
)

// connEntry is the bookkeeping Registry keeps for one locally registered connection.
type connEntry struct {
	id     string
	send   func([]byte) error
	pid    *actor.PID
	topics map[string]struct{}

	// reserved is true from the moment Register publishes this entry under the id
	// until its backing actor has finished spawning. It lets a concurrent Unregister
	// for the same id detect the in-flight registration instead of racing past it.
	reserved bool
	// dead is set by Unregister when it finds the id still reserved: it tells the
	// in-flight Register to roll back the spawn instead of finalizing an entry the
	// caller of Unregister already believes is gone.
	dead bool
}

// topicBridge is the cluster-wide fan-out side of a locally joined topic: a
// topicSubscription (see bridge.go) that receives publishes from every node in the
// cluster, including this one's own echo, and re-delivers them to this node's local
// members of the topic.
type topicBridge struct {
	subscription *topicSubscription
	members      int
}

// Registry is the local, per-node table of registered WebSocket/SSE connections. It is
// the "local connection table" the two-tier delivery model described in the gateway
// package documentation is built around: SendToConnection and Broadcast always prefer a
// direct write to a locally held connection over any actor/cluster machinery, and only
// fall back to cluster addressing when the target is not held by this node.
//
// A Registry is safe for concurrent use.
type Registry struct {
	system actor.ActorSystem
	logger log.Logger
	origin uuid.UUID

	mu     sync.RWMutex
	conns  map[string]*connEntry
	topics map[string]map[string]struct{} // topic -> set of local connection ids

	bridgeMu sync.Mutex
	bridges  map[string]*topicBridge // topic -> cluster fan-out subscription
}

// NewRegistry creates a Registry backed by system. system is used to spawn the
// per-connection ephemeral actors (relocation disabled, long-lived passivation) that make
// registered connections addressable from other nodes, and to bridge topic broadcasts
// across the cluster (see bridge.go).
func NewRegistry(system actor.ActorSystem, logger log.Logger) *Registry {
	if logger == nil {
		logger = log.DiscardLogger
	}
	return &Registry{
		system:  system,
		logger:  logger,
		origin:  uuid.New(),
		conns:   make(map[string]*connEntry),
		topics:  make(map[string]map[string]struct{}),
		bridges: make(map[string]*topicBridge),
	}
}

// registerSpawnBarrier, when non-nil, is invoked with id after Register has reserved id
// in the connection table and before it spawns the backing actor. It exists solely so
// tests can deterministically interleave a concurrent Unregister within that window;
// production code never sets it.
var registerSpawnBarrier func(id string)

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
// It returns ErrConnectionExists if id is already registered.
func (r *Registry) Register(ctx context.Context, id string, send func([]byte) error, topics ...string) error {
	entry := &connEntry{
		id:       id,
		send:     send,
		topics:   make(map[string]struct{}),
		reserved: true,
	}

	r.mu.Lock()
	if _, exists := r.conns[id]; exists {
		r.mu.Unlock()
		return ErrConnectionExists
	}
	r.conns[id] = entry
	r.mu.Unlock()

	if registerSpawnBarrier != nil {
		registerSpawnBarrier(id)
	}

	// no relocation (socket dies with its node), no passivation (Unregister stops it)
	pid, spawnErr := r.system.Spawn(ctx, connActorName(id), newConnActor(send),
		actor.WithRelocationDisabled(),
		actor.WithPassivationStrategy(passivation.NewLongLivedStrategy()),
	)

	r.mu.Lock()
	if entry.dead || spawnErr != nil {
		// Either the spawn failed, or a concurrent Unregister already claimed id while
		// it was still reserved: undo the reservation instead of finalizing it.
		delete(r.conns, id)
		r.mu.Unlock()
		if spawnErr != nil {
			return spawnErr
		}
		if shutdownErr := pid.Shutdown(ctx); shutdownErr != nil {
			r.logger.Warnf("gateway: failed to shut down actor for concurrently unregistered connection %q: %v", id, shutdownErr)
		}
		return ErrConnectionClosed
	}
	entry.pid = pid
	entry.reserved = false
	r.mu.Unlock()

	for _, topic := range topics {
		if err := r.Join(ctx, id, topic); err != nil {
			r.logger.Warnf("gateway: failed to join connection %q to topic %q: %v", id, topic, err)
		}
	}

	return nil
}

// Unregister removes a connection from the local table, leaves every topic it had
// joined, and shuts down its backing actor. It is a no-op if id is not registered.
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
	delete(r.conns, id)
	topicsToLeave := make([]string, 0, len(entry.topics))
	for topic := range entry.topics {
		topicsToLeave = append(topicsToLeave, topic)
	}
	r.mu.Unlock()

	for _, topic := range topicsToLeave {
		r.leaveTopicLocked(id, topic)
	}

	if entry.pid == nil {
		return nil
	}
	return entry.pid.Shutdown(ctx)
}

// Join adds an already-registered connection to a topic's local membership, creating
// the cluster fan-out bridge for that topic on first use. It returns ErrConnectionNotFound
// if id is not registered.
func (r *Registry) Join(_ context.Context, id, topic string) error {
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
	entry.topics[topic] = struct{}{}
	if r.topics[topic] == nil {
		r.topics[topic] = make(map[string]struct{})
	}
	r.topics[topic][id] = struct{}{}
	r.mu.Unlock()

	r.ensureBridge(topic)
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

	r.leaveTopicLocked(id, topic)
	return nil
}

// leaveTopicLocked removes id from topic's local membership set and tears down the
// bridge subscription once the topic has no more local members.
func (r *Registry) leaveTopicLocked(id, topic string) {
	r.mu.Lock()
	members, ok := r.topics[topic]
	if ok {
		delete(members, id)
		if len(members) == 0 {
			delete(r.topics, topic)
		}
	}
	r.mu.Unlock()

	if !ok || len(members) != 0 {
		return
	}
	r.releaseBridge(topic)
}

// ensureBridge lazily creates the topic bridge for topic (see bridge.go) so that a
// Broadcast on any node in the cluster reaches this node's local topic members. Safe to
// call more than once per topic.
func (r *Registry) ensureBridge(topic string) {
	r.bridgeMu.Lock()
	defer r.bridgeMu.Unlock()

	if b, exists := r.bridges[topic]; exists {
		b.members++
		return
	}

	sub, err := subscribeTopic(r.system, topic, func(_ context.Context, message proto.Message) {
		r.deliverFromBridge(topic, message)
	})
	if err != nil {
		r.logger.Warnf("gateway: failed to bridge topic %q: %v", topic, err)
		return
	}
	r.bridges[topic] = &topicBridge{subscription: sub, members: 1}
}

// releaseBridge decrements the bridge's reference count for topic and tears it down once
// no local connection is joined to it anymore.
func (r *Registry) releaseBridge(topic string) {
	r.bridgeMu.Lock()
	defer r.bridgeMu.Unlock()

	b, exists := r.bridges[topic]
	if !exists {
		return
	}
	b.members--
	if b.members > 0 {
		return
	}
	delete(r.bridges, topic)
	if err := b.subscription.Close(); err != nil {
		r.logger.Warnf("gateway: failed to close topic bridge for %q: %v", topic, err)
	}
}

// deliverFromBridge is invoked whenever a message is published to topic anywhere in the
// cluster (including by this very node - see Broadcast). It strips the broadcast's
// origin tag and re-delivers the payload to this node's local topic members, skipping
// deliveries that originated from this node since Broadcast already wrote to its own
// local members directly.
func (r *Registry) deliverFromBridge(topic string, message proto.Message) {
	bv, ok := message.(*wrapperspb.BytesValue)
	if !ok {
		return
	}
	envelope := bv.GetValue()
	if len(envelope) < len(r.origin) {
		return
	}
	if bytes.Equal(envelope[:len(r.origin)], r.origin[:]) {
		// our own broadcast echoing back through the topic actor: already delivered
		// directly to local members by Broadcast.
		return
	}
	payload := envelope[len(r.origin):]

	r.mu.RLock()
	members := r.topics[topic]
	sends := make([]func([]byte) error, 0, len(members))
	for id := range members {
		if entry, ok := r.conns[id]; ok {
			sends = append(sends, entry.send)
		}
	}
	r.mu.RUnlock()

	for _, send := range sends {
		if err := send(payload); err != nil {
			r.logger.Warnf("gateway: failed to deliver broadcast on topic %q to local connection: %v", topic, err)
		}
	}
}

// SendToConnection delivers payload to the connection identified by id anywhere in the
// cluster. If id is registered on this node, payload is written directly to the socket
// with no actor or cluster machinery involved. Otherwise, SendToConnection resolves the
// connection's owning node through the cluster-aware actor directory
// (ActorSystem.ActorOf) and delivers payload there.
//
// It returns ErrConnectionNotFound if id is not registered anywhere in the cluster.
func (r *Registry) SendToConnection(ctx context.Context, id string, payload []byte) error {
	r.mu.RLock()
	entry, ok := r.conns[id]
	r.mu.RUnlock()
	if ok {
		return entry.send(payload)
	}

	pid, err := r.system.ActorOf(ctx, connActorName(id))
	if err != nil {
		if errors.Is(err, gerrors.ErrActorNotFound) {
			return ErrConnectionNotFound
		}
		return err
	}

	return r.system.NoSender().Tell(ctx, pid, wrapperspb.Bytes(payload))
}

// Broadcast delivers payload to every connection joined to topic across the cluster.
// Local members are written to directly; remote members are reached through the topic
// bridge (see bridge.go).
func (r *Registry) Broadcast(ctx context.Context, topic string, payload []byte) error {
	r.mu.RLock()
	members := r.topics[topic]
	sends := make([]func([]byte) error, 0, len(members))
	for id := range members {
		if entry, ok := r.conns[id]; ok {
			sends = append(sends, entry.send)
		}
	}
	r.mu.RUnlock()

	for _, send := range sends {
		if err := send(payload); err != nil {
			r.logger.Warnf("gateway: failed to deliver broadcast on topic %q to local connection: %v", topic, err)
		}
	}

	if !r.system.InCluster() && !r.hasBridge(topic) {
		// no cluster and no local bridge means there is nothing else to reach.
		return nil
	}

	topicActorPID := r.system.TopicActor()
	if topicActorPID == nil {
		return nil
	}

	envelope := make([]byte, 0, len(r.origin)+len(payload))
	envelope = append(envelope, r.origin[:]...)
	envelope = append(envelope, payload...)

	return r.system.NoSender().Tell(ctx, topicActorPID, actor.NewPublish(uuid.NewString(), topic, wrapperspb.Bytes(envelope)))
}

// hasBridge reports whether a topic bridge already exists for topic.
func (r *Registry) hasBridge(topic string) bool {
	r.bridgeMu.Lock()
	defer r.bridgeMu.Unlock()
	_, exists := r.bridges[topic]
	return exists
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
