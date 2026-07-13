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
	"fmt"
	"sync"

	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"

	"github.com/tochemey/goakt/v4/actor"
	"github.com/tochemey/goakt/v4/log"
	"github.com/tochemey/goakt/v4/supervisor"
)

// bridgeActorNamePrefix names the internal actor spawned per topicSubscription. It
// intentionally does not start with the reserved "GoAkt" actor name prefix (see
// upstream's reservedNamesPrefix) so the bridge actor can be spawned and shut down like
// any ordinary actor from outside the actor package.
const bridgeActorNamePrefix = "goaktGatewayPubSubBridge"

// topicSubscription is a non-actor subscription to a pub/sub topic, built entirely on
// GoAkt's public actor.Subscribe/actor.Unsubscribe messaging (see actor.ActorSystem.
// TopicActor) rather than any internal pub/sub plumbing. Registry uses it to bridge
// Broadcast across the cluster without requiring callers to define their own Actor.
type topicSubscription struct {
	topic         string
	pid           *actor.PID
	topicActorPID *actor.PID
	system        actor.ActorSystem

	once sync.Once
	err  error
}

// subscribeTopic spawns a bridge actor that subscribes to topic on the actor system's
// TopicActor and forwards every delivered message to handler. It returns
// ErrPubSubUnavailable if the actor system was not started with pub/sub enabled.
//
// The bridge actor subscribes exactly like any other actor subscriber (Subscribe/Watch/
// Terminated on the topic actor's side), so dedup, retention, and cross-node
// dissemination semantics owned by the topic actor are untouched; this function only
// supplies the plain-callback bridging on top.
func subscribeTopic(ctx context.Context, system actor.ActorSystem, topic string, handler func(ctx context.Context, message proto.Message)) (*topicSubscription, error) {
	topicActorPID := system.TopicActor()
	if topicActorPID == nil {
		return nil, ErrPubSubUnavailable
	}

	name := fmt.Sprintf("%s-%s", bridgeActorNamePrefix, uuid.NewString())
	ready := make(chan struct{})
	pid, err := system.Spawn(ctx, name,
		newBridgeActor(topic, topicActorPID, handler, ready),
		actor.WithLongLived(),
		actor.WithRelocationDisabled(),
		actor.WithSupervisor(
			supervisor.NewSupervisor(
				supervisor.WithStrategy(supervisor.OneForOneStrategy),
				supervisor.WithAnyErrorDirective(supervisor.ResumeDirective),
			),
		),
	)
	if err != nil {
		return nil, err
	}

	subscription := &topicSubscription{topic: topic, pid: pid, topicActorPID: topicActorPID, system: system}
	select {
	case <-ready:
		return subscription, nil
	case <-ctx.Done():
		_ = subscription.Close()
		return nil, ctx.Err()
	}
}

// Close unsubscribes from the topic and stops the bridge actor. Safe to call more than
// once; only the first call has any effect.
func (s *topicSubscription) Close() error {
	s.once.Do(func() {
		ctx := context.Background()
		s.err = s.system.NoSender().Tell(ctx, s.topicActorPID, actor.NewUnsubscribe(s.topic))
		if shutdownErr := s.pid.Shutdown(ctx); shutdownErr != nil && s.err == nil {
			s.err = shutdownErr
		}
	})
	return s.err
}

// bridgeActor forwards messages published to a topic to a plain callback, so that
// Registry does not need to define and spawn its own Actor type for cluster-wide
// broadcast fan-out. It subscribes on PostStart and signals readiness only after the topic
// actor confirms that subscription.
type bridgeActor struct {
	topic         string
	topicActorPID *actor.PID
	handler       func(ctx context.Context, message proto.Message)
	ready         chan struct{}
	readyOnce     sync.Once
	logger        log.Logger
}

// enforce compilation error
var _ actor.Actor = (*bridgeActor)(nil)

// newBridgeActor creates the actor backing a topicSubscription.
func newBridgeActor(topic string, topicActorPID *actor.PID, handler func(ctx context.Context, message proto.Message), ready chan struct{}) *bridgeActor {
	return &bridgeActor{topic: topic, topicActorPID: topicActorPID, handler: handler, ready: ready}
}

// PreStart is called before the actor starts.
func (a *bridgeActor) PreStart(*actor.Context) error {
	return nil
}

// Receive subscribes to the topic on PostStart and forwards every other delivered
// message to handler.
func (a *bridgeActor) Receive(ctx *actor.ReceiveContext) {
	switch message := ctx.Message().(type) {
	case *actor.PostStart:
		a.logger = ctx.Logger()
		ctx.Tell(a.topicActorPID, actor.NewSubscribe(a.topic))
	case *actor.SubscribeAck:
		if message.Topic() == a.topic {
			a.readyOnce.Do(func() { close(a.ready) })
		}
	case *actor.UnsubscribeAck, *actor.Terminated:
		// subscription lifecycle signals; nothing to forward to the handler.
	default:
		a.dispatch(ctx)
	}
}

// dispatch forwards a delivered topic message to the registered handler.
func (a *bridgeActor) dispatch(ctx *actor.ReceiveContext) {
	message, ok := ctx.Message().(proto.Message)
	if !ok {
		a.logger.Warnf("gateway: pubsub bridge for topic=%q dropped a non-proto message of type %T", a.topic, ctx.Message())
		return
	}
	a.handler(ctx.Context(), message)
}

// PostStop is called when the actor is stopped.
func (a *bridgeActor) PostStop(*actor.Context) error {
	return nil
}
