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
	"errors"
	"fmt"
	"strings"

	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/tochemey/goakt/v4/actor"
)

// connActorNamePrefix names the per-connection actor spawned by Registry.Register.
// It intentionally does not start with the reserved "GoAkt" actor name prefix so it can
// be spawned and shut down like any ordinary actor.
const connActorNamePrefix = "goaktGatewayConn"

// evictReasonPrefix tags a cross-node takeover eviction so a connActor can tell it apart
// from an ordinary remote Disconnect. Both arrive as a StringValue reason, but a Disconnect
// only closes the socket (its handler unregisters as it tears down), whereas an eviction
// must also fully unregister the connection so its cluster-unique actor name is released for
// the new owner. The prefix is a control-byte sequence no human-facing disconnect reason
// carries, so it cannot collide with a real reason string. See Registry.requestRemoteEvict
// and connActor.Receive.
const evictReasonPrefix = "\x00goaktGatewayEvict\x00"

// connActorName derives the cluster-wide actor name a connection is addressable under
// from its connection id. Any node can resolve this name via ActorSystem.ActorOf to
// locate the node that owns the connection, without needing a separate presence store.
func connActorName(id string) string {
	return fmt.Sprintf("%s-%s", connActorNamePrefix, id)
}

// connActor is the addressable identity of a single registered WebSocket/SSE
// connection. It exists purely so that a node other than the one holding the socket
// can locate it (via ActorSystem.ActorOf, which is cluster-aware) and deliver a payload
// to it; the owning node's Registry always prefers a direct, actor-free write on the
// local fast path (see Registry.SendToConnection) and only reaches a remote connActor
// through this indirection.
//
// It resolves the connection through its Registry on every delivery rather than closing
// over the socket's send function, so that a takeover (see WithReplaceExisting) routes
// remote deliveries to the connection that currently owns the id, and so that every
// delivery failure is reported to the Registry's Observer.
type connActor struct {
	registry *Registry
	id       string
	// entry is the exact connection generation this actor was spawned to back. Deliveries
	// re-resolve by id so they follow whichever connection currently owns it, but eviction
	// keys on this specific entry: a takeover evict aimed at this actor must never tear down a
	// newer connection that has since reused the id (see Registry.evictLocal).
	entry *connEntry
}

// enforce compilation error
var _ actor.Actor = (*connActor)(nil)

// newConnActor creates the actor backing the given connection entry, addressable under
// connActorName(id).
func newConnActor(registry *Registry, id string, entry *connEntry) *connActor {
	return &connActor{registry: registry, id: id, entry: entry}
}

// PreStart is called before the actor starts.
func (a *connActor) PreStart(*actor.Context) error {
	return nil
}

// Receive handles the two messages a connActor understands from another node: a
// BytesValue payload to write to the local socket, and a StringValue carrying a reason for a
// remote-initiated Registry.Disconnect.
func (a *connActor) Receive(ctx *actor.ReceiveContext) {
	switch m := ctx.Message().(type) {
	case *wrapperspb.BytesValue:
		// sendTo re-reads the registry's table for a.id and, through deliverOne, fences the
		// write against whichever entry currently answers to it - not necessarily a.entry: a
		// same-node reconnect can have replaced a.entry with a fresh, higher-generation local
		// entry since this actor was spawned, and that newer entry must still receive the
		// delivery rather than being rejected as stale (see Registry.sendTo).
		err := a.registry.sendTo(ctx.Context(), a.id, m.GetValue())
		if err != nil && !errors.Is(err, ErrStaleOwner) {
			ctx.Logger().Warnf("gateway: failed to deliver remote message to connection %q: %v", a.id, err)
		}
		// Reply with an ack so a sender that used Ask (WithDeliveryConfirmation) learns the
		// payload reached this node's socket buffer, and reports whether the write succeeded.
		// When the sender used Tell (the default, fire-and-forget path), Response is a no-op,
		// so this is safe and cost-free on the unconfirmed path. A stale-owner rejection (a
		// takeover elsewhere has already superseded the generation whichever local entry
		// currently answers to a.id was spawned to back) acks false without logging it as a
		// failure: it is the fencing working as intended, not a delivery error.
		ctx.Response(wrapperspb.Bool(err == nil))
	case *wrapperspb.StringValue:
		if reason, isEvict := strings.CutPrefix(m.GetValue(), evictReasonPrefix); isEvict {
			// A cross-node takeover reaches the current owner as an evict-tagged reason: the
			// owning node force-closes and fully unregisters the connection so its
			// cluster-unique actor name is released for the node taking over.
			a.registry.evictLocal(a.entry, reason)
			return
		}
		// A remote Registry.Disconnect reaches the owning node as a StringValue reason; the
		// owning node runs the connection's registered close hook to force the socket shut.
		a.registry.triggerCloseHook(a.id, m.GetValue())
	default:
		ctx.Unhandled()
	}
}

// PostStop is called when the actor is stopped.
func (a *connActor) PostStop(*actor.Context) error {
	return nil
}
