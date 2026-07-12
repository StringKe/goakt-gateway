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
	"fmt"

	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/tochemey/goakt/v4/actor"
)

// connActorNamePrefix names the per-connection actor spawned by Registry.Register.
// It intentionally does not start with the reserved "GoAkt" actor name prefix so it can
// be spawned and shut down like any ordinary actor.
const connActorNamePrefix = "goaktGatewayConn"

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
type connActor struct {
	send func([]byte) error
}

// enforce compilation error
var _ actor.Actor = (*connActor)(nil)

// newConnActor creates the actor backing a registered connection. send delivers a
// payload to the underlying socket and is supplied by the WebSocket/SSE handler that
// registered the connection.
func newConnActor(send func([]byte) error) *connActor {
	return &connActor{send: send}
}

// PreStart is called before the actor starts.
func (a *connActor) PreStart(*actor.Context) error {
	return nil
}

// Receive delivers a payload written to this connection from another node.
func (a *connActor) Receive(ctx *actor.ReceiveContext) {
	switch m := ctx.Message().(type) {
	case *wrapperspb.BytesValue:
		if err := a.send(m.GetValue()); err != nil {
			ctx.Logger().Warnf("gateway: failed to deliver remote message to connection: %v", err)
		}
	default:
		ctx.Unhandled()
	}
}

// PostStop is called when the actor is stopped.
func (a *connActor) PostStop(*actor.Context) error {
	return nil
}
