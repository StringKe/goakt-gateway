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

package gateway_test

import (
	"context"
	"fmt"

	"github.com/tochemey/goakt/v4/actor"
	"github.com/tochemey/goakt/v4/log"

	gateway "github.com/StringKe/goakt-gateway"
)

// ExampleRegistry_SendToGroup shows one identity ("user:1") with two devices registered on
// this node. SendToGroup fans out to both of them, and a send to an identity that is not
// connected anywhere reports DeliveryResult.None, which is the signal to fall back to an
// offline channel such as web push.
func ExampleRegistry_SendToGroup() {
	ctx := context.Background()

	// Group fan-out rides on the internal pub/sub topic actor, so the system needs pub/sub
	// enabled even for a single-node, non-clustered deployment.
	system, err := actor.NewActorSystem("example", actor.WithLogger(log.DiscardLogger), actor.WithPubSub())
	if err != nil {
		panic(err)
	}
	if err := system.Start(ctx); err != nil {
		panic(err)
	}
	defer func() { _ = system.Stop(ctx) }()

	registry := gateway.NewRegistry(system, log.DiscardLogger)

	// Two devices of the same identity share one group.
	send := func([]byte) error { return nil }
	_ = registry.Register(ctx, "phone", send, gateway.WithConnGroup("user:1"))
	_ = registry.Register(ctx, "laptop", send, gateway.WithConnGroup("user:1"))

	online, _ := registry.SendToGroup(ctx, "user:1", []byte("ping"))
	fmt.Println("delivered:", online.Delivered)
	fmt.Println("online none:", online.None())

	offline, _ := registry.SendToGroup(ctx, "user:2", []byte("ping"))
	fmt.Println("offline none:", offline.None())

	// Output:
	// delivered: 2
	// online none: false
	// offline none: true
}
