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

// Observer receives the connection and delivery events a Registry produces, so that
// metrics, tracing and audit logging can be attached without this library depending on
// any particular telemetry stack.
//
// Every method is called synchronously on the goroutine that produced the event, which
// is frequently a delivery hot path: implementations must not block. An Observer is
// entirely optional (see WithObserver); a Registry without one calls nothing.
type Observer interface {
	// ConnectionRegistered is called once a connection is fully registered and
	// addressable cluster-wide.
	ConnectionRegistered(id, group string)

	// ConnectionUnregistered is called once a connection has been removed from the local
	// table, including when it is removed to make room for a takeover.
	ConnectionUnregistered(id, group string)

	// ConnectionReplaced is called when a registration with WithReplaceExisting evicted
	// an existing connection with the same id.
	ConnectionReplaced(id, group string)

	// DeliveryDropped is called when a payload could not be queued for a local connection
	// because its outbound buffer was full.
	DeliveryDropped(id, group string)

	// DeliveryFailed is called when a local connection's send function reported an error
	// other than backpressure, e.g. a socket that is already gone.
	DeliveryFailed(id string, err error)

	// BroadcastFanout is called once per Registry.Broadcast with the number of local
	// members the payload was written to on this node.
	BroadcastFanout(topic string, localMembers int)
}

// OfflineObserver is an optional Observer extension. When the configured Observer also
// implements it, the Registry reports every OfflineChannel fallback attempt through it:
// once with a nil error when the offline delivery succeeded, and once with the underlying
// error when it failed. It is kept off the core Observer interface so existing Observer
// implementations do not have to change to compile against a Registry that never uses an
// offline channel.
type OfflineObserver interface {
	// OfflineFallback is called after Registry.SendToGroup routes a group with no reachable
	// socket to the configured OfflineChannel. err is nil on success and the channel's
	// error on failure.
	OfflineFallback(group string, err error)
}
