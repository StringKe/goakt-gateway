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

// ConnInfo is the identity of a single connection, resolved once during the
// WebSocket/SSE handshake and then carried through registration and every subsequent
// callback. Resolving it once is the point: without it the auth token would be parsed
// again for the id, again for the topics, and again for whatever the application needs
// in its message handler.
type ConnInfo struct {
	// ID is the connection id, unique cluster-wide. When empty, the handler generates a
	// UUID for it.
	ID string

	// Group is the identity the connection belongs to, e.g. "user:123". Several devices
	// or browser tabs of the same identity share one Group, which is what makes
	// Registry.SendToGroup and Registry.IsOnline talk about a person rather than a socket.
	Group string

	// Topics are the pub/sub topics the connection is joined to at registration time.
	Topics []string

	// Meta carries application data resolved during the handshake (roles, tenant, plan)
	// through to the onConnect/onMessage callbacks unchanged.
	Meta map[string]string
}

// BackpressurePolicy decides what a handler does with a connection whose outbound buffer
// is full.
type BackpressurePolicy int

const (
	// BackpressureDrop discards the message and reports ErrBackpressure to the sender,
	// leaving the connection open. This is the default: one slow read must not cost the
	// client its session.
	BackpressureDrop BackpressurePolicy = iota

	// BackpressureClose closes the connection instead of dropping the message. Use it when
	// a client that cannot keep up is more harmful than a client that has to reconnect,
	// e.g. when the stream is only meaningful in full.
	BackpressureClose
)
