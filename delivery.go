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

// DeliveryResult reports what a fan-out delivery (Registry.SendToGroup,
// Registry.Broadcast) actually did. It is deliberately three separate counters rather
// than a single "sent" number, because the three outcomes have very different meanings
// for the caller: Delivered is a socket that will get the payload, Dropped is a socket
// that exists but lost the payload to backpressure, and Remote is a fan-out this node
// handed to the cluster and can no longer observe.
type DeliveryResult struct {
	// Delivered counts the connections held by this node whose outbound buffer accepted
	// the payload. It is not an acknowledgement that the bytes reached the peer.
	Delivered int

	// Dropped counts the connections held by this node that could not take the payload,
	// overwhelmingly because their outbound buffer was full (see ErrBackpressure). It also
	// counts a local entry a cross-node takeover has already fenced but not yet evicted (see
	// WithOwnerLease and Registry.staleOwner): the connection is still present in this node's
	// table, so it counts here rather than nowhere, but nothing was written to its socket.
	Dropped int

	// Remote counts the cluster fan-outs this node published for the delivery, or, on the
	// confirmed path, the remote sockets that acknowledged the write.
	//
	// Its meaning, and how much to trust it, depends on configuration:
	//   - No Presence backend: the registry cannot tell how many nodes hold members, so it
	//     is 1 whenever a cluster fan-out was published and 0 otherwise.
	//   - Presence, no WithDeliveryConfirmation: it is the number of group members Presence
	//     reports on other nodes. Presence membership is lease-based and lags reality, so a
	//     member on a node that has just crashed still counts until its lease lapses (up to
	//     the presence TTL). Remote can therefore be non-zero for a delivery no live socket
	//     will take.
	//   - Presence and WithDeliveryConfirmation: it is the number of remote sockets that
	//     acknowledged the write within the confirmation timeout. A member that received the
	//     payload but did not acknowledge in time (a slow or paused node) is not counted, so
	//     Remote can undercount an at-least-once delivery.
	Remote int
}

// Total returns the number of connections this delivery reached in any way, successful
// or not.
func (d DeliveryResult) Total() int {
	return d.Delivered + d.Dropped + d.Remote
}

// None reports whether the delivery reached nothing this node could account for: no local
// connection, and no remote fan-out or confirmed remote write (see Remote). It is the signal
// to fall back to an offline channel such as web push.
//
// None is exactly as accurate as Remote, and so is not an absolute "nothing anywhere took
// this payload":
//   - Without a Presence backend a clustered registry always publishes a fan-out, so None is
//     never true even when no node holds a matching connection.
//   - With Presence but no WithDeliveryConfirmation, a stale membership lease for a just
//     crashed node keeps None false for a delivery that in fact reached no live socket, up to
//     the presence TTL. Only WithDeliveryConfirmation closes this window.
//   - With Presence and WithDeliveryConfirmation, None becomes true when no remote socket
//     acknowledged within the confirmation timeout. A node that buffered the payload but was
//     too slow to acknowledge is not counted, so the offline fallback is at-least-once: it
//     may fire for an identity a slow node did ultimately deliver to, duplicating that
//     message on the offline channel. That is the deliberate trade: never lose a
//     notification, at the cost of a possible duplicate.
func (d DeliveryResult) None() bool {
	return d.Total() == 0
}
