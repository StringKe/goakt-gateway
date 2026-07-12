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

// Package gateway turns a GoAkt cluster into a single, cluster-aware ingress tier for
// HTTP, WebSocket, and Server-Sent Events (SSE) traffic - the role an nginx/caddy
// front-tier normally plays in front of a stateless HTTP fleet.
//
// Two design boundaries drive every type in this package:
//
//   - Plain HTTP requests are served by ordinary net/http handlers with zero actor
//     involvement. Actorizing a request/response cycle that lives for a few
//     milliseconds is pure overhead; nothing in this package puts one in the way of a
//     short-lived HTTP handler.
//   - Only long-lived WebSocket/SSE connections get an addressable identity. Each
//     accepted connection is registered in a local Registry and, so that it can be
//     addressed from any node in the cluster, backed by a lightweight ephemeral actor
//     (relocation disabled, long-lived passivation) whose sole job is to relay a
//     delivery to the socket it owns.
//
// Message delivery to a connection is two-tier: Registry.SendToConnection checks the
// local connection table first and, on a hit, writes the socket directly with no actor
// or cluster machinery involved. Only on a miss does it fall back to a cluster-wide
// actor lookup (actor.ActorSystem.ActorOf) and a remote Tell to the node that owns the
// connection. Registry.Broadcast follows the same shape for topic fan-out: local
// subscribers are written directly, and remote nodes are reached through a small
// pub/sub bridge built entirely on GoAkt's public actor.Subscribe/actor.Unsubscribe API
// (see bridge.go) - this package has no dependency on any GoAkt internal package.
//
// The TLS side of the package (CertIssuer, CertStore, Manager) lets every node in the
// cluster terminate TLS for any hosted domain from one certificate, issued exactly once
// cluster-wide: issuance is arbitrated through a storage-agnostic Coordinator this
// library owns (see coordinator.go), the resulting certificate is distributed to every
// node through the same Coordinator, and renewal is driven by GoAkt's cluster-single-fire
// cron schedule (actor.ActorSystem.ScheduleWithCron) so only one node re-issues per
// renewal window. See Manager, CertIssuer, CertStore, and Coordinator.
package gateway
