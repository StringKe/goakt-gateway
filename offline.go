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
	"errors"
	"time"
)

const (
	// defaultOfflineMaxConcurrent bounds how many offline fallback deliveries run at once.
	// SendToGroup issues at most one fallback per call, so under a broadcast storm to many
	// offline identities the number of in-flight fallback goroutines is what would otherwise
	// grow without limit; this caps it. 256 mirrors the default outbound buffer depth used
	// elsewhere in the gateway.
	defaultOfflineMaxConcurrent = 256

	// defaultOfflineTimeout bounds a single offline delivery. Offline transports (web push,
	// mail, SMS) reach the public internet and can hang; without a deadline a stalled one
	// would pin a fallback slot forever and eventually starve the concurrency bound. 10s
	// mirrors the default per-write timeout on the WebSocket path.
	defaultOfflineTimeout = 10 * time.Second
)

// ErrOfflineFallbackOverloaded is reported through OfflineObserver and the logger when an
// offline fallback is dropped because the concurrency bound is saturated, so the drop is
// visible rather than silent.
var ErrOfflineFallbackOverloaded = errors.New("gateway: offline fallback dropped, concurrency limit reached")

// OfflineChannel is an out-of-band delivery path for a group whose identity is not
// reachable over any live socket in the cluster: web push, mail, SMS. Registry.SendToGroup
// calls Deliver automatically when a fan-out reached nothing (DeliveryResult.None) and an
// OfflineChannel is configured (see WithOfflineChannel), so an application does not have to
// branch on the delivery result itself.
//
// Deliver runs off the caller's SendToGroup goroutine and must not be relied on to block
// that call: its outcome is reported through OfflineObserver and the Registry logger, not
// through SendToGroup's return.
type OfflineChannel interface {
	// Deliver hands payload to the offline transport for the identity group. The group is
	// the same identity string SendToGroup fans out to; the implementation maps it to the
	// concrete push subscriptions or addresses it holds.
	Deliver(ctx context.Context, group string, payload []byte) error
}

// OfflineOption tunes how the Registry drives a configured OfflineChannel.
type OfflineOption func(*boundedOffline)

// WithOfflineMaxConcurrent caps how many offline fallback deliveries run concurrently.
// When the cap is reached a further fallback is dropped rather than queued, and the drop is
// reported through OfflineObserver and the logger. Values below 1 are ignored. Defaults to
// 256.
func WithOfflineMaxConcurrent(n int) OfflineOption {
	return func(b *boundedOffline) {
		if n > 0 {
			b.maxConcurrent = n
		}
	}
}

// WithOfflineTimeout bounds a single offline delivery: the context handed to Deliver is
// cancelled after d. Values below or equal to zero are ignored. Defaults to 10 seconds.
func WithOfflineTimeout(d time.Duration) OfflineOption {
	return func(b *boundedOffline) {
		if d > 0 {
			b.timeout = d
		}
	}
}

// boundedOffline is what r.offline actually holds once WithOfflineChannel wraps the
// application's OfflineChannel. The concurrency bound and per-delivery timeout live here so
// they travel with the channel through the single existing r.offline field, without the
// fallback path needing extra Registry state. sem is a counting semaphore: a fallback takes a
// slot before it spawns and returns it when the delivery finishes, so the number of in-flight
// fallback goroutines can never exceed maxConcurrent.
type boundedOffline struct {
	ch            OfflineChannel
	maxConcurrent int
	timeout       time.Duration
	sem           chan struct{}
}

// Deliver forwards to the wrapped channel. It exists so boundedOffline itself satisfies
// OfflineChannel and can be stored in the r.offline field.
func (b *boundedOffline) Deliver(ctx context.Context, group string, payload []byte) error {
	return b.ch.Deliver(ctx, group, payload)
}

// WithOfflineChannel attaches an OfflineChannel the Registry falls back to when SendToGroup
// finds an identity offline everywhere in the cluster. Without it, SendToGroup simply returns
// a DeliveryResult whose None reports the same fact and leaves the fallback to the
// application. The fallback fires exactly when DeliveryResult.None is true, so its accuracy is
// that of None (see its doc): a clustered SendToGroup without a Presence backend never falls
// back, a stale Presence lease can suppress the fallback for up to the presence TTL, and with
// WithDeliveryConfirmation the fallback is at-least-once and may duplicate a slow in-cluster
// delivery onto the offline channel.
//
// Each fallback runs on its own goroutine bounded by WithOfflineMaxConcurrent and with a
// context deadline set by WithOfflineTimeout, so a storm of offline identities cannot spawn
// unbounded goroutines and a stalled transport cannot pin one forever.
func WithOfflineChannel(ch OfflineChannel, opts ...OfflineOption) RegistryOption {
	return func(r *Registry) {
		if ch == nil {
			return
		}
		b := &boundedOffline{
			ch:            ch,
			maxConcurrent: defaultOfflineMaxConcurrent,
			timeout:       defaultOfflineTimeout,
		}
		for _, opt := range opts {
			opt(b)
		}
		b.sem = make(chan struct{}, b.maxConcurrent)
		r.offline = b
	}
}

// maybeOfflineFallback routes group to the configured OfflineChannel when a SendToGroup
// fan-out reached nothing. It runs the delivery on its own goroutine so a slow or blocking
// offline transport cannot hold up SendToGroup's return, and reports the outcome through
// the optional OfflineObserver and the logger rather than through SendToGroup's error.
//
// The goroutine gets a fresh context.WithTimeout rather than SendToGroup's caller context:
// the caller context can be cancelled the moment SendToGroup returns, which would abort a
// fallback that has only just started, while an unbounded Background would let a stalled
// transport pin a concurrency slot forever.
func (r *Registry) maybeOfflineFallback(group string, result DeliveryResult, payload []byte) {
	if r.offline == nil || !result.None() {
		return
	}
	// The r.offline field is only ever set by WithOfflineChannel, which always stores a
	// *boundedOffline, so this assertion holds for every configured offline channel.
	b := r.offline.(*boundedOffline)

	// Take a semaphore slot without blocking SendToGroup. When the bound is saturated the
	// fallback is dropped and reported, which keeps the number of in-flight fallback
	// goroutines bounded under a storm of offline targets.
	select {
	case b.sem <- struct{}{}:
	default:
		r.logger.Warnf("gateway: offline channel fallback for group %q dropped: %v", group, ErrOfflineFallbackOverloaded)
		if o, ok := r.observer.(OfflineObserver); ok {
			o.OfflineFallback(group, ErrOfflineFallbackOverloaded)
		}
		return
	}

	// Copy the payload before handing it to the goroutine: SendToGroup's caller owns the
	// original buffer and may reuse or mutate it the moment SendToGroup returns.
	buf := make([]byte, len(payload))
	copy(buf, payload)

	go func() {
		defer func() { <-b.sem }()

		ctx, cancel := context.WithTimeout(context.Background(), b.timeout)
		defer cancel()

		err := b.ch.Deliver(ctx, group, buf)
		if err != nil {
			r.logger.Warnf("gateway: offline channel delivery failed for group %q: %v", group, err)
		}
		if o, ok := r.observer.(OfflineObserver); ok {
			o.OfflineFallback(group, err)
		}
	}()
}
