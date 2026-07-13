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

// Package webpush implements gateway.OfflineChannel over the Web Push protocol
// (RFC 8291 encryption, RFC 8292 VAPID). When Registry.SendToGroup finds an
// identity offline everywhere in the cluster, the Registry hands the payload to
// Channel.Deliver, which encrypts and POSTs it to every push subscription the
// application holds for that group.
//
// It is a separate package specifically so that importing the root gateway
// package never pulls in github.com/SherClockHolmes/webpush-go and its crypto
// dependencies for applications that do not use offline delivery.
//
// # Expired subscriptions
//
// A push service answers 404 or 410 for an endpoint whose subscription the
// browser has revoked. Deliver treats those two statuses as authoritative
// removal signals and calls SubscriptionStore.Remove so the application can drop
// the dead subscription, rather than retrying it forever.
package webpush

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	webpushgo "github.com/SherClockHolmes/webpush-go"
	gateway "github.com/StringKe/goakt-gateway"
)

// Channel satisfies gateway.OfflineChannel; this fails to compile if the method
// set drifts from the interface the Registry fans out to.
var _ gateway.OfflineChannel = (*Channel)(nil)

// Subscription is a single browser Web Push subscription: the push service
// endpoint URL plus the two client public keys needed to encrypt the payload for
// it. These map directly onto the fields a browser's PushSubscription exposes
// (endpoint, keys.p256dh, keys.auth).
type Subscription struct {
	// Endpoint is the push service URL the encrypted notification is POSTed to.
	Endpoint string
	// P256dh is the base64url-encoded P-256 ECDH public key of the subscription.
	P256dh string
	// Auth is the base64url-encoded authentication secret of the subscription.
	Auth string
}

// SubscriptionStore is the application-owned mapping from an identity group to
// the Web Push subscriptions registered for it. The application populates it as
// clients subscribe; the Channel only reads from it and asks it to remove
// subscriptions the push service has reported as gone.
type SubscriptionStore interface {
	// Get returns every push subscription currently registered for group. An
	// empty slice means the group has no push subscriptions and Deliver becomes
	// a no-op for it.
	Get(ctx context.Context, group string) ([]Subscription, error)
	// Remove drops the subscription identified by endpoint from group. Deliver
	// calls it when the push service answers 404 or 410 for that endpoint, i.e.
	// the browser has revoked the subscription. Remove of an endpoint that is
	// already absent must not error.
	Remove(ctx context.Context, group string, endpoint string) error
}

// Channel is a gateway.OfflineChannel backed by Web Push. Construct it with New.
// It is safe for concurrent use: it holds only immutable configuration and
// delegates all mutable state to the SubscriptionStore and the HTTP client.
type Channel struct {
	vapidPublic  string
	vapidPrivate string
	subject      string
	store        SubscriptionStore

	httpClient webpushgo.HTTPClient
	ttl        int
	urgency    webpushgo.Urgency
	recordSize uint32
}

// Option customizes a Channel at construction time.
type Option func(*Channel)

// WithHTTPClient sets the HTTP client used to POST notifications to push
// services. The default is http.DefaultClient. Supplying a client is the seam
// tests use to redirect delivery to an httptest server, and lets applications
// impose their own timeouts and transport.
func WithHTTPClient(c webpushgo.HTTPClient) Option {
	return func(ch *Channel) { ch.httpClient = c }
}

// WithTTL sets the seconds a push service should retain an undelivered
// notification for an offline client. The default is 60 seconds.
func WithTTL(seconds int) Option {
	return func(ch *Channel) { ch.ttl = seconds }
}

// WithUrgency sets the Urgency header a push service uses to decide how eagerly
// to wake a device. The default is webpushgo.UrgencyNormal.
func WithUrgency(u webpushgo.Urgency) Option {
	return func(ch *Channel) { ch.urgency = u }
}

// WithRecordSize caps the encrypted record size in bytes. Zero (the default)
// lets the underlying library pick its maximum.
func WithRecordSize(size uint32) Option {
	return func(ch *Channel) { ch.recordSize = size }
}

// New builds a Channel that signs notifications with the given VAPID key pair.
// subject is the VAPID "sub" claim identifying the application server to the
// push service, typically a "mailto:" or "https:" URL. store supplies the push
// subscriptions per group and receives removal of subscriptions the push service
// reports as gone.
func New(vapidPublic, vapidPrivate, subject string, store SubscriptionStore, opts ...Option) *Channel {
	ch := &Channel{
		vapidPublic:  vapidPublic,
		vapidPrivate: vapidPrivate,
		subject:      subject,
		store:        store,
		httpClient:   http.DefaultClient,
		ttl:          60,
		urgency:      webpushgo.UrgencyNormal,
	}
	for _, opt := range opts {
		opt(ch)
	}
	return ch
}

// Deliver encrypts payload and POSTs it to every push subscription registered
// for group. It attempts every subscription regardless of individual failures
// and joins the resulting errors, so one dead endpoint does not suppress
// delivery to the rest. A subscription the push service answers 404 or 410 for
// is removed via SubscriptionStore.Remove and is not counted as a delivery error.
//
// Deliver satisfies gateway.OfflineChannel.
func (ch *Channel) Deliver(ctx context.Context, group string, payload []byte) error {
	subs, err := ch.store.Get(ctx, group)
	if err != nil {
		return fmt.Errorf("webpush: load subscriptions for group %q: %w", group, err)
	}

	var errs []error
	for _, sub := range subs {
		if err := ch.deliverOne(ctx, group, sub, payload); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// deliverOne sends payload to a single subscription. It returns nil when the
// push service accepts the notification or when it reports the subscription as
// gone (404/410), because a revoked subscription is an expected, self-healing
// condition rather than a delivery failure to surface to the caller.
func (ch *Channel) deliverOne(ctx context.Context, group string, sub Subscription, payload []byte) error {
	resp, err := webpushgo.SendNotificationWithContext(ctx, payload, &webpushgo.Subscription{
		Endpoint: sub.Endpoint,
		Keys: webpushgo.Keys{
			P256dh: sub.P256dh,
			Auth:   sub.Auth,
		},
	}, &webpushgo.Options{
		HTTPClient:      ch.httpClient,
		Subscriber:      ch.subject,
		VAPIDPublicKey:  ch.vapidPublic,
		VAPIDPrivateKey: ch.vapidPrivate,
		TTL:             ch.ttl,
		Urgency:         ch.urgency,
		RecordSize:      ch.recordSize,
		VapidExpiration: time.Now().Add(12 * time.Hour),
	})
	if err != nil {
		return fmt.Errorf("webpush: send to %s: %w", sub.Endpoint, err)
	}
	// Drain and close so the transport can reuse the connection; the push
	// service response body carries no payload we consume.
	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()

	switch {
	case resp.StatusCode == http.StatusNotFound || resp.StatusCode == http.StatusGone:
		if rerr := ch.store.Remove(ctx, group, sub.Endpoint); rerr != nil {
			return fmt.Errorf("webpush: remove expired subscription %s: %w", sub.Endpoint, rerr)
		}
		return nil
	case resp.StatusCode >= 200 && resp.StatusCode < 300:
		return nil
	default:
		return fmt.Errorf("webpush: push service %s returned status %d", sub.Endpoint, resp.StatusCode)
	}
}
