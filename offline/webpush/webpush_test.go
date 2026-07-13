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

package webpush_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	webpushgo "github.com/SherClockHolmes/webpush-go"
	"github.com/stretchr/testify/require"

	"github.com/StringKe/goakt-gateway/offline/webpush"
)

// memStore is an in-memory SubscriptionStore for exercising Channel without a
// real push service. It records Remove calls so tests can assert soft deletion.
type memStore struct {
	mu   sync.Mutex
	subs map[string][]webpush.Subscription
}

func newMemStore() *memStore {
	return &memStore{subs: make(map[string][]webpush.Subscription)}
}

func (s *memStore) add(group string, sub webpush.Subscription) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.subs[group] = append(s.subs[group], sub)
}

func (s *memStore) Get(_ context.Context, group string) ([]webpush.Subscription, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]webpush.Subscription, len(s.subs[group]))
	copy(out, s.subs[group])
	return out, nil
}

func (s *memStore) Remove(_ context.Context, group, endpoint string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	kept := s.subs[group][:0]
	for _, sub := range s.subs[group] {
		if sub.Endpoint != endpoint {
			kept = append(kept, sub)
		}
	}
	s.subs[group] = kept
	return nil
}

func (s *memStore) count(group string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.subs[group])
}

// testKeys is a browser-side P-256 subscription key pair matching the encryption
// the library performs. The auth secret and P256dh public key below are a
// self-consistent pair so SendNotification encrypts without error; the fake push
// endpoint never decrypts, so the payload plaintext is asserted at the HTTP layer
// by length/headers rather than by decoding it.
const (
	testP256dh = "BNcRdreALRFXTkOOUHK1EtK2wtaz5Ry4YfYCA_0QTpQtUbVlUls0VJXg7A8u-Ts1XbjhazAkj7I99e8QcYP7DkM"
	testAuth   = "tBHItJI5svbpez7KI4CCXg"
)

func newVAPID(t *testing.T) (pub, priv string) {
	t.Helper()
	priv, pub, err := webpushgo.GenerateVAPIDKeys()
	require.NoError(t, err)
	return pub, priv
}

func TestDeliverPostsEncryptedPayload(t *testing.T) {
	var (
		mu       sync.Mutex
		gotBody  []byte
		gotAuth  string
		gotTTL   string
		gotCalls int
	)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body := make([]byte, r.ContentLength)
		_, _ = r.Body.Read(body)
		mu.Lock()
		gotBody = body
		gotAuth = r.Header.Get("Authorization")
		gotTTL = r.Header.Get("TTL")
		gotCalls++
		mu.Unlock()
		w.WriteHeader(http.StatusCreated)
	}))
	defer srv.Close()

	store := newMemStore()
	store.add("user-1", webpush.Subscription{Endpoint: srv.URL, P256dh: testP256dh, Auth: testAuth})

	pub, priv := newVAPID(t)
	ch := webpush.New(pub, priv, "mailto:ops@example.com", store, webpush.WithTTL(90))

	err := ch.Deliver(context.Background(), "user-1", []byte(`{"hello":"world"}`))
	require.NoError(t, err)

	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, 1, gotCalls)
	require.NotEmpty(t, gotBody, "encrypted payload must reach the push endpoint")
	require.Contains(t, gotAuth, "vapid", "VAPID authorization header must be present")
	require.Equal(t, "90", gotTTL, "configured TTL must be forwarded")
	require.Equal(t, 1, store.count("user-1"), "healthy subscription must be retained")
}

func TestDeliverRemovesGoneSubscription(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusGone)
	}))
	defer srv.Close()

	store := newMemStore()
	store.add("user-2", webpush.Subscription{Endpoint: srv.URL, P256dh: testP256dh, Auth: testAuth})

	pub, priv := newVAPID(t)
	ch := webpush.New(pub, priv, "mailto:ops@example.com", store)

	// A 410 is a self-healing condition, not a delivery error.
	err := ch.Deliver(context.Background(), "user-2", []byte("payload"))
	require.NoError(t, err)
	require.Equal(t, 0, store.count("user-2"), "410 subscription must be soft-removed")
}

func TestDeliverRemovesNotFoundSubscription(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer srv.Close()

	store := newMemStore()
	store.add("user-3", webpush.Subscription{Endpoint: srv.URL, P256dh: testP256dh, Auth: testAuth})

	pub, priv := newVAPID(t)
	ch := webpush.New(pub, priv, "mailto:ops@example.com", store)

	err := ch.Deliver(context.Background(), "user-3", []byte("payload"))
	require.NoError(t, err)
	require.Equal(t, 0, store.count("user-3"), "404 subscription must be soft-removed")
}

func TestDeliverServerErrorSurfacesAndRetains(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	store := newMemStore()
	store.add("user-4", webpush.Subscription{Endpoint: srv.URL, P256dh: testP256dh, Auth: testAuth})

	pub, priv := newVAPID(t)
	ch := webpush.New(pub, priv, "mailto:ops@example.com", store)

	err := ch.Deliver(context.Background(), "user-4", []byte("payload"))
	require.Error(t, err, "5xx must surface as a delivery error")
	require.Equal(t, 1, store.count("user-4"), "5xx must not remove the subscription")
}

func TestDeliverFanOutIsolatesFailures(t *testing.T) {
	var okCalls int
	var mu sync.Mutex
	okSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body := make([]byte, r.ContentLength)
		_, _ = r.Body.Read(body)
		mu.Lock()
		okCalls++
		mu.Unlock()
		w.WriteHeader(http.StatusCreated)
	}))
	defer okSrv.Close()
	goneSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusGone)
	}))
	defer goneSrv.Close()

	store := newMemStore()
	store.add("team", webpush.Subscription{Endpoint: okSrv.URL, P256dh: testP256dh, Auth: testAuth})
	store.add("team", webpush.Subscription{Endpoint: goneSrv.URL, P256dh: testP256dh, Auth: testAuth})

	pub, priv := newVAPID(t)
	ch := webpush.New(pub, priv, "mailto:ops@example.com", store)

	err := ch.Deliver(context.Background(), "team", []byte("broadcast"))
	require.NoError(t, err, "a gone endpoint alongside a healthy one is not an error")

	mu.Lock()
	require.Equal(t, 1, okCalls, "healthy endpoint must still receive delivery")
	mu.Unlock()
	require.Equal(t, 1, store.count("team"), "only the gone endpoint must be removed")
}

func TestDeliverEmptyGroupIsNoOp(t *testing.T) {
	store := newMemStore()
	pub, priv := newVAPID(t)
	ch := webpush.New(pub, priv, "mailto:ops@example.com", store)

	err := ch.Deliver(context.Background(), "nobody", []byte("payload"))
	require.NoError(t, err)
}
