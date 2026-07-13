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

// Command offline-push demonstrates Registry.WithOfflineChannel: the library-native
// replacement for the old pattern where every caller of SendToGroup had to inspect
// DeliveryResult.None itself and remember to invoke its own offline transport (see
// examples/notification, which still shows that manual style for comparison).
//
// With WithOfflineChannel configured, the Registry makes that decision internally:
//
//   - A group with a live WebSocket connection is delivered to directly; SendToGroup
//     returns a DeliveryResult with Delivered > 0 and the offline channel is never touched.
//   - A group with no reachable socket anywhere in the cluster (DeliveryResult.None) is
//     hallowed off, on the Registry's own goroutine, to the configured OfflineChannel - here
//     a real github.com/StringKe/goakt-gateway/offline/webpush.Channel - without the /notify
//     handler below ever branching on the result.
//
// The push "service" the demo talks to is an in-process httptest.Server standing in for a
// real provider (FCM, Mozilla autopush, ...). It never decrypts the payload - a real push
// service cannot, only the browser can with the subscription's private key - it only proves
// that a correctly VAPID-signed, RFC 8291-encrypted POST arrived, which is everything an
// application-level integration test can observe without a browser in the loop. The
// "browser" side is stood in by generateDemoSubscriptionKeys, which mints a fresh P-256 key
// pair the same shape a real PushSubscription.getKey() would hand the application server.
//
// See README.md for what problem this closes and how to drive the demo end to end.
package main

import (
	"context"
	"crypto/elliptic"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"sync"
	"time"

	"github.com/google/uuid"

	webpushgo "github.com/SherClockHolmes/webpush-go"
	"github.com/tochemey/goakt/v4/actor"
	golog "github.com/tochemey/goakt/v4/log"

	gateway "github.com/StringKe/goakt-gateway"
	"github.com/StringKe/goakt-gateway/offline/webpush"
)

// userGroup derives the identity group a user's connections and push subscriptions
// share. It is the same string Registry.SendToGroup fans out to and webpush.Channel
// looks subscriptions up by, so a user online on any device and a user with a push
// subscription are addressed identically.
func userGroup(user string) string {
	return "user:" + user
}

func main() {
	ctx := context.Background()

	// WithPubSub is required: SendToGroup's cross-node fan-out and the group's local
	// membership bridge both ride on the actor system's topic actor, even though this
	// demo runs a single node.
	system, err := actor.NewActorSystem("gateway-offline-push", actor.WithLogger(golog.DiscardLogger), actor.WithPubSub())
	if err != nil {
		log.Fatal(err)
	}
	if err := system.Start(ctx); err != nil {
		log.Fatal(err)
	}
	defer func() { _ = system.Stop(ctx) }()

	// pushService stands in for a real Web Push provider: it accepts the encrypted POST
	// Channel.Deliver sends, records enough to prove it was genuine (byte length, VAPID
	// Authorization header, TTL header), and answers 201 Created. It cannot and does not
	// decrypt the body - only the "browser" holding the subscription's private key could.
	pushService := newFakePushService()
	defer pushService.Close()

	store := newSubscriptionStore()

	vapidPrivate, vapidPublic, err := webpushgo.GenerateVAPIDKeys()
	if err != nil {
		log.Fatalf("generate VAPID keys: %v", err)
	}
	offlineChannel := webpush.New(vapidPublic, vapidPrivate, "mailto:demo@example.com", store)

	obs := newFallbackObserver()

	registry := gateway.NewRegistry(system, golog.DiscardLogger,
		gateway.WithPresence(gateway.NewMemoryPresence()),
		gateway.WithObserver(obs),
		gateway.WithOfflineChannel(offlineChannel),
	)
	defer func() { _ = registry.Close(ctx) }()

	mux := http.NewServeMux()
	mux.HandleFunc("/", serveIndex)
	mux.Handle("/ws", gateway.NewWSHandler(registry,
		gateway.WithWSAuth(authenticateFromQuery),
		gateway.WithWSOnConnect(func(_ context.Context, info *gateway.ConnInfo, _ *http.Request) {
			log.Printf("connect: conn=%q group=%q", info.ID, info.Group)
		}),
		gateway.WithWSOnDisconnect(func(info *gateway.ConnInfo) {
			log.Printf("disconnect: conn=%q group=%q", info.ID, info.Group)
		}),
	))
	mux.HandleFunc("/subscribe", subscribeHandler(store, pushService.URL))
	mux.HandleFunc("/notify", notifyHandler(registry))
	mux.HandleFunc("/status", statusHandler(obs))

	server, err := gateway.NewServer("127.0.0.1:8085", mux)
	if err != nil {
		log.Fatal(err)
	}

	log.Println("gateway-offline-push listening on http://127.0.0.1:8085")
	log.Printf("fake push service listening on %s (never receives cleartext payloads)", pushService.URL)
	log.Fatal(server.ListenAndServe(ctx))
}

// authenticateFromQuery stands in for real session/token authentication: it reads the
// user id from the query string and derives the connection's identity from it. Each
// connection gets its own random ID so the same user can open several tabs without a
// takeover evicting the previous one, and shares its Group with every other connection
// of that user - the same group SendToGroup and the offline channel address.
func authenticateFromQuery(r *http.Request) (*gateway.ConnInfo, error) {
	user := r.URL.Query().Get("user")
	if user == "" {
		return nil, gateway.ErrUnauthorized
	}
	return &gateway.ConnInfo{
		ID:    user + "-" + uuid.NewString(),
		Group: userGroup(user),
	}, nil
}

// notifyResponse is what /notify reports back. Unlike examples/notification's
// notifyResponse, there is no OfflineFallback field here: with WithOfflineChannel
// configured, SendToGroup itself decides and fires the fallback, and does so
// asynchronously (see offline.go's maybeOfflineFallback), so the HTTP handler that
// called SendToGroup has nothing to report about it beyond the None verdict that
// triggered it. GET /status shows the fallback's actual outcome once it lands.
type notifyResponse struct {
	Delivered int  `json:"delivered"`
	Dropped   int  `json:"dropped"`
	Remote    int  `json:"remote"`
	None      bool `json:"none"`
}

// notifyHandler fans msg out to every connection of user via Registry.SendToGroup.
// It never inspects the result to decide whether to push - that decision now belongs
// to the Registry's configured OfflineChannel - it only reports the delivery counters
// back to the caller for display.
func notifyHandler(registry *gateway.Registry) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "POST required", http.StatusMethodNotAllowed)
			return
		}
		var req struct{ User, Msg string }
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.User == "" || req.Msg == "" {
			http.Error(w, "user and msg fields are required", http.StatusBadRequest)
			return
		}

		result, err := registry.SendToGroup(r.Context(), userGroup(req.User), []byte(req.Msg))
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(notifyResponse{
			Delivered: result.Delivered,
			Dropped:   result.Dropped,
			Remote:    result.Remote,
			None:      result.None(),
		})
	}
}

// subscribeHandler simulates a browser that has just completed
// PushManager.subscribe(): it mints a fresh demo subscription (a real one would arrive
// from the browser's PushSubscription object) and registers it with the store under
// the user's group, exactly as an application's real "/api/push-subscribe" endpoint
// would persist a browser-supplied subscription.
func subscribeHandler(store *subscriptionStore, pushServiceURL string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "POST required", http.StatusMethodNotAllowed)
			return
		}
		var req struct{ User string }
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.User == "" {
			http.Error(w, "user field is required", http.StatusBadRequest)
			return
		}

		p256dh, auth, err := generateDemoSubscriptionKeys()
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		sub := webpush.Subscription{
			// The subscription id in the path lets the fake push service's log line name
			// which demo user a delivery was for; a real push service endpoint carries no
			// such thing; it is purely for this demo's observability.
			Endpoint: pushServiceURL + "/push/" + req.User + "/" + uuid.NewString(),
			P256dh:   p256dh,
			Auth:     auth,
		}
		store.add(userGroup(req.User), sub)
		log.Printf("subscribe: user=%q now has %d push subscription(s)", req.User, store.count(userGroup(req.User)))

		w.WriteHeader(http.StatusCreated)
	}
}

// statusHandler exposes the fallbackObserver's recent OfflineFallback events so the
// demo page can prove, from plain polling, that a push actually fired - the
// asynchronous counterpart to notifyHandler's immediate DeliveryResult.
func statusHandler(obs *fallbackObserver) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(obs.snapshot())
	}
}

// subscriptionStore is an in-memory webpush.SubscriptionStore keyed by identity group,
// standing in for the durable table a real application would keep push subscriptions
// in (see webpush.SubscriptionStore's godoc for the contract this satisfies).
type subscriptionStore struct {
	mu   sync.Mutex
	subs map[string][]webpush.Subscription
}

func newSubscriptionStore() *subscriptionStore {
	return &subscriptionStore{subs: make(map[string][]webpush.Subscription)}
}

func (s *subscriptionStore) add(group string, sub webpush.Subscription) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.subs[group] = append(s.subs[group], sub)
}

func (s *subscriptionStore) count(group string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.subs[group])
}

// Get satisfies webpush.SubscriptionStore.
func (s *subscriptionStore) Get(_ context.Context, group string) ([]webpush.Subscription, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]webpush.Subscription, len(s.subs[group]))
	copy(out, s.subs[group])
	return out, nil
}

// Remove satisfies webpush.SubscriptionStore: the Channel calls it when the push
// service answers 404/410 for a subscription, which the fake push service in this demo
// never does, but a real one would once a browser revokes permission.
func (s *subscriptionStore) Remove(_ context.Context, group, endpoint string) error {
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

// fallbackObserver is a gateway.Observer that also implements gateway.OfflineObserver,
// the optional extension Registry.SendToGroup reports OfflineChannel outcomes through.
// The six core hooks are no-ops here; this demo only cares about the offline fallback
// one, which it both logs and buffers for /status to serve.
type fallbackObserver struct {
	mu     sync.Mutex
	events []fallbackEvent
}

// fallbackEvent is a single OfflineFallback report, JSON-encoded for /status.
type fallbackEvent struct {
	Group   string    `json:"group"`
	Success bool      `json:"success"`
	Error   string    `json:"error,omitempty"`
	At      time.Time `json:"at"`
}

func newFallbackObserver() *fallbackObserver {
	return &fallbackObserver{}
}

var (
	_ gateway.Observer        = (*fallbackObserver)(nil)
	_ gateway.OfflineObserver = (*fallbackObserver)(nil)
)

func (o *fallbackObserver) ConnectionRegistered(string, string)   {}
func (o *fallbackObserver) ConnectionUnregistered(string, string) {}
func (o *fallbackObserver) ConnectionReplaced(string, string)     {}
func (o *fallbackObserver) DeliveryDropped(string, string)        {}
func (o *fallbackObserver) DeliveryFailed(string, error)          {}
func (o *fallbackObserver) BroadcastFanout(string, int)           {}

// OfflineFallback is the hook Registry.SendToGroup calls after routing a group with no
// reachable socket to the configured OfflineChannel - once per attempt, with err nil on
// success. This is the only place in the demo that observes the fallback actually
// happening, since notifyHandler's HTTP response returns before it completes.
func (o *fallbackObserver) OfflineFallback(group string, err error) {
	ev := fallbackEvent{Group: group, Success: err == nil, At: time.Now()}
	if err != nil {
		ev.Error = err.Error()
	}
	log.Printf("[offline-fallback] group=%q success=%v err=%v", group, err == nil, err)

	o.mu.Lock()
	defer o.mu.Unlock()
	o.events = append(o.events, ev)
	if len(o.events) > 20 {
		o.events = o.events[len(o.events)-20:]
	}
}

func (o *fallbackObserver) snapshot() []fallbackEvent {
	o.mu.Lock()
	defer o.mu.Unlock()
	out := make([]fallbackEvent, len(o.events))
	copy(out, o.events)
	return out
}

// newFakePushService starts an httptest.Server standing in for a real Web Push
// provider (FCM, Mozilla autopush, ...). It cannot decrypt the RFC 8291 payload
// webpush.Channel sends - only the "browser" holding the subscription's private key
// could - so it only proves the POST was genuine: a non-empty encrypted body and a
// VAPID Authorization header, both logged, before answering 201 Created.
func newFakePushService() *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		log.Printf("[push-service] received encrypted push for %s: %d bytes, ttl=%s, auth=%v",
			r.URL.Path, len(body), r.Header.Get("TTL"), r.Header.Get("Authorization") != "")
		w.WriteHeader(http.StatusCreated)
	}))
}

// generateDemoSubscriptionKeys mints a fresh P-256 key pair and random authentication
// secret in exactly the shape a browser's PushSubscription.getKey('p256dh'/'auth')
// would hand an application server: an uncompressed EC point and a 16-byte secret,
// both base64url-encoded without padding. This demo has no browser to subscribe with,
// so subscribeHandler calls this in its place; a real application never generates
// these itself; it only ever receives and stores what the browser sent it.
func generateDemoSubscriptionKeys() (p256dh, auth string, err error) {
	curve := elliptic.P256()
	_, x, y, err := elliptic.GenerateKey(curve, rand.Reader)
	if err != nil {
		return "", "", fmt.Errorf("generate subscriber key pair: %w", err)
	}
	pub := elliptic.Marshal(curve, x, y)

	authSecret := make([]byte, 16)
	if _, err := rand.Read(authSecret); err != nil {
		return "", "", fmt.Errorf("generate auth secret: %w", err)
	}

	return base64.RawURLEncoding.EncodeToString(pub), base64.RawURLEncoding.EncodeToString(authSecret), nil
}

// serveIndex serves a small self-contained page for driving the demo from a browser.
func serveIndex(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = w.Write([]byte(indexHTML))
}

const indexHTML = `<!doctype html>
<html>
<head>
<meta charset="utf-8">
<title>gateway-offline-push demo</title>
<style>
  body { font-family: system-ui, sans-serif; max-width: 680px; margin: 2rem auto; padding: 0 1rem; }
  fieldset { margin-bottom: 1.5rem; }
  input { width: 10rem; }
  #log, #status { background: #111; color: #0f0; padding: 0.75rem; height: 10rem; overflow-y: auto; font-family: monospace; white-space: pre-wrap; font-size: 0.85rem; }
  .hint { color: #666; font-size: 0.85rem; }
</style>
</head>
<body>
<h1>gateway-offline-push demo</h1>
<p class="hint">Registry.WithOfflineChannel decides for you: online goes straight to the socket,
offline falls back to Web Push automatically. Nothing below calls the push channel directly.</p>

<fieldset>
  <legend>1. Connect as a user (optional - skip this step to see the offline path)</legend>
  <label>user id <input id="user" value="alice"></label>
  <button id="connect">Connect</button>
  <span id="status1">disconnected</span>
</fieldset>

<fieldset>
  <legend>2. Grant push permission (simulates the browser's PushManager.subscribe)</legend>
  <label>user id <input id="subUser" value="alice"></label>
  <button id="subscribe">Subscribe to push</button>
  <span id="status2"></span>
</fieldset>

<fieldset>
  <legend>3. Notify a user - online delivers to the socket, offline falls back to push</legend>
  <label>target user <input id="targetUser" value="alice"></label>
  <label>message <input id="msg" value="hello"></label>
  <button id="notify">Notify</button>
  <pre id="result"></pre>
</fieldset>

<fieldset>
  <legend>4. Offline fallback events (polled from /status every second)</legend>
  <div id="status"></div>
</fieldset>

<fieldset>
  <legend>WebSocket log</legend>
  <div id="log"></div>
</fieldset>

<script>
let ws = null;
const log = document.getElementById('log');
function append(el, line) {
  el.textContent += line + "\n";
  el.scrollTop = el.scrollHeight;
}

document.getElementById('connect').onclick = () => {
  const user = document.getElementById('user').value;
  if (ws) { ws.close(); ws = null; }
  ws = new WebSocket('ws://' + location.host + '/ws?user=' + encodeURIComponent(user));
  ws.onopen = () => { document.getElementById('status1').textContent = 'connected as ' + user; append(log, '[open] ' + user); };
  ws.onclose = () => { document.getElementById('status1').textContent = 'disconnected'; append(log, '[close]'); };
  ws.onmessage = (ev) => append(log, '[message] ' + ev.data);
  ws.onerror = (ev) => append(log, '[error] ' + ev);
};

document.getElementById('subscribe').onclick = async () => {
  const user = document.getElementById('subUser').value;
  const res = await fetch('/subscribe', { method: 'POST', body: JSON.stringify({ User: user }) });
  document.getElementById('status2').textContent = res.ok ? 'subscribed ' + user + ' to push' : 'failed: ' + res.status;
};

document.getElementById('notify').onclick = async () => {
  const user = document.getElementById('targetUser').value;
  const msg = document.getElementById('msg').value;
  const res = await fetch('/notify', { method: 'POST', body: JSON.stringify({ User: user, Msg: msg }) });
  const body = await res.json();
  document.getElementById('result').textContent = JSON.stringify(body, null, 2) +
    (body.none ? '\n\nnone=true: no live socket anywhere - the Registry is now, asynchronously,\nrouting this to the offline push channel. Watch section 4.' : '\n\ndelivered directly to a live socket; the offline channel was never touched.');
};

async function pollStatus() {
  try {
    const res = await fetch('/status');
    const events = await res.json();
    const el = document.getElementById('status');
    el.textContent = (events || []).map(e => '[' + e.at + '] group=' + e.group + ' success=' + e.success + (e.error ? ' error=' + e.error : '')).join('\n');
  } catch (e) {}
}
setInterval(pollStatus, 1000);
pollStatus();
</script>
</body>
</html>`
