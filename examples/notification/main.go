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

// Command notification is a minimal, runnable version of the "multi-device group push
// with offline fallback" pattern every notification-style consumer of gateway needs:
//
//   - The same identity (a user) can hold several live connections at once - one per
//     browser tab or device - all sharing one ConnInfo.Group ("user:<id>").
//   - A single HTTP call fans a message out to every one of that identity's connections
//     with Registry.SendToGroup, without the caller enumerating connection ids.
//   - DeliveryResult.None() is the signal that the delivery reached nothing this node could
//     account for, which is the moment an application falls back to an offline channel (real
//     Web Push, email, ...). This sample stands in a fakeWebPush logger for that channel. How
//     exact that signal is depends on configuration - see DeliveryResult.None; in short exact
//     reachability needs WithDeliveryConfirmation, and even then the fallback is
//     at-least-once.
//   - Registry.IsOnline is a presence query, not a local-table lookup: with a Presence
//     backend configured it answers for the whole cluster, not just this process.
//
// What this sample deliberately does not demonstrate: an actual multi-node GoAkt
// cluster. It runs one actor system, so Registry.SendToGroup's local fan-out path (direct
// writes to the sockets this process holds) is the only path that ever delivers a socket
// write here. Swapping MemoryPresence for the Redis-backed presence/redis package is still
// meaningful in single-node mode - it is the same Presence interface a real cluster
// deployment would configure - but proving it changes the online verdict across processes
// needs a real cluster, which is out of scope for this sample. See README.md.
package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/google/uuid"
	goredis "github.com/redis/go-redis/v9"

	"github.com/tochemey/goakt/v4/actor"
	golog "github.com/tochemey/goakt/v4/log"

	gateway "github.com/StringKe/goakt-gateway"
	redispresence "github.com/StringKe/goakt-gateway/presence/redis"
)

// userGroup derives the identity group every connection of user shares. All of a user's
// tabs/devices register under the same group, which is what makes SendToGroup a
// "notify this person" call instead of a "notify this socket" call.
func userGroup(user string) string {
	return "user:" + user
}

func main() {
	ctx := context.Background()

	// WithPubSub is required: SendToGroup's cross-node fan-out and the group's local
	// membership bridge both ride on the actor system's topic actor.
	system, err := actor.NewActorSystem("gateway-notification", actor.WithLogger(golog.DiscardLogger), actor.WithPubSub())
	if err != nil {
		log.Fatal(err)
	}
	if err := system.Start(ctx); err != nil {
		log.Fatal(err)
	}
	defer func() { _ = system.Stop(ctx) }()

	presence := buildPresence(ctx)
	registry := gateway.NewRegistry(system, golog.DiscardLogger, gateway.WithPresence(presence))
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
	mux.HandleFunc("/notify", notifyHandler(registry))
	mux.HandleFunc("/online", onlineHandler(registry))

	server, err := gateway.NewServer("127.0.0.1:8080", mux)
	if err != nil {
		log.Fatal(err)
	}

	log.Println("gateway-notification listening on http://127.0.0.1:8080")
	log.Fatal(server.ListenAndServe(ctx))
}

// buildPresence picks the Presence backend from REDIS_ADDR: unset means a single-node
// deployment, where the in-process MemoryPresence is both sufficient and correct (there is
// no other node whose view it would need to share). A set REDIS_ADDR opts into the
// cluster-shared backend so the same binary run on several nodes agrees on who is online.
func buildPresence(ctx context.Context) gateway.Presence {
	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		log.Println("REDIS_ADDR not set: using in-process MemoryPresence (online status only covers this process)")
		return gateway.NewMemoryPresence()
	}

	client := goredis.NewClient(&goredis.Options{Addr: addr})
	pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := client.Ping(pingCtx).Err(); err != nil {
		log.Fatalf("REDIS_ADDR=%q set but unreachable: %v", addr, err)
	}
	log.Printf("using Redis presence backend at %s", addr)
	return redispresence.NewPresence(client)
}

// authenticateFromQuery stands in for real session/token authentication: it reads the
// user id from the query string and derives the connection's identity from it. Each
// connection gets its own random ID (so the same user can open any number of tabs/devices
// without a takeover evicting the previous one) and shares its Group with every other
// connection of that user.
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

// notifyResponse is what /notify reports back: the raw DeliveryResult plus whether the
// offline fallback fired, since None() alone does not say what the caller did about it.
type notifyResponse struct {
	Delivered       int  `json:"delivered"`
	Dropped         int  `json:"dropped"`
	Remote          int  `json:"remote"`
	OfflineFallback bool `json:"offlineFallback"`
}

// notifyHandler fans msg out to every connection of user and falls back to
// fakeWebPush when DeliveryResult.None reports nothing took it - the exact decision the
// old Broadcast API (which always returned nil) could never make for its caller.
func notifyHandler(registry *gateway.Registry) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		user := r.URL.Query().Get("user")
		msg := r.URL.Query().Get("msg")
		if user == "" || msg == "" {
			http.Error(w, "user and msg query parameters are required", http.StatusBadRequest)
			return
		}

		result, err := registry.SendToGroup(r.Context(), userGroup(user), []byte(msg))
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		resp := notifyResponse{Delivered: result.Delivered, Dropped: result.Dropped, Remote: result.Remote}
		if result.None() {
			fakeWebPush(user, msg)
			resp.OfflineFallback = true
		}

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
	}
}

// onlineHandler exposes Registry.IsOnline directly so the presence-vs-local-table
// distinction described in README.md can be checked from a plain curl call.
func onlineHandler(registry *gateway.Registry) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		user := r.URL.Query().Get("user")
		if user == "" {
			http.Error(w, "user query parameter is required", http.StatusBadRequest)
			return
		}

		online, err := registry.IsOnline(r.Context(), userGroup(user))
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]bool{"online": online})
	}
}

// fakeWebPush stands in for a real Web Push send (VAPID + push service HTTP call). It only
// runs when DeliveryResult.None() proved no live socket exists anywhere in the cluster, so
// logging it is enough to see the fallback trigger in the demo.
func fakeWebPush(user, msg string) {
	log.Printf("[fakeWebPush] user %q is offline cluster-wide, would push: %q", user, msg)
}

// serveIndex serves a small self-contained page for driving the demo from a browser: open
// it in two tabs (or two browsers) with different "user" values to see per-user isolation,
// or the same value to see multi-device fan-out to one group.
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
<title>gateway-notification demo</title>
<style>
  body { font-family: system-ui, sans-serif; max-width: 640px; margin: 2rem auto; padding: 0 1rem; }
  fieldset { margin-bottom: 1.5rem; }
  input { width: 10rem; }
  #log { background: #111; color: #0f0; padding: 0.75rem; height: 12rem; overflow-y: auto; font-family: monospace; white-space: pre-wrap; }
</style>
</head>
<body>
<h1>gateway-notification demo</h1>

<fieldset>
  <legend>1. Connect as a user (open this page again in another tab to simulate a second device)</legend>
  <label>user id <input id="user" value="alice"></label>
  <button id="connect">Connect</button>
  <span id="status">disconnected</span>
</fieldset>

<fieldset>
  <legend>2. Push to a user's group (fans out to every device connected as that user)</legend>
  <label>target user <input id="targetUser" value="alice"></label>
  <label>message <input id="msg" value="hello"></label>
  <button id="notify">Notify</button>
</fieldset>

<fieldset>
  <legend>3. Check cluster-wide online status</legend>
  <label>user <input id="onlineUser" value="alice"></label>
  <button id="checkOnline">Check</button>
</fieldset>

<div id="log"></div>

<script>
let ws = null;
const logEl = document.getElementById('log');
function logLine(text) {
  logEl.textContent += text + "\n";
  logEl.scrollTop = logEl.scrollHeight;
}

document.getElementById('connect').onclick = () => {
  if (ws) { ws.close(); }
  const user = document.getElementById('user').value;
  const proto = location.protocol === 'https:' ? 'wss:' : 'ws:';
  ws = new WebSocket(proto + '//' + location.host + '/ws?user=' + encodeURIComponent(user));
  ws.onopen = () => { document.getElementById('status').textContent = 'connected as ' + user; logLine('[open] ' + user); };
  ws.onmessage = (evt) => logLine('[message] ' + evt.data);
  ws.onclose = () => { document.getElementById('status').textContent = 'disconnected'; logLine('[close]'); };
  ws.onerror = () => logLine('[error]');
};

document.getElementById('notify').onclick = async () => {
  const user = document.getElementById('targetUser').value;
  const msg = document.getElementById('msg').value;
  const res = await fetch('/notify?user=' + encodeURIComponent(user) + '&msg=' + encodeURIComponent(msg));
  logLine('[notify result] ' + await res.text());
};

document.getElementById('checkOnline').onclick = async () => {
  const user = document.getElementById('onlineUser').value;
  const res = await fetch('/online?user=' + encodeURIComponent(user));
  logLine('[online] ' + await res.text());
};
</script>
</body>
</html>`
