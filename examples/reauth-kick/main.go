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

// Command reauth-kick is a minimal, runnable version of the "authorization can be revoked
// out from under a live socket" pattern: a WebSocket handshake only proves who the caller
// was at connect time, not who they still are five minutes into a long-lived session. Two
// independent gateway mechanisms answer that:
//
//   - WithWSReauth periodically re-runs the handshake's auth check against the retained
//     upgrade request. When a user's access is revoked, every one of their open connections
//     fails its next reauth tick on its own schedule and self-closes with WebSocket status
//     1008 (policy violation) and the fixed reason "reauthentication failed". This is the
//     passive path: the server does not have to know which sockets exist, only that the
//     permission check now fails.
//   - Registry.Disconnect / Registry.DisconnectGroup force-close a specific connection or
//     every connection of an identity group immediately, with an admin-supplied reason, and
//     without touching whatever permission state WithWSReauth consults. This is the active
//     path: "get this session off my server right now" for a reason that says nothing about
//     whether the user is still authorized (abuse, a support request, a forced re-login).
//
// The example wires both to one shared in-memory permission store so the difference between
// them is visible from a browser: revoking a user's access waits out the reauth interval,
// while an admin kick is immediate. See README.md for the full walkthrough.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/tochemey/goakt/v4/actor"
	golog "github.com/tochemey/goakt/v4/log"

	gateway "github.com/StringKe/goakt-gateway"
)

// reauthInterval is deliberately short so a revoke is visible within a few seconds during
// the demo. A production deployment would use something on the order of 30s-5m: reauth is a
// polling check, and its cost (one WSAuthFunc call per connection per tick) scales with both
// connection count and how aggressively this is set.
const reauthInterval = 3 * time.Second

// userGroup derives the identity group every connection of user shares. All of a user's
// tabs/devices register under the same group, which is what makes DisconnectGroup a "kick
// this person everywhere" call instead of a "kick this one socket" call.
func userGroup(user string) string {
	return "user:" + user
}

func main() {
	ctx := context.Background()

	// WithPubSub is required: every connection here registers with a non-empty Group (so
	// DisconnectGroup has something to enumerate), and the group's local membership bridge
	// rides on the actor system's topic actor.
	system, err := actor.NewActorSystem("gateway-reauth-kick", actor.WithLogger(golog.DiscardLogger), actor.WithPubSub())
	if err != nil {
		log.Fatal(err)
	}
	if err := system.Start(ctx); err != nil {
		log.Fatal(err)
	}
	defer func() { _ = system.Stop(ctx) }()

	registry := gateway.NewRegistry(system, golog.DiscardLogger)
	defer func() { _ = registry.Close(ctx) }()

	perms := newPermissionStore()
	authFunc := authenticateUser(perms)

	mux := http.NewServeMux()
	mux.HandleFunc("/", serveIndex)
	mux.Handle("/ws", gateway.NewWSHandler(registry,
		gateway.WithWSAuth(authFunc),
		// The reauth loop re-runs authFunc against the retained upgrade request on every
		// tick. It only cares about the error: a role change or a revoked token cannot be
		// applied to a live socket's group/topics without stranding in-flight deliveries, so
		// the connection is closed instead and the client has to reconnect to pick up its
		// new (or restored) permissions.
		gateway.WithWSReauth(reauthInterval, authFunc),
		gateway.WithWSOnConnect(func(ctx context.Context, info *gateway.ConnInfo, _ *http.Request) {
			log.Printf("connect: conn=%q group=%q", info.ID, info.Group)
			// Push the connection its own id so the demo page can target admin/kick at one
			// specific device instead of the whole group.
			welcome, _ := json.Marshal(wsMessage{Type: "welcome", ID: info.ID, Group: info.Group})
			if err := registry.SendToConnection(ctx, info.ID, welcome); err != nil {
				log.Printf("failed to send welcome to %q: %v", info.ID, err)
			}
		}),
		gateway.WithWSOnDisconnect(func(info *gateway.ConnInfo) {
			log.Printf("disconnect: conn=%q group=%q", info.ID, info.Group)
		}),
	))
	mux.HandleFunc("/admin/revoke", adminRevokeHandler(perms))
	mux.HandleFunc("/admin/grant", adminGrantHandler(perms))
	mux.HandleFunc("/admin/kick", adminKickHandler(registry))
	mux.HandleFunc("/admin/kick-group", adminKickGroupHandler(registry))

	server, err := gateway.NewServer("127.0.0.1:8080", mux)
	if err != nil {
		log.Fatal(err)
	}

	log.Println("gateway-reauth-kick listening on http://127.0.0.1:8080")
	log.Fatal(server.ListenAndServe(ctx))
}

// permissionStore is a deliberately trivial stand-in for whatever authorization state a real
// application checks during auth (a roles table, a token allowlist, a subscription status).
// A user is allowed unless explicitly revoked; nothing here is persisted or shared across
// processes, which is fine for a single-binary demo and would not be for a real deployment.
type permissionStore struct {
	mu     sync.Mutex
	denied map[string]bool
}

func newPermissionStore() *permissionStore {
	return &permissionStore{denied: make(map[string]bool)}
}

func (s *permissionStore) allowed(user string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return !s.denied[user]
}

func (s *permissionStore) revoke(user string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.denied[user] = true
}

func (s *permissionStore) grant(user string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.denied, user)
}

// authenticateUser builds the WSAuthFunc shared by the handshake and by WithWSReauth. Using
// the same function for both is the point: a reauth check is only meaningful if it applies
// the exact same rule the handshake did, not a looser approximation of it.
func authenticateUser(perms *permissionStore) gateway.WSAuthFunc {
	return func(r *http.Request) (*gateway.ConnInfo, error) {
		user := r.URL.Query().Get("user")
		if user == "" {
			return nil, gateway.ErrUnauthorized
		}
		if !perms.allowed(user) {
			return nil, gateway.ErrUnauthorized
		}
		return &gateway.ConnInfo{
			ID:    user + "-" + uuid.NewString(),
			Group: userGroup(user),
		}, nil
	}
}

// wsMessage is the one message shape this demo ever pushes to a client: a welcome frame
// carrying the connection id an admin action can target.
type wsMessage struct {
	Type  string `json:"type"`
	ID    string `json:"id"`
	Group string `json:"group"`
}

// adminRevokeHandler simulates a permission change (a role downgrade, a ban, an expired
// subscription): it does not touch any open socket. Every connection of user finds out on
// its own next reauth tick, up to reauthInterval later.
func adminRevokeHandler(perms *permissionStore) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		user := r.URL.Query().Get("user")
		if user == "" {
			http.Error(w, "user query parameter is required", http.StatusBadRequest)
			return
		}
		perms.revoke(user)
		fmt.Fprintf(w, "revoked access for %q; open connections will be closed within %s\n", user, reauthInterval)
	}
}

// adminGrantHandler restores a user's access. It does not reopen any connection the reauth
// loop already closed - the client has to reconnect, the same as after any other close.
func adminGrantHandler(perms *permissionStore) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		user := r.URL.Query().Get("user")
		if user == "" {
			http.Error(w, "user query parameter is required", http.StatusBadRequest)
			return
		}
		perms.grant(user)
		fmt.Fprintf(w, "granted access for %q\n", user)
	}
}

// adminKickHandler force-closes one specific connection by id via Registry.Disconnect,
// immediately and regardless of the reauth schedule. Unlike adminRevokeHandler it does not
// change any permission: the user (and their other devices, if any) stays authorized and can
// reconnect this same socket right away.
func adminKickHandler(registry *gateway.Registry) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id := r.URL.Query().Get("id")
		if id == "" {
			http.Error(w, "id query parameter is required", http.StatusBadRequest)
			return
		}
		reason := r.URL.Query().Get("reason")
		if reason == "" {
			reason = "kicked by admin"
		}
		if err := registry.Disconnect(r.Context(), id, reason); err != nil {
			http.Error(w, err.Error(), http.StatusNotFound)
			return
		}
		fmt.Fprintf(w, "disconnected %q: %s\n", id, reason)
	}
}

// adminKickGroupHandler force-closes every connection of a user's identity group via
// Registry.DisconnectGroup, immediately. It is the multi-device counterpart to
// adminKickHandler: every open tab/device of user is closed in one call, still without
// touching the permission store.
func adminKickGroupHandler(registry *gateway.Registry) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		user := r.URL.Query().Get("user")
		if user == "" {
			http.Error(w, "user query parameter is required", http.StatusBadRequest)
			return
		}
		reason := r.URL.Query().Get("reason")
		if reason == "" {
			reason = "kicked by admin"
		}
		n, err := registry.DisconnectGroup(r.Context(), userGroup(user), reason)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		fmt.Fprintf(w, "disconnected %d connection(s) of %q: %s\n", n, user, reason)
	}
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
<title>gateway-reauth-kick demo</title>
<style>
  body { font-family: system-ui, sans-serif; max-width: 720px; margin: 2rem auto; padding: 0 1rem; }
  fieldset { margin-bottom: 1.5rem; }
  input { width: 10rem; }
  #log { background: #111; color: #0f0; padding: 0.75rem; height: 14rem; overflow-y: auto; font-family: monospace; white-space: pre-wrap; }
</style>
</head>
<body>
<h1>gateway-reauth-kick demo</h1>
<p>Open this page in one or more tabs, connect as the same or different users, then use the
admin actions below to see the two ways a session ends involuntarily: a permission revoke
that closes within ` + "`reauthInterval`" + ` (3s here), and an immediate admin kick.</p>

<fieldset>
  <legend>1. Connect as a user</legend>
  <label>user <input id="user" value="alice"></label>
  <button id="connect">Connect</button>
  <span id="status">disconnected</span>
  <div>connection id: <code id="connId">(none yet)</code></div>
</fieldset>

<fieldset>
  <legend>2a. Revoke access (passive: closes on the next reauth tick, up to 3s later)</legend>
  <label>user <input id="revokeUser" value="alice"></label>
  <button id="revoke">Revoke</button>
  <button id="grant">Grant back</button>
</fieldset>

<fieldset>
  <legend>2b. Kick immediately (active: closes right now, permissions untouched)</legend>
  <label>connection id <input id="kickId" placeholder="paste from above"></label>
  <button id="kick">Kick this connection</button>
</fieldset>

<fieldset>
  <legend>2c. Kick a user's every device immediately</legend>
  <label>user <input id="kickGroupUser" value="alice"></label>
  <button id="kickGroup">Kick all devices</button>
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
  ws.onmessage = (evt) => {
    logLine('[message] ' + evt.data);
    try {
      const msg = JSON.parse(evt.data);
      if (msg.type === 'welcome') {
        document.getElementById('connId').textContent = msg.id;
        document.getElementById('kickId').value = msg.id;
      }
    } catch (e) {}
  };
  ws.onclose = (evt) => {
    document.getElementById('status').textContent = 'disconnected';
    logLine('[close] code=' + evt.code + ' reason=' + JSON.stringify(evt.reason));
  };
  ws.onerror = () => logLine('[error]');
};

async function post(path) {
  const res = await fetch(path, { method: 'POST' });
  logLine('[' + path + '] ' + await res.text());
}

document.getElementById('revoke').onclick = () => {
  post('/admin/revoke?user=' + encodeURIComponent(document.getElementById('revokeUser').value));
};
document.getElementById('grant').onclick = () => {
  post('/admin/grant?user=' + encodeURIComponent(document.getElementById('revokeUser').value));
};
document.getElementById('kick').onclick = () => {
  post('/admin/kick?id=' + encodeURIComponent(document.getElementById('kickId').value) + '&reason=' + encodeURIComponent('kicked from the demo page'));
};
document.getElementById('kickGroup').onclick = () => {
  post('/admin/kick-group?user=' + encodeURIComponent(document.getElementById('kickGroupUser').value) + '&reason=' + encodeURIComponent('kicked all devices from the demo page'));
};
</script>
</body>
</html>`
