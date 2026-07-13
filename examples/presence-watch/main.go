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

// Command presence-watch is a minimal, runnable version of the "friend came online"
// pattern: a watcher subscribes once to a group's membership changes with
// Registry.WatchPresence and receives a PresenceJoin/PresenceLeave event the instant a
// connection registers or unregisters for that group, instead of polling
// Registry.GroupMembers or Registry.IsOnline on a timer.
//
// Two things are demonstrated side by side:
//
//   - Registry.WatchPresence: a long-lived subscription to one identity group's
//     membership changes, exposed here over a plain SSE endpoint so a browser tab can
//     watch it live.
//   - Registry.GroupMembers: a point-in-time snapshot of who is online in a group right
//     now, including the per-connection metadata recorded at registration (Redis-backed
//     presence carries this cluster-wide; the in-process backend carries it for this
//     node only, which is everything there is on a single node).
//
// REDIS_ADDR selects the Presence backend, exactly as in examples/notification: unset
// uses gateway.MemoryPresence (single node only), set uses presence/redis.Presence.
// Unlike Registry.SendToGroup's cluster fan-out (which rides the GoAkt actor system and
// needs real cluster discovery to cross a process boundary - see examples/notification's
// README for that caveat), WatchPresence and GroupMembers against the Redis backend talk
// to Redis directly. Run two instances of this binary against the same REDIS_ADDR and a
// join/leave on one process is visible on the other's /watch stream without any actor
// cluster at all.
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

// userGroup derives the identity group every connection of user shares. WatchPresence
// and GroupMembers both operate on this group, not on individual connection ids.
func userGroup(user string) string {
	return "user:" + user
}

func main() {
	ctx := context.Background()

	// WithPubSub is required even though this example never calls Registry.SendToGroup:
	// Registry.Register sets up a group's local fan-out bridge for any grouped connection
	// (see finalizeRegistration in registry.go), and that bridge rides the actor system's
	// topic actor regardless of which delivery APIs the application ends up using.
	system, err := actor.NewActorSystem("gateway-presence-watch", actor.WithLogger(golog.DiscardLogger), actor.WithPubSub())
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
			log.Printf("connect: conn=%q group=%q meta=%v", info.ID, info.Group, info.Meta)
		}),
		gateway.WithWSOnDisconnect(func(info *gateway.ConnInfo) {
			log.Printf("disconnect: conn=%q group=%q", info.ID, info.Group)
		}),
	))
	mux.HandleFunc("/watch", watchHandler(registry))
	mux.HandleFunc("/members", membersHandler(registry))

	server, err := gateway.NewServer("127.0.0.1:8080", mux)
	if err != nil {
		log.Fatal(err)
	}

	log.Println("gateway-presence-watch listening on http://127.0.0.1:8080")
	log.Fatal(server.ListenAndServe(ctx))
}

// buildPresence picks the Presence backend from REDIS_ADDR, exactly as in
// examples/notification: unset means single-node (gateway.MemoryPresence), set means the
// cluster-shared presence/redis.Presence. Both implement gateway.PresenceWatcher and
// gateway.PresenceDirectory, so WatchPresence and GroupMembers work against either.
func buildPresence(ctx context.Context) gateway.Presence {
	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		log.Println("REDIS_ADDR not set: using in-process MemoryPresence (watch/members only cover this process)")
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

// authenticateFromQuery derives a connection's identity from the query string: user picks
// the group everyone watching that user's presence cares about, device is optional
// metadata carried through Register into Presence.JoinWithMeta so /members can show it.
// Each connection gets its own random id so the same user can open several tabs/devices
// without a takeover evicting the previous one.
func authenticateFromQuery(r *http.Request) (*gateway.ConnInfo, error) {
	user := r.URL.Query().Get("user")
	if user == "" {
		return nil, gateway.ErrUnauthorized
	}
	info := &gateway.ConnInfo{
		ID:    user + "-" + uuid.NewString(),
		Group: userGroup(user),
	}
	if device := r.URL.Query().Get("device"); device != "" {
		info.Meta = map[string]string{"device": device}
	}
	return info, nil
}

// wireEvent is the JSON form of a gateway.PresenceEvent sent down the /watch SSE stream.
// Kind is rendered as a string ("join"/"leave") rather than the int PresenceEventKind so
// the browser side needs no lookup table.
type wireEvent struct {
	Group  string `json:"group"`
	ConnID string `json:"connID"`
	Kind   string `json:"kind"`
}

func kindString(k gateway.PresenceEventKind) string {
	if k == gateway.PresenceJoin {
		return "join"
	}
	return "leave"
}

// watchHandler subscribes to Registry.WatchPresence for the requested user's group and
// streams every PresenceJoin/PresenceLeave as a Server-Sent Event. This is deliberately a
// plain http.HandlerFunc rather than gateway.SSEHandler: SSEHandler models a registered
// gateway connection with its own backpressure/history machinery, but this endpoint is
// just a thin window onto a Go channel for the demo UI, with no gateway identity of its
// own.
func watchHandler(registry *gateway.Registry) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		user := r.URL.Query().Get("user")
		if user == "" {
			http.Error(w, "user query parameter is required", http.StatusBadRequest)
			return
		}

		flusher, ok := w.(http.Flusher)
		if !ok {
			http.Error(w, "streaming unsupported", http.StatusInternalServerError)
			return
		}

		events, cancel, err := registry.WatchPresence(r.Context(), userGroup(user))
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		defer cancel()

		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		w.Header().Set("Connection", "keep-alive")
		w.WriteHeader(http.StatusOK)
		flusher.Flush()

		for {
			select {
			case <-r.Context().Done():
				return
			case event, ok := <-events:
				if !ok {
					return
				}
				payload, err := json.Marshal(wireEvent{Group: event.Group, ConnID: event.ConnID, Kind: kindString(event.Kind)})
				if err != nil {
					log.Printf("watch: marshal event: %v", err)
					continue
				}
				if _, err := w.Write([]byte("data: " + string(payload) + "\n\n")); err != nil {
					return
				}
				flusher.Flush()
			}
		}
	}
}

// membersHandler exposes Registry.GroupMembers as a point-in-time snapshot, so the demo
// UI can show who is online (and their device metadata) without waiting for a /watch
// event.
func membersHandler(registry *gateway.Registry) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		user := r.URL.Query().Get("user")
		if user == "" {
			http.Error(w, "user query parameter is required", http.StatusBadRequest)
			return
		}

		entries, err := registry.GroupMembers(r.Context(), userGroup(user))
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(entries)
	}
}

// serveIndex serves a small self-contained page for driving the demo from a browser: open
// it in one tab to watch a user's presence and in another to connect/disconnect as that
// user, and see the join/leave events arrive live.
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
<title>gateway-presence-watch demo</title>
<style>
  body { font-family: system-ui, sans-serif; max-width: 640px; margin: 2rem auto; padding: 0 1rem; }
  fieldset { margin-bottom: 1.5rem; }
  input { width: 10rem; }
  #log { background: #111; color: #0f0; padding: 0.75rem; height: 12rem; overflow-y: auto; font-family: monospace; white-space: pre-wrap; }
  #members { background: #f4f4f4; padding: 0.75rem; font-family: monospace; white-space: pre-wrap; }
</style>
</head>
<body>
<h1>gateway-presence-watch demo</h1>
<p>Simulates "friend came online": watch one user's presence in one tab, connect/disconnect as
that user in another, and see the join/leave events arrive without polling.</p>

<fieldset>
  <legend>1. Watch a user's presence (open this in its own tab)</legend>
  <label>watched user <input id="watchUser" value="alice"></label>
  <button id="watch">Start watching</button>
  <span id="watchStatus">idle</span>
</fieldset>

<fieldset>
  <legend>2. Connect/disconnect as a user (simulates that user's device coming online/offline)</legend>
  <label>user id <input id="user" value="alice"></label>
  <label>device <input id="device" value="phone"></label>
  <button id="connect">Connect</button>
  <button id="disconnect">Disconnect</button>
  <span id="status">disconnected</span>
</fieldset>

<fieldset>
  <legend>3. Snapshot current members (point-in-time, no watch needed)</legend>
  <label>user <input id="membersUser" value="alice"></label>
  <button id="checkMembers">Check</button>
  <div id="members"></div>
</fieldset>

<div id="log"></div>

<script>
let ws = null;
let es = null;
const logEl = document.getElementById('log');
function logLine(text) {
  logEl.textContent += text + "\n";
  logEl.scrollTop = logEl.scrollHeight;
}

document.getElementById('watch').onclick = () => {
  if (es) { es.close(); }
  const user = document.getElementById('watchUser').value;
  es = new EventSource('/watch?user=' + encodeURIComponent(user));
  document.getElementById('watchStatus').textContent = 'watching ' + user;
  es.onmessage = (evt) => logLine('[presence] ' + evt.data);
  es.onerror = () => { document.getElementById('watchStatus').textContent = 'watch error'; };
};

document.getElementById('connect').onclick = () => {
  if (ws) { ws.close(); }
  const user = document.getElementById('user').value;
  const device = document.getElementById('device').value;
  const proto = location.protocol === 'https:' ? 'wss:' : 'ws:';
  ws = new WebSocket(proto + '//' + location.host + '/ws?user=' + encodeURIComponent(user) + '&device=' + encodeURIComponent(device));
  ws.onopen = () => { document.getElementById('status').textContent = 'connected as ' + user + ' (' + device + ')'; logLine('[open] ' + user); };
  ws.onclose = () => { document.getElementById('status').textContent = 'disconnected'; logLine('[close]'); };
  ws.onerror = () => logLine('[error]');
};

document.getElementById('disconnect').onclick = () => {
  if (ws) { ws.close(); }
};

document.getElementById('checkMembers').onclick = async () => {
  const user = document.getElementById('membersUser').value;
  const res = await fetch('/members?user=' + encodeURIComponent(user));
  document.getElementById('members').textContent = await res.text();
};
</script>
</body>
</html>`
