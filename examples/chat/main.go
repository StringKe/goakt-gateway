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

// Command chat is a runnable demonstration of topic broadcast in the gateway package: a
// room-based WebSocket chat where every message a client sends is fanned out to every other
// client in the same room via Registry.Broadcast, and WithExclude keeps the sender from
// getting an echo of its own line back. See README.md for what this sample does and does not
// show, and for the distinction it draws between a "room" (a pub/sub topic, shared by
// everyone listening) and a "user" (a ConnInfo.Group, shared by one identity's own
// devices/tabs).
package main

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"strings"

	"github.com/tochemey/goakt/v4/actor"
	golog "github.com/tochemey/goakt/v4/log"

	gateway "github.com/StringKe/goakt-gateway"
)

// errMissingParams is returned by authConn when the WebSocket upgrade is missing the room or
// user query parameter; ws.go surfaces it to the client as a 403.
var errMissingParams = errors.New("room and user query parameters are required")

// chatMessage is the JSON envelope every client receives: either another user's chat line
// (Type "chat") or a room-wide join/leave notice (Type "system").
type chatMessage struct {
	Type string `json:"type"`
	Room string `json:"room"`
	User string `json:"user,omitempty"`
	Text string `json:"text"`
}

// roomTopic maps a room name to the pub/sub topic every connection in that room joins.
// Namespacing it keeps a room name typed by a user from ever colliding with an internal
// gateway topic.
func roomTopic(room string) string {
	return "room:" + room
}

func main() {
	ctx := context.Background()

	// WithPubSub is required: ConnInfo.Topics/Group registration and Registry.Broadcast both
	// ride on the actor system's topic actor, so without it every /ws upgrade in this sample
	// would fail its registration.
	system, err := actor.NewActorSystem("gateway-chat", actor.WithLogger(golog.DiscardLogger), actor.WithPubSub())
	if err != nil {
		log.Fatal(err)
	}
	if err := system.Start(ctx); err != nil {
		log.Fatal(err)
	}
	defer func() { _ = system.Stop(ctx) }()

	registry := gateway.NewRegistry(system, golog.DiscardLogger)
	defer func() { _ = registry.Close(ctx) }()

	mux := http.NewServeMux()
	mux.HandleFunc("/", serveIndex)
	mux.Handle("/ws", newChatHandler(registry))

	server, err := gateway.NewServer("127.0.0.1:8082", mux)
	if err != nil {
		log.Fatal(err)
	}

	log.Println("gateway-chat listening on http://127.0.0.1:8082 - open it in two browser tabs to chat")
	log.Fatal(server.ListenAndServe(ctx))
}

// newChatHandler wires the /ws endpoint: WithWSAuth resolves the room/user query parameters
// into a ConnInfo once, and the OnConnect/OnMessage/OnDisconnect callbacks broadcast to the
// resolved room topic from there.
func newChatHandler(registry *gateway.Registry) *gateway.WSHandler {
	return gateway.NewWSHandler(registry,
		gateway.WithWSAuth(authConn),
		gateway.WithWSOnConnect(func(ctx context.Context, info *gateway.ConnInfo, _ *http.Request) {
			room, user := info.Meta["room"], info.Meta["user"]
			log.Printf("chat: %q joined room %q (conn=%s)", user, room, info.ID)
			broadcastSystem(ctx, registry, room, user+" joined the room")
		}),
		gateway.WithWSOnMessage(func(ctx context.Context, info *gateway.ConnInfo, payload []byte) {
			room, user := info.Meta["room"], info.Meta["user"]
			data, err := json.Marshal(chatMessage{Type: "chat", Room: room, User: user, Text: string(payload)})
			if err != nil {
				log.Printf("chat: marshal message from %q failed: %v", user, err)
				return
			}
			// Exclude the sender's own connection: it already has the text it just typed, an
			// echo of it back over the wire would just be noise. WithExclude travels with the
			// payload to every node, so this holds even if the sender's own socket is on a
			// different node than the one running this handler.
			result, err := registry.Broadcast(ctx, roomTopic(room), data, gateway.WithExclude(info.ID))
			if err != nil {
				log.Printf("chat: broadcast to room %q failed: %v", room, err)
				return
			}
			log.Printf("chat: room=%q user=%q delivered=%d dropped=%d remote=%d",
				room, user, result.Delivered, result.Dropped, result.Remote)
		}),
		gateway.WithWSOnDisconnect(func(info *gateway.ConnInfo) {
			room, user := info.Meta["room"], info.Meta["user"]
			log.Printf("chat: %q left room %q (conn=%s)", user, room, info.ID)
			// OnDisconnect carries no context (the request that created it is long gone), so
			// this uses a fresh one; the broadcast is best-effort cleanup, not part of any
			// caller's request lifecycle.
			broadcastSystem(context.Background(), registry, room, user+" left the room")
		}),
	)
}

// authConn resolves the room and user query parameters into a ConnInfo: room becomes the
// pub/sub topic the connection joins, user becomes the identity Group. ID is left empty so
// the handler assigns a fresh uuid per connection - that is what lets the same user open two
// browser tabs (same Group, two different connection ids) without one evicting the other.
func authConn(r *http.Request) (*gateway.ConnInfo, error) {
	room := strings.TrimSpace(r.URL.Query().Get("room"))
	user := strings.TrimSpace(r.URL.Query().Get("user"))
	if room == "" || user == "" {
		return nil, errMissingParams
	}
	return &gateway.ConnInfo{
		Group:  "user:" + user,
		Topics: []string{roomTopic(room)},
		Meta:   map[string]string{"room": room, "user": user},
	}, nil
}

// broadcastSystem sends a room-wide join/leave notice. Unlike a chat message it excludes
// nobody: the connection that triggered it should see its own join/leave confirmed too.
func broadcastSystem(ctx context.Context, registry *gateway.Registry, room, text string) {
	data, err := json.Marshal(chatMessage{Type: "system", Room: room, Text: text})
	if err != nil {
		log.Printf("chat: marshal system message failed: %v", err)
		return
	}
	if _, err := registry.Broadcast(ctx, roomTopic(room), data); err != nil {
		log.Printf("chat: system broadcast to room %q failed: %v", room, err)
	}
}

// serveIndex serves the single-page chat UI. It is a plain http.HandlerFunc, not part of the
// gateway package - the point of this sample is that gateway only owns the /ws upgrade, and
// everything else is the application's own HTTP handling.
func serveIndex(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = w.Write([]byte(indexHTML))
}

// indexHTML is a minimal, dependency-free chat UI: a room/user join form and a message log.
// It is embedded here rather than shipped as a separate static file so `go run ./examples/chat`
// is the only step required to try the sample.
const indexHTML = `<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>gateway chat example</title>
<style>
  body { font-family: system-ui, sans-serif; max-width: 640px; margin: 2rem auto; padding: 0 1rem; }
  #join label { display: block; margin-bottom: 0.75rem; }
  #join input { width: 100%; box-sizing: border-box; padding: 0.4rem; }
  #log { height: 320px; overflow-y: auto; border: 1px solid #ccc; border-radius: 4px; padding: 0.5rem; margin-bottom: 0.5rem; }
  #log div { padding: 0.15rem 0; }
  .system { color: #888; font-style: italic; }
  .error { color: #c00; font-weight: bold; }
  #compose { display: flex; gap: 0.5rem; }
  #msg { flex: 1; padding: 0.4rem; }
</style>
</head>
<body>
<h1>gateway chat example</h1>

<div id="join">
  <label>Room <input id="room" value="lobby"></label>
  <label>User <input id="user" placeholder="e.g. alice"></label>
  <button id="joinBtn">Join</button>
</div>

<div id="chat" style="display:none">
  <div id="log"></div>
  <div id="compose">
    <input id="msg" placeholder="type a message and press Enter">
    <button id="sendBtn">Send</button>
    <button id="leaveBtn">Leave</button>
  </div>
</div>

<script>
(function () {
  var ws = null;
  var joinDiv = document.getElementById('join');
  var chatDiv = document.getElementById('chat');
  var logDiv = document.getElementById('log');
  var roomInput = document.getElementById('room');
  var userInput = document.getElementById('user');
  var msgInput = document.getElementById('msg');

  function appendLine(text, cls) {
    var line = document.createElement('div');
    if (cls) line.className = cls;
    line.textContent = text;
    logDiv.appendChild(line);
    logDiv.scrollTop = logDiv.scrollHeight;
  }

  function join() {
    var room = roomInput.value.trim();
    var user = userInput.value.trim();
    if (!room || !user) {
      alert('room and user are both required');
      return;
    }
    var proto = location.protocol === 'https:' ? 'wss:' : 'ws:';
    var url = proto + '//' + location.host + '/ws?room=' + encodeURIComponent(room) + '&user=' + encodeURIComponent(user);
    ws = new WebSocket(url);

    ws.onopen = function () {
      joinDiv.style.display = 'none';
      chatDiv.style.display = 'block';
      logDiv.textContent = '';
      appendLine('connected to room "' + room + '" as "' + user + '"', 'system');
      msgInput.focus();
    };

    ws.onmessage = function (event) {
      // Regression check: the server must send WebSocket text frames (websocket.MessageText),
      // so event.data has to already be a string here. A Blob here would mean binary frames
      // slipped back in and this sample would be silently lying about the fix.
      if (typeof event.data !== 'string') {
        appendLine('ERROR: received a non-text frame (' + Object.prototype.toString.call(event.data) + ')', 'error');
        return;
      }
      var payload;
      try {
        payload = JSON.parse(event.data);
      } catch (e) {
        appendLine('ERROR: could not parse message: ' + event.data, 'error');
        return;
      }
      if (payload.type === 'system') {
        appendLine(payload.text, 'system');
      } else {
        appendLine(payload.user + ': ' + payload.text);
      }
    };

    ws.onclose = function () {
      appendLine('disconnected', 'system');
      joinDiv.style.display = 'block';
      chatDiv.style.display = 'none';
      ws = null;
    };

    ws.onerror = function () {
      appendLine('websocket error, see devtools console', 'error');
    };
  }

  function send() {
    var text = msgInput.value.trim();
    if (!text || !ws || ws.readyState !== WebSocket.OPEN) {
      return;
    }
    ws.send(text);
    msgInput.value = '';
  }

  document.getElementById('joinBtn').addEventListener('click', join);
  document.getElementById('sendBtn').addEventListener('click', send);
  document.getElementById('leaveBtn').addEventListener('click', function () {
    if (ws) ws.close();
  });
  msgInput.addEventListener('keydown', function (e) {
    if (e.key === 'Enter') {
      send();
    }
  });
})();
</script>
</body>
</html>
`
