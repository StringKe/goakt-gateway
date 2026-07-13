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

// Command persistence is a minimal, runnable version of gateway's opt-in at-least-once
// delivery: a gateway.Outbox plus Registry.Ack turning the default fire-and-forget socket
// write into "persist, deliver, wait for the client's ack, redeliver on reconnect if it
// never came".
//
//   - Registry.SendToConnection persists every payload to the Outbox before it touches the
//     socket (see WithOutbox). That happens whether or not the connection is currently
//     online: sending to an offline user still queues the message, it just returns
//     ErrConnectionNotFound instead of writing anywhere. This sample's /send handler treats
//     that as success, not failure.
//   - A reconnect (Registry.Register) redelivers every message the Outbox still holds for
//     that connection id, in Seq order, before the caller of Register gets control back. The
//     demo page's "Connect" button exercises this: send messages, close the tab without
//     acking, reconnect, watch the same messages arrive again.
//   - Registry.Ack is how redelivery stops: it removes one message from the Outbox by the id
//     the Outbox assigned it. SendToConnection does not hand that id back to its caller (it
//     keeps the "send" primitive a plain payload-in/error-out call, see the package's
//     interfaceNotes on WithOutbox), so this sample recovers it the way the library expects
//     applications to: read it back with Outbox.Unacked. The wire message this sample sends
//     to the client carries its own application-level id (a UUID distinct from the Outbox's
//     id); on ack, the handler scans Unacked, matches that application id against the
//     (still small, in-flight) unacknowledged tail, and acks the Outbox entry it finds it in.
//
// What this deliberately does not demonstrate: exactly-once delivery. At-least-once means a
// message can be redelivered after the client already processed it but before its ack
// reached the server (the server crashes, or the connection drops, in that window). The demo
// page's message log shows duplicate ids arriving after a reconnect for exactly this reason;
// a real client discards a redelivered id it already rendered instead of showing it twice.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/google/uuid"
	goredis "github.com/redis/go-redis/v9"

	"github.com/tochemey/goakt/v4/actor"
	golog "github.com/tochemey/goakt/v4/log"

	gateway "github.com/StringKe/goakt-gateway"
	redisoutbox "github.com/StringKe/goakt-gateway/persistence/redis"
)

// wireMessage is the JSON payload this sample writes to the socket and expects the client to
// echo the ID of back in an ack frame. ID is an application-level identifier, chosen here
// with a fresh UUID per message; it is unrelated to the id the Outbox assigns the same
// message internally (see ackHandler for how the two are reconciled).
type wireMessage struct {
	ID   string `json:"id"`
	Text string `json:"text"`
}

// inboundFrame is what the client sends back over the same WebSocket. The only frame this
// sample understands is an ack, naming the wireMessage.ID it received and processed.
type inboundFrame struct {
	Type string `json:"type"`
	ID   string `json:"id"`
}

func main() {
	ctx := context.Background()

	system, err := actor.NewActorSystem("gateway-persistence", actor.WithLogger(golog.DiscardLogger))
	if err != nil {
		log.Fatal(err)
	}
	if err := system.Start(ctx); err != nil {
		log.Fatal(err)
	}
	defer func() { _ = system.Stop(ctx) }()

	outbox := buildOutbox(ctx)
	registry := gateway.NewRegistry(system, golog.DiscardLogger, gateway.WithOutbox(outbox))
	defer func() { _ = registry.Close(ctx) }()

	mux := http.NewServeMux()
	mux.HandleFunc("/", serveIndex)
	mux.Handle("/ws", gateway.NewWSHandler(registry,
		gateway.WithWSAuth(authenticateFromQuery),
		gateway.WithWSOnMessage(ackHandler(registry, outbox)),
		gateway.WithWSOnConnect(func(_ context.Context, info *gateway.ConnInfo, _ *http.Request) {
			log.Printf("connect: conn=%q (any unacked tail was just redelivered)", info.ID)
		}),
		gateway.WithWSOnDisconnect(func(info *gateway.ConnInfo) {
			log.Printf("disconnect: conn=%q", info.ID)
		}),
	))
	mux.HandleFunc("/send", sendHandler(registry))
	mux.HandleFunc("/unacked", unackedHandler(outbox))

	server, err := gateway.NewServer("127.0.0.1:8080", mux)
	if err != nil {
		log.Fatal(err)
	}

	log.Println("gateway-persistence listening on http://127.0.0.1:8080")
	log.Fatal(server.ListenAndServe(ctx))
}

// buildOutbox picks the Outbox backend from REDIS_ADDR: unset means a single-node
// deployment, where the in-process MemoryOutbox is both sufficient and correct (there is no
// other node a reconnect could land on). A set REDIS_ADDR opts into the cluster-shared
// backend, so a reconnect that lands on a different process still finds the unacknowledged
// tail the original process persisted.
func buildOutbox(ctx context.Context) gateway.Outbox {
	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		log.Println("REDIS_ADDR not set: using in-process MemoryOutbox (unacked tail only survives within this process)")
		return gateway.NewMemoryOutbox()
	}

	client := goredis.NewClient(&goredis.Options{Addr: addr})
	pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := client.Ping(pingCtx).Err(); err != nil {
		log.Fatalf("REDIS_ADDR=%q set but unreachable: %v", addr, err)
	}
	log.Printf("using Redis outbox backend at %s", addr)
	return redisoutbox.New(client)
}

// authenticateFromQuery stands in for real session/token authentication. The connection id
// is fixed to the user id rather than randomized per socket, so that closing the tab and
// reconnecting as the same user resumes the same Outbox tail instead of starting a fresh one
// (takeover, on by default, is what lets the reconnect claim the same id at all).
func authenticateFromQuery(r *http.Request) (*gateway.ConnInfo, error) {
	user := r.URL.Query().Get("user")
	if user == "" {
		return nil, gateway.ErrUnauthorized
	}
	return &gateway.ConnInfo{ID: user}, nil
}

// ackHandler processes inbound WebSocket frames looking for acks. On an ack it has to
// recover the Outbox's internal message id from the application-level id the client reports,
// because SendToConnection never handed that internal id back to the sender (see the package
// doc comment). Unacked's result is the connection's small in-flight tail, so a linear scan
// matching wireMessage.ID against it is cheap and exactly the pattern the library's
// interfaceNotes describe.
func ackHandler(registry *gateway.Registry, outbox gateway.Outbox) func(context.Context, *gateway.ConnInfo, []byte) {
	return func(ctx context.Context, info *gateway.ConnInfo, payload []byte) {
		var frame inboundFrame
		if err := json.Unmarshal(payload, &frame); err != nil || frame.Type != "ack" || frame.ID == "" {
			return
		}

		unacked, err := outbox.Unacked(ctx, info.ID)
		if err != nil {
			log.Printf("ack: failed to read unacked tail for conn=%q: %v", info.ID, err)
			return
		}

		for _, msg := range unacked {
			var wm wireMessage
			if err := json.Unmarshal(msg.Payload, &wm); err != nil || wm.ID != frame.ID {
				continue
			}
			if err := registry.Ack(ctx, info.ID, msg.ID); err != nil {
				log.Printf("ack: failed to ack message %q for conn=%q: %v", frame.ID, info.ID, err)
				return
			}
			log.Printf("ack: conn=%q acked message %q (seq=%d)", info.ID, frame.ID, msg.Seq)
			return
		}
		log.Printf("ack: conn=%q acked message %q which is not in the unacked tail (already acked, or never sent)", info.ID, frame.ID)
	}
}

// sendHandler builds a wireMessage, hands it to SendToConnection, and reports whether it
// went straight to a live socket or was only queued because the target is offline right now.
// Both are success from this handler's point of view: with an Outbox configured,
// SendToConnection persists the message before it even looks for a live socket, so an
// offline send is not lost, only delayed until the next Register.
func sendHandler(registry *gateway.Registry) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		user := r.URL.Query().Get("user")
		text := r.URL.Query().Get("text")
		if user == "" || text == "" {
			http.Error(w, "user and text query parameters are required", http.StatusBadRequest)
			return
		}

		wm := wireMessage{ID: uuid.NewString(), Text: text}
		payload, err := json.Marshal(wm)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		err = registry.SendToConnection(r.Context(), user, payload)
		w.Header().Set("Content-Type", "application/json")
		switch {
		case err == nil:
			w.WriteHeader(http.StatusOK)
			_ = json.NewEncoder(w).Encode(map[string]string{"id": wm.ID, "status": "delivered (unacked until the client acks it)"})
		case errors.Is(err, gateway.ErrConnectionNotFound):
			w.WriteHeader(http.StatusAccepted)
			_ = json.NewEncoder(w).Encode(map[string]string{"id": wm.ID, "status": "queued (user is offline, will redeliver on reconnect)"})
		default:
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	}
}

// unackedEntry is the JSON shape /unacked reports: the Outbox's own id/seq alongside the
// text recovered from the stored payload, so the demo page can show what is actually still
// pending redelivery without a second source of truth.
type unackedEntry struct {
	OutboxID string `json:"outboxId"`
	Seq      uint64 `json:"seq"`
	Text     string `json:"text"`
}

// unackedHandler exposes Outbox.Unacked directly, so the demo page can show the tail
// draining as acks arrive instead of taking it on faith.
func unackedHandler(outbox gateway.Outbox) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		user := r.URL.Query().Get("user")
		if user == "" {
			http.Error(w, "user query parameter is required", http.StatusBadRequest)
			return
		}

		msgs, err := outbox.Unacked(r.Context(), user)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		entries := make([]unackedEntry, 0, len(msgs))
		for _, msg := range msgs {
			var wm wireMessage
			text := ""
			if json.Unmarshal(msg.Payload, &wm) == nil {
				text = wm.Text
			}
			entries = append(entries, unackedEntry{OutboxID: msg.ID, Seq: msg.Seq, Text: text})
		}

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(entries)
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
<title>gateway-persistence demo</title>
<style>
  body { font-family: system-ui, sans-serif; max-width: 720px; margin: 2rem auto; padding: 0 1rem; }
  fieldset { margin-bottom: 1.5rem; }
  input { width: 10rem; }
  #log { background: #111; color: #0f0; padding: 0.75rem; height: 12rem; overflow-y: auto; font-family: monospace; white-space: pre-wrap; }
  .msg { border-bottom: 1px solid #333; padding: 0.25rem 0; }
  .msg button { margin-left: 0.5rem; }
  #unacked { background: #222; color: #fc0; padding: 0.5rem; font-family: monospace; white-space: pre-wrap; min-height: 2rem; }
</style>
</head>
<body>
<h1>gateway-persistence demo</h1>
<p>At-least-once delivery: every message sent to a user is persisted before it reaches the
socket, redelivered on reconnect if unacked, and removed once the client acks it.</p>

<fieldset>
  <legend>1. Connect as a user</legend>
  <label>user id <input id="user" value="alice"></label>
  <button id="connect">Connect</button>
  <button id="disconnect">Disconnect (do not ack anything first)</button>
  <span id="status">disconnected</span>
</fieldset>

<fieldset>
  <legend>2. Send a message to a user (works even while they are offline)</legend>
  <label>target user <input id="targetUser" value="alice"></label>
  <label>text <input id="text" value="hello"></label>
  <button id="send">Send</button>
  <label><input type="checkbox" id="autoAck" checked> auto-ack on receipt</label>
</fieldset>

<fieldset>
  <legend>3. Received messages (uncheck auto-ack above, then Disconnect + Connect to see redelivery)</legend>
  <div id="messages"></div>
</fieldset>

<fieldset>
  <legend>4. Outbox tail for a user</legend>
  <label>user <input id="unackedUser" value="alice"></label>
  <button id="checkUnacked">Refresh</button>
  <div id="unacked"></div>
</fieldset>

<div id="log"></div>

<script>
let ws = null;
const logEl = document.getElementById('log');
const messagesEl = document.getElementById('messages');
function logLine(text) {
  logEl.textContent += text + "\n";
  logEl.scrollTop = logEl.scrollHeight;
}

function addMessage(id, text) {
  const div = document.createElement('div');
  div.className = 'msg';
  div.id = 'msg-' + id;
  const ackBtn = document.createElement('button');
  ackBtn.textContent = 'Ack';
  ackBtn.onclick = () => ackMessage(id);
  div.textContent = text + ' [' + id.slice(0, 8) + ']';
  div.appendChild(ackBtn);
  messagesEl.appendChild(div);
}

function ackMessage(id) {
  if (ws && ws.readyState === WebSocket.OPEN) {
    ws.send(JSON.stringify({type: 'ack', id: id}));
    logLine('[ack sent] ' + id);
    const el = document.getElementById('msg-' + id);
    if (el) { el.style.opacity = '0.4'; }
  }
}

document.getElementById('connect').onclick = () => {
  if (ws) { ws.close(); }
  const user = document.getElementById('user').value;
  const proto = location.protocol === 'https:' ? 'wss:' : 'ws:';
  ws = new WebSocket(proto + '//' + location.host + '/ws?user=' + encodeURIComponent(user));
  ws.onopen = () => { document.getElementById('status').textContent = 'connected as ' + user; logLine('[open] ' + user); };
  ws.onmessage = (evt) => {
    logLine('[message] ' + evt.data);
    const msg = JSON.parse(evt.data);
    addMessage(msg.id, msg.text);
    if (document.getElementById('autoAck').checked) { ackMessage(msg.id); }
  };
  ws.onclose = () => { document.getElementById('status').textContent = 'disconnected'; logLine('[close]'); };
  ws.onerror = () => logLine('[error]');
};

document.getElementById('disconnect').onclick = () => {
  if (ws) { ws.close(); ws = null; }
};

document.getElementById('send').onclick = async () => {
  const user = document.getElementById('targetUser').value;
  const text = document.getElementById('text').value;
  const res = await fetch('/send?user=' + encodeURIComponent(user) + '&text=' + encodeURIComponent(text));
  logLine('[send result] ' + await res.text());
};

document.getElementById('checkUnacked').onclick = async () => {
  const user = document.getElementById('unackedUser').value;
  const res = await fetch('/unacked?user=' + encodeURIComponent(user));
  document.getElementById('unacked').textContent = JSON.stringify(await res.json(), null, 2);
};
</script>
</body>
</html>`
