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

// Command graphql-ws demonstrates that gateway.Registry is usable without
// gateway.WSHandler at all. It hand-rolls the graphql-transport-ws subprotocol
// (https://github.com/enisdenjo/graphql-ws/blob/master/PROTOCOL.md) directly on top of
// github.com/coder/websocket and drives the Registry the same way WSHandler does
// internally: Register/Unregister/Join/Leave/Broadcast are all exported, ordinary
// methods, not something only the shipped handler can call.
//
// The scenario this mirrors is mip-aio's real one: it hosts WebSocket through gqlgen,
// which owns the upgrade and the message framing, and authenticates on the first
// application-level frame (connection_init.payload.Authorization) rather than on the
// HTTP upgrade request - a shape gateway.WSHandler's WSAuthFunc (which only ever sees
// the *http.Request) cannot express. See README.md for the full write-up of when to
// reach for WSHandler versus writing a handler like this one.
//
// This is a deliberately minimal subset of graphql-transport-ws: connection_init,
// connection_ack, subscribe, next, complete, ping and pong. It is enough to prove the
// Registry integration; it is not a spec-complete implementation (no query execution,
// no error/complete-on-error framing, no duplicate-subscription-id rejection). See
// README.md for the exact list of what is left out.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/coder/websocket"
	"github.com/google/uuid"

	"github.com/tochemey/goakt/v4/actor"
	golog "github.com/tochemey/goakt/v4/log"

	gateway "github.com/StringKe/goakt-gateway"
)

// graphqlWSSubprotocol is the subprotocol name graphql-ws clients (Apollo Client,
// urql, graphiql) negotiate for the transport described by PROTOCOL.md.
const graphqlWSSubprotocol = "graphql-transport-ws"

// demoToken is the bearer token connection_init.payload.Authorization must carry.
// Hardcoded because this is a demo of the transport, not of a real auth scheme; swap in
// your own token/JWT validation in resolveConnInfo's place (see README.md).
const demoToken = "Bearer demo-token"

// connectionInitWait bounds how long the server waits for connection_init after the
// upgrade completes. A client that never authenticates would otherwise hold the socket
// (and its goroutines) open forever.
const connectionInitWait = 10 * time.Second

// graphql-ws message types this server understands. The protocol has a few more
// (error, ping/pong are listed, subscribe/next/complete are listed); this is the subset
// named in the task and README.md.
const (
	msgConnectionInit = "connection_init"
	msgConnectionAck  = "connection_ack"
	msgSubscribe      = "subscribe"
	msgNext           = "next"
	msgComplete       = "complete"
	msgPing           = "ping"
	msgPong           = "pong"
)

// graphql-ws close codes, from PROTOCOL.md. Both are outside the standard RFC 6455
// range (3000-4999 is reserved for application use) so a client library can tell them
// apart from a generic abnormal close.
const (
	closeCodeUnauthorized  websocket.StatusCode = 4401
	closeCodeInitTimeout   websocket.StatusCode = 4408
	closeCodeInvalidPacket websocket.StatusCode = 4400
)

// clientMessage is the wire shape of every frame in both directions. payload is kept as
// json.RawMessage because its shape depends on messageType.
type clientMessage struct {
	ID      string          `json:"id,omitempty"`
	Type    string          `json:"type"`
	Payload json.RawMessage `json:"payload,omitempty"`
}

// initPayload is connection_init's payload: the one place this transport carries
// authentication, since it happens after the HTTP upgrade rather than in a header.
type initPayload struct {
	Authorization string `json:"Authorization"`
}

// subscribePayload is subscribe's payload. A real GraphQL server would also carry
// query/variables/operationName here; this demo only needs a topic to hand to
// Registry.Join, so that is the only field it defines.
type subscribePayload struct {
	Topic string `json:"topic"`
}

// publishEnvelope is the payload gateway.Registry.Broadcast carries end to end. The
// Registry treats payloads as opaque bytes and does not tell a delivered connection
// which topic a message arrived on (one physical socket can be joined to several), so
// the topic has to travel inside the payload itself for the server to route it back to
// the right subscription id(s).
type publishEnvelope struct {
	Topic string          `json:"topic"`
	Data  json.RawMessage `json:"data"`
}

func main() {
	addr := flag.String("addr", "127.0.0.1:8090", "listen address")
	flag.Parse()

	ctx := context.Background()

	// WithPubSub is required: Registry.Join/subscribe registration builds its cluster
	// fan-out on the actor system's pub/sub, and Register/Join return
	// ErrPubSubUnavailable without it.
	system, err := actor.NewActorSystem("gateway-graphql-ws", actor.WithLogger(golog.DiscardLogger), actor.WithPubSub())
	if err != nil {
		log.Fatal(err)
	}
	if err := system.Start(ctx); err != nil {
		log.Fatal(err)
	}
	defer func() { _ = system.Stop(ctx) }()

	registry := gateway.NewRegistry(system, golog.DiscardLogger)
	defer func() { _ = registry.Close(context.Background()) }()

	srv := &graphqlWSServer{registry: registry}

	mux := http.NewServeMux()
	mux.HandleFunc("/graphql", srv.serveWS)
	mux.HandleFunc("/publish", srv.servePublish)

	log.Printf("gateway-graphql-ws listening on http://%s (ws: /graphql, publish: /publish?topic=room:1&data=hello)", *addr)
	log.Fatal(http.ListenAndServe(*addr, mux))
}

// graphqlWSServer owns the Registry and has no other state: every connection's
// subscription bookkeeping lives in its own gqlConn, not here.
type graphqlWSServer struct {
	registry *gateway.Registry
}

// servePublish is the "ordinary HTTP handler talks to a websocket connection through
// the Registry" side of the demo. It has no idea whether topic's subscribers are on
// this process or another node in the cluster - that is exactly the point of
// Registry.Broadcast.
func (s *graphqlWSServer) servePublish(w http.ResponseWriter, r *http.Request) {
	topic := r.URL.Query().Get("topic")
	data := r.URL.Query().Get("data")
	if topic == "" || data == "" {
		http.Error(w, "topic and data query parameters are required", http.StatusBadRequest)
		return
	}

	envelope, err := json.Marshal(publishEnvelope{Topic: topic, Data: mustRawString(data)})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	result, err := s.registry.Broadcast(r.Context(), topic, envelope)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if _, err := fmt.Fprintf(w, "broadcast to topic %q: delivered=%d dropped=%d remote=%d\n",
		topic, result.Delivered, result.Dropped, result.Remote); err != nil {
		log.Printf("write broadcast response: %v", err)
	}
}

// mustRawString marshals s as a JSON string. Used to build publishEnvelope.Data from a
// plain query parameter without pulling in a JSON round trip at the call site.
func mustRawString(s string) json.RawMessage {
	b, _ := json.Marshal(s)
	return b
}

// serveWS upgrades to graphql-transport-ws and runs the connection until it closes.
// This function - not gateway.WSHandler - owns the entire lifecycle: negotiating the
// subprotocol, the connection_init handshake, and every read/write on the socket. The
// Registry only ever sees a connection id and a send func, exactly as it would from
// WSHandler.
func (s *graphqlWSServer) serveWS(w http.ResponseWriter, r *http.Request) {
	conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{
		Subprotocols: []string{graphqlWSSubprotocol},
	})
	if err != nil {
		// Accept already wrote the failure response.
		return
	}
	defer func() {
		if err := conn.CloseNow(); err != nil {
			log.Printf("close websocket connection: %v", err)
		}
	}()

	if conn.Subprotocol() != graphqlWSSubprotocol {
		// graphql-ws clients refuse to talk to a server that did not negotiate their
		// subprotocol; failing the same way here surfaces a clear reason in the logs
		// instead of a confusing later protocol error.
		_ = conn.Close(websocket.StatusProtocolError, "server requires the graphql-transport-ws subprotocol")
		return
	}

	c := &gqlConn{
		id:     uuid.NewString(),
		conn:   conn,
		server: s,
		subs:   make(map[string]string),
		topics: make(map[string]int),
	}
	c.run(r.Context())
}

// gqlConn is one accepted graphql-transport-ws socket: its Registry connection id, its
// live subscriptions, and the write serialization every WebSocket connection needs
// (coder/websocket forbids concurrent writers).
type gqlConn struct {
	id     string
	conn   *websocket.Conn
	server *graphqlWSServer

	writeMu sync.Mutex

	mu sync.Mutex
	// subs maps a client-chosen subscription id to the topic it subscribed to, so a
	// complete message or teardown knows what to unwind.
	subs map[string]string
	// topics reference-counts how many live subscriptions on this connection point at
	// each topic. Two subscribe messages can name the same topic under different
	// subscription ids; the Registry must only see one Join and one matching Leave.
	topics map[string]int

	registered bool
}

// run drives the connection end to end: wait for connection_init, register with the
// Registry once authenticated, then pump inbound frames until the socket dies.
func (c *gqlConn) run(reqCtx context.Context) {
	ctx, cancel := context.WithCancel(context.WithoutCancel(reqCtx))
	defer cancel()

	info, err := c.awaitInit(ctx)
	if err != nil {
		if errors.Is(err, errInitTimeout) {
			_ = c.conn.Close(closeCodeInitTimeout, "connection initialisation timeout")
		} else if errors.Is(err, errUnauthorized) {
			_ = c.conn.Close(closeCodeUnauthorized, "Unauthorized")
		} else {
			_ = c.conn.Close(closeCodeInvalidPacket, "invalid connection_init")
		}
		return
	}

	// send is the Registry's delivery function for this connection. It closes over ctx
	// (valid for the connection's whole registered lifetime - see the defer ordering
	// below) and hands every delivered payload to deliver, which decodes the
	// publishEnvelope and fans it out to the right subscription id(s).
	send := func(payload []byte) error {
		return c.deliver(ctx, payload)
	}

	// Registered under c.id with no topics yet: subscribe messages Join topics one at a
	// time, exactly mirroring how a GraphQL server only knows what a client wants to
	// hear about once it sends a subscribe operation.
	if err := c.server.registry.Register(ctx, c.id, send, gateway.WithConnMeta(info)); err != nil {
		log.Printf("graphql-ws %s: registry.Register failed: %v", c.id, err)
		_ = c.conn.Close(websocket.StatusInternalError, "registration failed")
		return
	}
	c.registered = true
	defer c.teardown(context.WithoutCancel(ctx))

	if err := c.writeMessage(ctx, clientMessage{Type: msgConnectionAck}); err != nil {
		return
	}

	c.readLoop(ctx)
}

var (
	errInitTimeout  = errors.New("graphql-ws: connection_init timed out")
	errUnauthorized = errors.New("graphql-ws: unauthorized")
	errBadInit      = errors.New("graphql-ws: first message was not connection_init")
)

// awaitInit reads exactly one message and requires it to be a connection_init carrying
// the demo bearer token. Per PROTOCOL.md this is the only place authentication happens
// in this transport: everything before it is just the HTTP upgrade, which in mip-aio's
// real deployment carries no auth at all.
func (c *gqlConn) awaitInit(ctx context.Context) (map[string]string, error) {
	initCtx, cancel := context.WithTimeout(ctx, connectionInitWait)
	defer cancel()

	_, data, err := c.conn.Read(initCtx)
	if err != nil {
		if initCtx.Err() != nil {
			return nil, errInitTimeout
		}
		return nil, err
	}

	var msg clientMessage
	if err := json.Unmarshal(data, &msg); err != nil || msg.Type != msgConnectionInit {
		return nil, errBadInit
	}

	var payload initPayload
	if len(msg.Payload) > 0 {
		if err := json.Unmarshal(msg.Payload, &payload); err != nil {
			return nil, errBadInit
		}
	}
	if payload.Authorization != demoToken {
		return nil, errUnauthorized
	}

	// Carried into ConnInfo.Meta purely so it is visible to anything inspecting the
	// Registry's entry for this connection (e.g. an Observer); this demo does not read
	// it back.
	return map[string]string{"authorization": payload.Authorization}, nil
}

// readLoop pumps subscribe/complete/ping/pong frames until the socket closes.
func (c *gqlConn) readLoop(ctx context.Context) {
	for {
		_, data, err := c.conn.Read(ctx)
		if err != nil {
			return
		}

		var msg clientMessage
		if err := json.Unmarshal(data, &msg); err != nil {
			_ = c.conn.Close(closeCodeInvalidPacket, "invalid message")
			return
		}

		switch msg.Type {
		case msgSubscribe:
			c.handleSubscribe(ctx, msg)
		case msgComplete:
			c.handleComplete(ctx, msg)
		case msgPing:
			_ = c.writeMessage(ctx, clientMessage{Type: msgPong})
		case msgPong:
			// No server-initiated pings in this demo; nothing to correlate a pong to.
		default:
			log.Printf("graphql-ws %s: ignoring unknown message type %q", c.id, msg.Type)
		}
	}
}

// handleSubscribe joins the connection to the requested topic (Registry.Join is a
// no-op if it is already joined) and records the subscription id -> topic mapping used
// both to fan a delivered payload out to the right "next" frame(s) and to unwind on
// complete/disconnect.
func (c *gqlConn) handleSubscribe(ctx context.Context, msg clientMessage) {
	var payload subscribePayload
	if err := json.Unmarshal(msg.Payload, &payload); err != nil || payload.Topic == "" {
		_ = c.conn.Close(closeCodeInvalidPacket, "subscribe requires a topic")
		return
	}

	c.mu.Lock()
	if _, exists := c.subs[msg.ID]; exists {
		c.mu.Unlock()
		// PROTOCOL.md calls for close code 4409 here; out of scope for this demo's
		// subset, logging is enough to make the situation visible.
		log.Printf("graphql-ws %s: subscription id %q already in use", c.id, msg.ID)
		return
	}
	c.subs[msg.ID] = payload.Topic
	firstOnTopic := c.topics[payload.Topic] == 0
	c.topics[payload.Topic]++
	c.mu.Unlock()

	if !firstOnTopic {
		return
	}
	if err := c.server.registry.Join(ctx, c.id, payload.Topic); err != nil {
		c.mu.Lock()
		delete(c.subs, msg.ID)
		c.topics[payload.Topic]--
		c.mu.Unlock()
		log.Printf("graphql-ws %s: failed to join topic %q: %v", c.id, payload.Topic, err)
		_ = c.conn.Close(websocket.StatusInternalError, "subscribe failed")
	}
}

// handleComplete unwinds a single subscription id and, once it was the last one on its
// topic, leaves the topic in the Registry.
func (c *gqlConn) handleComplete(ctx context.Context, msg clientMessage) {
	c.mu.Lock()
	topic, exists := c.subs[msg.ID]
	if !exists {
		c.mu.Unlock()
		return
	}
	delete(c.subs, msg.ID)
	c.topics[topic]--
	lastOnTopic := c.topics[topic] == 0
	if lastOnTopic {
		delete(c.topics, topic)
	}
	c.mu.Unlock()

	if lastOnTopic {
		if err := c.server.registry.Leave(ctx, c.id, topic); err != nil {
			log.Printf("graphql-ws %s: failed to leave topic %q: %v", c.id, topic, err)
		}
	}
}

// deliver is the Registry's send func for this connection (installed in run). A
// payload always arrives as a publishEnvelope, since that is the only shape this
// server's own /publish handler ever broadcasts; it is decoded here and fanned out to
// every subscription id currently listening on its topic as a graphql-ws "next" frame.
//
// Named deliver rather than being an inline closure so the topic->subscription fan-out
// logic (which needs c.mu) is readable on its own; run wires it into
// Registry.Register as send.
func (c *gqlConn) deliver(ctx context.Context, payload []byte) error {
	var envelope publishEnvelope
	if err := json.Unmarshal(payload, &envelope); err != nil {
		return fmt.Errorf("graphql-ws: payload is not a publishEnvelope: %w", err)
	}

	c.mu.Lock()
	var ids []string
	for subID, topic := range c.subs {
		if topic == envelope.Topic {
			ids = append(ids, subID)
		}
	}
	c.mu.Unlock()

	for _, id := range ids {
		if err := c.writeMessage(ctx, clientMessage{ID: id, Type: msgNext, Payload: envelope.Data}); err != nil {
			return err
		}
	}
	return nil
}

// writeMessage serializes and writes a single graphql-ws frame, serialized against
// every other writer on this connection (coder/websocket forbids concurrent writes).
func (c *gqlConn) writeMessage(ctx context.Context, msg clientMessage) error {
	data, err := json.Marshal(msg)
	if err != nil {
		return err
	}

	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	writeCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	return c.conn.Write(writeCtx, websocket.MessageText, data)
}

// teardown leaves every topic the connection was still subscribed to and unregisters
// it from the Registry. Called once, from a defer in run, only when Register actually
// succeeded.
func (c *gqlConn) teardown(ctx context.Context) {
	if !c.registered {
		return
	}

	c.mu.Lock()
	topics := make([]string, 0, len(c.topics))
	for topic := range c.topics {
		topics = append(topics, topic)
	}
	c.mu.Unlock()

	for _, topic := range topics {
		if err := c.server.registry.Leave(ctx, c.id, topic); err != nil {
			log.Printf("graphql-ws %s: failed to leave topic %q during teardown: %v", c.id, topic, err)
		}
	}
	if err := c.server.registry.Unregister(ctx, c.id); err != nil {
		log.Printf("graphql-ws %s: failed to unregister: %v", c.id, err)
	}
}
