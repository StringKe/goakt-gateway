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

package gateway_test

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/coder/websocket"
	"github.com/stretchr/testify/require"

	"github.com/tochemey/goakt/v4/actor"
	"github.com/tochemey/goakt/v4/log"
	"github.com/tochemey/goakt/v4/testkit"

	gateway "github.com/StringKe/goakt-gateway"
)

func wsURL(t *testing.T, server *httptest.Server, path string) string {
	t.Helper()
	return "ws" + strings.TrimPrefix(server.URL, "http") + path
}

// dialWS opens a client connection to server and closes it when the test ends.
func dialWS(t *testing.T, server *httptest.Server, path string, opts *websocket.DialOptions) *websocket.Conn {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, _, err := websocket.Dial(ctx, wsURL(t, server, path), opts)
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.CloseNow() })
	return conn
}

// readWS reads a single message with a deadline.
func readWS(t *testing.T, conn *websocket.Conn) (websocket.MessageType, []byte) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	typ, payload, err := conn.Read(ctx)
	require.NoError(t, err)
	return typ, payload
}

// awaitRegistered waits for id to show up in the registry, which happens asynchronously with
// respect to the client's successful dial.
func awaitRegistered(t *testing.T, registry *gateway.Registry, id string, want bool) {
	t.Helper()
	require.Eventually(t, func() bool {
		return registry.Has(id) == want
	}, 5*time.Second, 10*time.Millisecond)
}

func TestWSHandlerEchoUsesTextFrames(t *testing.T) {
	system := newTestSystem(t)
	registry := gateway.NewRegistry(system, log.DiscardLogger)

	handler := gateway.NewWSHandler(registry,
		gateway.WithWSIDFunc(func(r *http.Request) string { return r.URL.Query().Get("id") }),
		gateway.WithWSOnMessage(func(ctx context.Context, info *gateway.ConnInfo, payload []byte) {
			_ = registry.SendToConnection(ctx, info.ID, payload)
		}),
	)

	server := httptest.NewServer(handler)
	defer server.Close()

	conn := dialWS(t, server, "/?id=echo-1", nil)
	require.NoError(t, conn.Write(context.Background(), websocket.MessageText, []byte("ping")))

	typ, reply := readWS(t, conn)
	require.Equal(t, websocket.MessageText, typ)
	require.Equal(t, []byte("ping"), reply)
}

func TestWSHandlerBinaryMessageType(t *testing.T) {
	system := newTestSystem(t)
	registry := gateway.NewRegistry(system, log.DiscardLogger)

	handler := gateway.NewWSHandler(registry,
		gateway.WithWSIDFunc(func(r *http.Request) string { return r.URL.Query().Get("id") }),
		gateway.WithWSMessageType(websocket.MessageBinary),
	)

	server := httptest.NewServer(handler)
	defer server.Close()

	conn := dialWS(t, server, "/?id=bin-1", nil)
	awaitRegistered(t, registry, "bin-1", true)

	require.NoError(t, registry.SendToConnection(context.Background(), "bin-1", []byte("blob")))

	typ, reply := readWS(t, conn)
	require.Equal(t, websocket.MessageBinary, typ)
	require.Equal(t, []byte("blob"), reply)
}

func TestWSHandlerOutboxEnvelopeReplaysAndAcksBinaryPayload(t *testing.T) {
	system := newTestSystem(t)
	outbox := gateway.NewMemoryOutbox()
	registry := gateway.NewRegistry(system, log.DiscardLogger, gateway.WithOutbox(outbox), gateway.WithOutboxEnvelope())
	t.Cleanup(func() { _ = registry.Close(context.Background()) })

	handler := gateway.NewWSHandler(registry,
		gateway.WithWSIDFunc(func(r *http.Request) string { return r.URL.Query().Get("id") }),
	)
	server := httptest.NewServer(handler)
	defer server.Close()

	const id = "outbox-ws"
	wantPayload := []byte{0x00, 0x0A, 0xFF, 0x3A}
	first := dialWS(t, server, "/?id="+id, nil)
	awaitRegistered(t, registry, id, true)

	require.NoError(t, registry.SendToConnection(context.Background(), id, wantPayload))
	typ, frame := readWS(t, first)
	require.Equal(t, websocket.MessageText, typ)
	require.True(t, utf8.Valid(frame))
	msgID, seq, payload, err := gateway.DecodeOutboxTextEnvelope(frame)
	require.NoError(t, err)
	require.Equal(t, wantPayload, payload)

	require.NoError(t, first.Close(websocket.StatusNormalClosure, "reconnect"))
	awaitRegistered(t, registry, id, false)

	second := dialWS(t, server, "/?id="+id, nil)
	awaitRegistered(t, registry, id, true)
	replayType, replayFrame := readWS(t, second)
	require.Equal(t, websocket.MessageText, replayType)
	require.True(t, utf8.Valid(replayFrame))
	replayID, replaySeq, replayPayload, err := gateway.DecodeOutboxTextEnvelope(replayFrame)
	require.NoError(t, err)
	require.Equal(t, msgID, replayID)
	require.Equal(t, seq, replaySeq)
	require.Equal(t, wantPayload, replayPayload)

	require.NoError(t, registry.Ack(context.Background(), id, msgID))
	unacked, err := outbox.Unacked(context.Background(), id)
	require.NoError(t, err)
	require.Empty(t, unacked)
}

func TestWSHandlerAuthRejected(t *testing.T) {
	system := newTestSystem(t)
	registry := gateway.NewRegistry(system, log.DiscardLogger)

	handler := gateway.NewWSHandler(registry,
		gateway.WithWSAuth(func(*http.Request) (*gateway.ConnInfo, error) {
			return nil, errors.New("no token")
		}),
	)

	server := httptest.NewServer(handler)
	defer server.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, resp, err := websocket.Dial(ctx, wsURL(t, server, "/"), nil)
	require.Error(t, err)
	require.NotNil(t, resp)
	require.Equal(t, http.StatusForbidden, resp.StatusCode)
}

func TestWSHandlerAuthConnInfoPropagates(t *testing.T) {
	system := newTestSystem(t, actor.WithPubSub())
	registry := gateway.NewRegistry(system, log.DiscardLogger)

	connected := make(chan *gateway.ConnInfo, 1)
	messages := make(chan *gateway.ConnInfo, 1)

	handler := gateway.NewWSHandler(registry,
		gateway.WithWSSubprotocols("gateway.v1"),
		gateway.WithWSAuth(func(*http.Request) (*gateway.ConnInfo, error) {
			return &gateway.ConnInfo{
				ID:     "conn-1",
				Group:  "user:123",
				Topics: []string{"room-7"},
				Meta:   map[string]string{"role": "admin"},
			}, nil
		}),
		gateway.WithWSOnConnect(func(_ context.Context, info *gateway.ConnInfo, _ *http.Request) {
			connected <- info
		}),
		gateway.WithWSOnMessage(func(_ context.Context, info *gateway.ConnInfo, _ []byte) {
			messages <- info
		}),
	)

	server := httptest.NewServer(handler)
	defer server.Close()

	conn := dialWS(t, server, "/", &websocket.DialOptions{Subprotocols: []string{"gateway.v1"}})

	var info *gateway.ConnInfo
	select {
	case info = <-connected:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for the connect callback")
	}
	require.Equal(t, "conn-1", info.ID)
	require.Equal(t, "user:123", info.Group)
	require.Equal(t, []string{"room-7"}, info.Topics)
	require.Equal(t, "admin", info.Meta["role"])
	require.Equal(t, "gateway.v1", info.Meta["subprotocol"])
	require.Equal(t, []string{"conn-1"}, registry.LocalConnectionsOf("user:123"))

	require.NoError(t, conn.Write(context.Background(), websocket.MessageText, []byte("hello")))

	select {
	case info = <-messages:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for the message callback")
	}
	require.Equal(t, "conn-1", info.ID)
	require.Equal(t, "user:123", info.Group)
	require.Equal(t, "admin", info.Meta["role"])
	require.Equal(t, "gateway.v1", info.Meta["subprotocol"])
}

func TestWSHandlerTopicBroadcast(t *testing.T) {
	system := newTestSystem(t, actor.WithPubSub())
	registry := gateway.NewRegistry(system, log.DiscardLogger)

	// Registry.Has already reports the id while its registration is still in flight, so topic
	// membership is only guaranteed once the connect callback has run.
	connected := make(chan *gateway.ConnInfo, 1)
	handler := gateway.NewWSHandler(registry,
		gateway.WithWSIDFunc(func(r *http.Request) string { return r.URL.Query().Get("id") }),
		gateway.WithWSTopics(func(*http.Request) []string { return []string{"room-42"} }),
		gateway.WithWSOnConnect(func(_ context.Context, info *gateway.ConnInfo, _ *http.Request) {
			connected <- info
		}),
	)

	server := httptest.NewServer(handler)
	defer server.Close()

	conn := dialWS(t, server, "/?id=room-member", nil)
	select {
	case <-connected:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for the connection to be registered")
	}

	_, err := registry.Broadcast(context.Background(), "room-42", []byte("announcement"))
	require.NoError(t, err)

	_, reply := readWS(t, conn)
	require.Equal(t, []byte("announcement"), reply)
}

func TestWSHandlerUnregistersOnDisconnect(t *testing.T) {
	system := newTestSystem(t)
	registry := gateway.NewRegistry(system, log.DiscardLogger)

	disconnected := make(chan *gateway.ConnInfo, 1)
	handler := gateway.NewWSHandler(registry,
		gateway.WithWSIDFunc(func(r *http.Request) string { return r.URL.Query().Get("id") }),
		gateway.WithWSOnDisconnect(func(info *gateway.ConnInfo) { disconnected <- info }),
	)

	server := httptest.NewServer(handler)
	defer server.Close()

	conn := dialWS(t, server, "/?id=bye", nil)
	awaitRegistered(t, registry, "bye", true)

	require.NoError(t, conn.Close(websocket.StatusNormalClosure, "done"))

	select {
	case info := <-disconnected:
		require.Equal(t, "bye", info.ID)
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for the disconnect callback")
	}
	awaitRegistered(t, registry, "bye", false)
}

// TestWSHandlerDisconnectClampsLongReason proves a Disconnect reason longer than a close
// frame can carry (123 bytes) still produces a clean 1008 close with a truncated reason,
// rather than coder/websocket refusing the frame and dropping to an abrupt 1006 with no code
// or reason.
func TestWSHandlerDisconnectClampsLongReason(t *testing.T) {
	system := newTestSystem(t)
	registry := gateway.NewRegistry(system, log.DiscardLogger)

	handler := gateway.NewWSHandler(registry,
		gateway.WithWSIDFunc(func(r *http.Request) string { return r.URL.Query().Get("id") }),
	)

	server := httptest.NewServer(handler)
	defer server.Close()

	conn := dialWS(t, server, "/?id=verbose", nil)
	awaitRegistered(t, registry, "verbose", true)

	longReason := strings.Repeat("x", 200)
	require.NoError(t, registry.Disconnect(context.Background(), "verbose", longReason))

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, _, err := conn.Read(ctx)
	require.Error(t, err)
	// A downgrade to CloseNow would surface as 1006 (abnormal) with no CloseError; a clean
	// close keeps the 1008 code the API promised.
	require.Equal(t, websocket.StatusPolicyViolation, websocket.CloseStatus(err))

	var ce websocket.CloseError
	require.ErrorAs(t, err, &ce)
	require.LessOrEqual(t, len(ce.Reason), 123, "the reason must be clamped to the close-frame limit")
	require.Equal(t, strings.Repeat("x", 123), ce.Reason)

	awaitRegistered(t, registry, "verbose", false)
}

// TestWSHandlerTeardownUsesEntryGuardedUnregister pins the fix for a naked, id-scoped
// Unregister: WSHandler now tears a connection's registration down through the entry-guarded
// ConnHandle RegisterHandle returned, not a bare Unregister(id). An id-scoped call resolves
// whatever is currently registered under an id at the moment it runs, so once a genuine
// cross-node takeover has installed a new registration under the same id, the evicted socket's
// own, asynchronously-triggered teardown must not be able to remove it.
//
// This needs a real cluster: a non-clustered actor.ActorSystem's Spawn silently hands back the
// existing PID for a name that is already running instead of erring, so a second, same-process
// Registry sharing that system would never drive spawnConnActor's ErrActorAlreadyExists retry
// loop - the takeover path that actually evicts the WS connection through its close hook. Only
// in cluster mode does that precondition check run, which is why this uses testkit.NewMultiNodes
// (the same technique registry_evict_race_test.go uses for the analogous Registry-level test)
// instead of the single-process newTestSystem every other test in this file uses.
func TestWSHandlerTeardownUsesEntryGuardedUnregister(t *testing.T) {
	ctx := context.Background()

	multi := testkit.NewMultiNodes(t, log.DiscardLogger, []actor.Actor{&clusterKindActor{}}, nil)
	multi.Start()
	t.Cleanup(multi.Stop)

	nodeA := multi.StartNode(ctx, "ws-entry-guard-node")
	nodeB := multi.StartNode(ctx, "ws-entry-guard-node")

	registry := gateway.NewRegistry(nodeA.ActorSystem(), log.DiscardLogger)
	takeoverRegistry := gateway.NewRegistry(nodeB.ActorSystem(), log.DiscardLogger)
	t.Cleanup(func() { _ = takeoverRegistry.Close(context.Background()) })

	handler := gateway.NewWSHandler(registry,
		gateway.WithWSIDFunc(func(r *http.Request) string { return r.URL.Query().Get("id") }),
	)

	server := httptest.NewServer(handler)
	defer server.Close()

	const id = "entry-guard-ws"
	conn := dialWS(t, server, "/?id="+id, nil)
	awaitRegistered(t, registry, id, true)

	// Let the cluster directory propagate node A's ownership of the actor name to node B before
	// the takeover, exactly as TestGatewayMultiNodesCrossNodeTakeover does: without this, node
	// B's Spawn can race ahead of the directory and succeed as if the name were unclaimed,
	// never driving the ErrActorAlreadyExists retry loop this test needs to exercise.
	time.Sleep(2 * time.Second)

	var newReceived atomic.Int64
	require.NoError(t, takeoverRegistry.Register(context.Background(), id, func([]byte) error {
		newReceived.Add(1)
		return nil
	}, gateway.WithReplaceExisting()))

	// The evicted socket learns about the takeover through a close frame and tears itself down
	// asynchronously from here.
	readCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, _, err := conn.Read(readCtx)
	require.Error(t, err)
	require.Equal(t, websocket.StatusPolicyViolation, websocket.CloseStatus(err))

	require.Eventually(t, func() bool { return !registry.Has(id) }, 5*time.Second, 20*time.Millisecond,
		"the evicted socket's own node must end up with no local entry for id")
	require.True(t, takeoverRegistry.Has(id), "the evicted socket's own teardown must not remove the takeover's registration")
	require.Equal(t, 0, registry.Len())
	require.Equal(t, 1, takeoverRegistry.Len())
	require.NoError(t, takeoverRegistry.SendToConnection(context.Background(), id, []byte("ping")))
	require.EqualValues(t, 1, newReceived.Load(), "delivery for id must reach the takeover's registration, not a resurrected old one")
}

// TestWSHandlerDrainWaitsForRegistryUnregisterBeforeReturning pins the fix for Drain returning
// as soon as every socket's close handshake finished, without waiting for the corresponding
// Registry teardown: a caller that proceeds to kill the process right after Drain returns must
// not be able to observe a connection's registration - its cluster-wide actor name and, with
// WithOwnerLease, its owner lease - still present.
func TestWSHandlerDrainWaitsForRegistryUnregisterBeforeReturning(t *testing.T) {
	system := newTestSystem(t)
	registry := gateway.NewRegistry(system, log.DiscardLogger)

	handler := gateway.NewWSHandler(registry,
		gateway.WithWSIDFunc(func(r *http.Request) string { return r.URL.Query().Get("id") }),
	)

	server := httptest.NewServer(handler)
	defer server.Close()

	conn := dialWS(t, server, "/?id=drain-sync", nil)
	awaitRegistered(t, registry, "drain-sync", true)

	// Read concurrently with Drain so the server's close handshake completes as soon as the
	// close frame is written instead of blocking on coder/websocket's 5s peer-response
	// timeout - that timeout is orthogonal to what this test checks, and letting it dominate
	// Drain's duration would not distinguish a synchronous unregister from an asynchronous one.
	readDone := make(chan struct{})
	go func() {
		defer close(readDone)
		readCtx, readCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer readCancel()
		for {
			if _, _, err := conn.Read(readCtx); err != nil {
				return
			}
		}
	}()

	handler.Drain()

	require.False(t, registry.Has("drain-sync"), "Drain must not return until the drained connection has actually unregistered, not merely closed its socket")

	select {
	case <-readDone:
	case <-time.After(5 * time.Second):
		t.Fatal("client read loop did not observe the close Drain triggered")
	}
}

func TestWSHandlerTakeoverEvictsPreviousConnection(t *testing.T) {
	system := newTestSystem(t)
	registry := gateway.NewRegistry(system, log.DiscardLogger)

	handler := gateway.NewWSHandler(registry,
		gateway.WithWSIDFunc(func(r *http.Request) string { return r.URL.Query().Get("id") }),
	)

	server := httptest.NewServer(handler)
	defer server.Close()

	first := dialWS(t, server, "/?id=phone", nil)
	awaitRegistered(t, registry, "phone", true)

	second := dialWS(t, server, "/?id=phone", nil)

	// The evicted socket learns about the takeover through a close frame instead of hanging
	// until TCP notices.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, _, err := first.Read(ctx)
	require.Error(t, err)
	require.Equal(t, websocket.StatusPolicyViolation, websocket.CloseStatus(err))

	// The takeover must leave the id registered and pointing at the new socket.
	require.True(t, registry.Has("phone"))
	require.Eventually(t, func() bool {
		return registry.SendToConnection(context.Background(), "phone", []byte("after-takeover")) == nil
	}, 5*time.Second, 10*time.Millisecond)

	_, reply := readWS(t, second)
	require.Equal(t, []byte("after-takeover"), reply)
	require.Equal(t, 1, registry.Len())
}

func TestWSHandlerPingKeepsIdleConnectionAlive(t *testing.T) {
	system := newTestSystem(t)
	registry := gateway.NewRegistry(system, log.DiscardLogger)

	handler := gateway.NewWSHandler(registry,
		gateway.WithWSIDFunc(func(r *http.Request) string { return r.URL.Query().Get("id") }),
		gateway.WithWSPingInterval(100*time.Millisecond),
		gateway.WithWSPongTimeout(2*time.Second),
	)

	server := httptest.NewServer(handler)
	defer server.Close()

	conn := dialWS(t, server, "/?id=idle", nil)
	awaitRegistered(t, registry, "idle", true)

	// The client's read loop is what answers the server's pings, so run one while the
	// connection stays idle at the application level.
	inbound := make(chan []byte, 1)
	readErr := make(chan error, 1)
	go func() {
		_, payload, err := conn.Read(context.Background())
		if err != nil {
			readErr <- err
			return
		}
		inbound <- payload
	}()

	select {
	case err := <-readErr:
		t.Fatalf("connection died while idle: %v", err)
	case <-time.After(time.Second):
	}
	require.True(t, registry.Has("idle"))

	require.NoError(t, registry.SendToConnection(context.Background(), "idle", []byte("still here")))
	select {
	case payload := <-inbound:
		require.Equal(t, []byte("still here"), payload)
	case err := <-readErr:
		t.Fatalf("connection died while idle: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for a delivery on the idle connection")
	}
}

func TestWSHandlerRejectsCrossOrigin(t *testing.T) {
	system := newTestSystem(t)
	registry := gateway.NewRegistry(system, log.DiscardLogger)

	handler := gateway.NewWSHandler(registry)

	server := httptest.NewServer(handler)
	defer server.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, resp, err := websocket.Dial(ctx, wsURL(t, server, "/"), &websocket.DialOptions{
		HTTPHeader: http.Header{"Origin": []string{"http://evil.example"}},
	})
	require.Error(t, err)
	require.NotNil(t, resp)
	require.Equal(t, http.StatusForbidden, resp.StatusCode)
	require.Equal(t, 0, registry.Len())
}

func TestWSHandlerAllowsConfiguredOrigin(t *testing.T) {
	system := newTestSystem(t)
	registry := gateway.NewRegistry(system, log.DiscardLogger)

	handler := gateway.NewWSHandler(registry,
		gateway.WithWSIDFunc(func(r *http.Request) string { return r.URL.Query().Get("id") }),
		gateway.WithWSOriginPatterns("app.example.com"),
	)

	server := httptest.NewServer(handler)
	defer server.Close()

	conn := dialWS(t, server, "/?id=cross", &websocket.DialOptions{
		HTTPHeader: http.Header{"Origin": []string{"https://app.example.com"}},
	})
	awaitRegistered(t, registry, "cross", true)

	require.NoError(t, registry.SendToConnection(context.Background(), "cross", []byte("allowed")))
	_, reply := readWS(t, conn)
	require.Equal(t, []byte("allowed"), reply)
}

func TestWSHandlerBackpressureDropKeepsConnection(t *testing.T) {
	system := newTestSystem(t)
	registry := gateway.NewRegistry(system, log.DiscardLogger)

	handler := gateway.NewWSHandler(registry,
		gateway.WithWSIDFunc(func(r *http.Request) string { return r.URL.Query().Get("id") }),
		gateway.WithWSSendBuffer(1),
		gateway.WithWSBackpressurePolicy(gateway.BackpressureDrop),
	)

	server := httptest.NewServer(handler)
	defer server.Close()

	// The client never reads, so the socket's send window closes, the writer goroutine blocks
	// and the outbound buffer fills.
	dialWS(t, server, "/?id=slow", nil)
	awaitRegistered(t, registry, "slow", true)

	require.Eventually(t, func() bool {
		err := registry.SendToConnection(context.Background(), "slow", make([]byte, 256*1024))
		return errors.Is(err, gateway.ErrBackpressure)
	}, 10*time.Second, 10*time.Millisecond)

	require.True(t, registry.Has("slow"))
}

func TestWSHandlerBackpressureCloseEvictsSlowConsumer(t *testing.T) {
	system := newTestSystem(t)
	registry := gateway.NewRegistry(system, log.DiscardLogger)

	handler := gateway.NewWSHandler(registry,
		gateway.WithWSIDFunc(func(r *http.Request) string { return r.URL.Query().Get("id") }),
		gateway.WithWSSendBuffer(1),
		// A jammed peer also jams the close handshake; a short write timeout lets the writer
		// give up so the eviction completes in test time rather than in socket time.
		gateway.WithWSWriteTimeout(500*time.Millisecond),
		gateway.WithWSBackpressurePolicy(gateway.BackpressureClose),
	)

	server := httptest.NewServer(handler)
	defer server.Close()

	dialWS(t, server, "/?id=slow", nil)
	awaitRegistered(t, registry, "slow", true)

	require.Eventually(t, func() bool {
		err := registry.SendToConnection(context.Background(), "slow", make([]byte, 256*1024))
		return errors.Is(err, gateway.ErrBackpressure) || errors.Is(err, gateway.ErrConnectionClosed) ||
			errors.Is(err, gateway.ErrConnectionNotFound)
	}, 10*time.Second, 10*time.Millisecond)

	awaitRegistered(t, registry, "slow", false)
}

func TestWSHandlerReadLimit(t *testing.T) {
	system := newTestSystem(t)
	registry := gateway.NewRegistry(system, log.DiscardLogger)

	received := make(chan []byte, 1)
	handler := gateway.NewWSHandler(registry,
		gateway.WithWSIDFunc(func(r *http.Request) string { return r.URL.Query().Get("id") }),
		gateway.WithWSReadLimit(64),
		gateway.WithWSOnMessage(func(_ context.Context, _ *gateway.ConnInfo, payload []byte) {
			received <- payload
		}),
	)

	server := httptest.NewServer(handler)
	defer server.Close()

	conn := dialWS(t, server, "/?id=fat", nil)
	awaitRegistered(t, registry, "fat", true)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, conn.Write(ctx, websocket.MessageText, make([]byte, 1024)))

	_, _, err := conn.Read(ctx)
	require.Error(t, err)
	require.Equal(t, websocket.StatusMessageTooBig, websocket.CloseStatus(err))

	require.Empty(t, received)
	awaitRegistered(t, registry, "fat", false)
}

func TestWSHandlerInboundRateLimitCloses(t *testing.T) {
	system := newTestSystem(t)
	registry := gateway.NewRegistry(system, log.DiscardLogger)

	handler := gateway.NewWSHandler(registry,
		gateway.WithWSIDFunc(func(r *http.Request) string { return r.URL.Query().Get("id") }),
		gateway.WithWSInboundRate(1, 1),
		gateway.WithWSBackpressurePolicy(gateway.BackpressureClose),
	)

	server := httptest.NewServer(handler)
	defer server.Close()

	conn := dialWS(t, server, "/?id=flood", nil)
	awaitRegistered(t, registry, "flood", true)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for range 20 {
		if err := conn.Write(ctx, websocket.MessageText, []byte("spam")); err != nil {
			break
		}
	}

	_, _, err := conn.Read(ctx)
	require.Error(t, err)
	require.Equal(t, websocket.StatusPolicyViolation, websocket.CloseStatus(err))
	awaitRegistered(t, registry, "flood", false)
}

func TestWSHandlerDrainSendsGoingAway(t *testing.T) {
	system := newTestSystem(t)
	registry := gateway.NewRegistry(system, log.DiscardLogger)

	handler := gateway.NewWSHandler(registry,
		gateway.WithWSIDFunc(func(r *http.Request) string { return r.URL.Query().Get("id") }),
	)

	server := httptest.NewServer(handler)
	defer server.Close()

	conn := dialWS(t, server, "/?id=deploy", nil)
	awaitRegistered(t, registry, "deploy", true)

	handler.Drain()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, _, err := conn.Read(ctx)
	require.Error(t, err)
	require.Equal(t, websocket.StatusGoingAway, websocket.CloseStatus(err))
	awaitRegistered(t, registry, "deploy", false)

	// A drained handler must not accept new sockets either.
	_, resp, err := websocket.Dial(ctx, wsURL(t, server, "/?id=late"), nil)
	require.Error(t, err)
	require.NotNil(t, resp)
	require.Equal(t, http.StatusServiceUnavailable, resp.StatusCode)
}

// TestWSHandlerCompressionNegotiated verifies that with WithWSCompression the handshake
// negotiates permessage-deflate with a client that offers it, and that payloads still round-trip
// once compression is active.
func TestWSHandlerCompressionNegotiated(t *testing.T) {
	system := newTestSystem(t)
	registry := gateway.NewRegistry(system, log.DiscardLogger)

	handler := gateway.NewWSHandler(registry,
		gateway.WithWSIDFunc(func(r *http.Request) string { return r.URL.Query().Get("id") }),
		gateway.WithWSCompression(websocket.CompressionContextTakeover),
	)

	server := httptest.NewServer(handler)
	defer server.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, resp, err := websocket.Dial(ctx, wsURL(t, server, "/?id=zip"), &websocket.DialOptions{
		CompressionMode: websocket.CompressionContextTakeover,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.CloseNow() })
	// Raise the client read limit above the default 32 KiB so the large delivery below is not
	// rejected client-side before it can be asserted.
	conn.SetReadLimit(1 << 20)
	require.Contains(t, resp.Header.Get("Sec-WebSocket-Extensions"), "permessage-deflate")

	awaitRegistered(t, registry, "zip", true)
	big := bytes.Repeat([]byte("compress-me "), 4096)
	require.NoError(t, registry.SendToConnection(context.Background(), "zip", big))
	_, reply := readWS(t, conn)
	require.Equal(t, big, reply)
}

// TestWSHandlerCompressionDisabledByDefault verifies an unconfigured handler negotiates no
// compression even when the client offers it, so the default stays zero-cost.
func TestWSHandlerCompressionDisabledByDefault(t *testing.T) {
	system := newTestSystem(t)
	registry := gateway.NewRegistry(system, log.DiscardLogger)

	handler := gateway.NewWSHandler(registry,
		gateway.WithWSIDFunc(func(r *http.Request) string { return r.URL.Query().Get("id") }),
	)

	server := httptest.NewServer(handler)
	defer server.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, resp, err := websocket.Dial(ctx, wsURL(t, server, "/?id=plain"), &websocket.DialOptions{
		CompressionMode: websocket.CompressionContextTakeover,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.CloseNow() })
	require.NotContains(t, resp.Header.Get("Sec-WebSocket-Extensions"), "permessage-deflate")
}

// TestWSHandlerReauthFailureKicks verifies a periodic reauthentication that starts failing
// closes the connection with status 1008 and unregisters it.
func TestWSHandlerReauthFailureKicks(t *testing.T) {
	system := newTestSystem(t)
	registry := gateway.NewRegistry(system, log.DiscardLogger)

	handler := gateway.NewWSHandler(registry,
		gateway.WithWSIDFunc(func(r *http.Request) string { return r.URL.Query().Get("id") }),
		gateway.WithWSReauth(100*time.Millisecond, func(*http.Request) (*gateway.ConnInfo, error) {
			return nil, errors.New("token expired")
		}),
	)

	server := httptest.NewServer(handler)
	defer server.Close()

	conn := dialWS(t, server, "/?id=revoked", nil)
	awaitRegistered(t, registry, "revoked", true)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, _, err := conn.Read(ctx)
	require.Error(t, err)
	require.Equal(t, websocket.StatusPolicyViolation, websocket.CloseStatus(err))
	awaitRegistered(t, registry, "revoked", false)
}

// TestWSHandlerReauthSuccessKeepsConnection verifies a reauthentication that keeps succeeding
// leaves the connection open and delivering across several cycles.
func TestWSHandlerReauthSuccessKeepsConnection(t *testing.T) {
	system := newTestSystem(t)
	registry := gateway.NewRegistry(system, log.DiscardLogger)

	var calls atomic.Int64
	handler := gateway.NewWSHandler(registry,
		gateway.WithWSIDFunc(func(r *http.Request) string { return r.URL.Query().Get("id") }),
		gateway.WithWSReauth(50*time.Millisecond, func(*http.Request) (*gateway.ConnInfo, error) {
			calls.Add(1)
			return &gateway.ConnInfo{}, nil
		}),
	)

	server := httptest.NewServer(handler)
	defer server.Close()

	conn := dialWS(t, server, "/?id=fresh", nil)
	awaitRegistered(t, registry, "fresh", true)

	require.Eventually(t, func() bool { return calls.Load() >= 3 }, 5*time.Second, 20*time.Millisecond)

	require.NoError(t, registry.SendToConnection(context.Background(), "fresh", []byte("alive")))
	_, reply := readWS(t, conn)
	require.Equal(t, []byte("alive"), reply)
	require.True(t, registry.Has("fresh"))
}

// TestWSHandlerGroupRateSharedBucket verifies that connections in the same group share one
// token bucket: after one connection spends the group's only token, another connection in the
// same group is rate limited (and, under BackpressureClose, evicted with 1008).
func TestWSHandlerGroupRateSharedBucket(t *testing.T) {
	// Grouped connections ride the cluster group bridge, which requires pub/sub.
	system := newTestSystem(t, actor.WithPubSub())
	registry := gateway.NewRegistry(system, log.DiscardLogger)

	received := make(chan string, 8)
	handler := gateway.NewWSHandler(registry,
		gateway.WithWSAuth(func(r *http.Request) (*gateway.ConnInfo, error) {
			return &gateway.ConnInfo{ID: r.URL.Query().Get("id"), Group: "team"}, nil
		}),
		// One token that effectively never refills during the test, so the bucket is empty for
		// the second connection once the first has spent it.
		gateway.WithWSGroupRate(0.01, 1),
		gateway.WithWSBackpressurePolicy(gateway.BackpressureClose),
		gateway.WithWSOnMessage(func(_ context.Context, info *gateway.ConnInfo, _ []byte) {
			received <- info.ID
		}),
	)

	server := httptest.NewServer(handler)
	defer server.Close()

	a := dialWS(t, server, "/?id=a", nil)
	awaitRegistered(t, registry, "a", true)
	b := dialWS(t, server, "/?id=b", nil)
	awaitRegistered(t, registry, "b", true)

	require.NoError(t, a.Write(context.Background(), websocket.MessageText, []byte("first")))
	select {
	case id := <-received:
		require.Equal(t, "a", id)
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for the first message to be accepted")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = b.Write(ctx, websocket.MessageText, []byte("second"))
	_, _, err := b.Read(ctx)
	require.Error(t, err)
	require.Equal(t, websocket.StatusPolicyViolation, websocket.CloseStatus(err))

	require.True(t, registry.Has("a"))
}

// TestWSHandlerDisconnectSends1008 verifies Registry.Disconnect force-closes a locally held
// socket with status 1008 and the caller-supplied reason, and that the socket unregisters.
func TestWSHandlerDisconnectSends1008(t *testing.T) {
	system := newTestSystem(t)
	registry := gateway.NewRegistry(system, log.DiscardLogger)

	handler := gateway.NewWSHandler(registry,
		gateway.WithWSIDFunc(func(r *http.Request) string { return r.URL.Query().Get("id") }),
	)

	server := httptest.NewServer(handler)
	defer server.Close()

	conn := dialWS(t, server, "/?id=kickme", nil)
	awaitRegistered(t, registry, "kickme", true)

	require.NoError(t, registry.Disconnect(context.Background(), "kickme", "policy update"))

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, _, err := conn.Read(ctx)
	require.Error(t, err)
	require.Equal(t, websocket.StatusPolicyViolation, websocket.CloseStatus(err))

	var ce websocket.CloseError
	require.True(t, errors.As(err, &ce))
	require.Equal(t, "policy update", ce.Reason)

	awaitRegistered(t, registry, "kickme", false)
}
