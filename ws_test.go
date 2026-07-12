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
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/net/websocket"

	"github.com/tochemey/goakt/v4/log"

	gateway "github.com/StringKe/goakt-gateway"
)

func wsURL(t *testing.T, server *httptest.Server, path string) string {
	t.Helper()
	return "ws" + strings.TrimPrefix(server.URL, "http") + path
}

func TestWSHandlerEcho(t *testing.T) {
	system := newTestSystem(t)
	registry := gateway.NewRegistry(system, log.DiscardLogger)

	handler := gateway.NewWSHandler(registry,
		gateway.WithWSIDFunc(func(r *http.Request) string { return r.URL.Query().Get("id") }),
		gateway.WithWSOnMessage(func(ctx context.Context, id string, payload []byte) {
			_ = registry.SendToConnection(ctx, id, payload)
		}),
	)

	server := httptest.NewServer(handler)
	defer server.Close()

	ws, err := websocket.Dial(wsURL(t, server, "/?id=echo-1"), "", server.URL)
	require.NoError(t, err)
	defer func() { _ = ws.Close() }()

	require.NoError(t, websocket.Message.Send(ws, []byte("ping")))

	var reply []byte
	require.NoError(t, ws.SetReadDeadline(time.Now().Add(5*time.Second)))
	require.NoError(t, websocket.Message.Receive(ws, &reply))
	require.Equal(t, []byte("ping"), reply)
}

func TestWSHandlerAuthRejected(t *testing.T) {
	system := newTestSystem(t)
	registry := gateway.NewRegistry(system, log.DiscardLogger)

	handler := gateway.NewWSHandler(registry,
		gateway.WithWSAuthFunc(func(*http.Request) error {
			return errors.New("no token")
		}),
	)

	server := httptest.NewServer(handler)
	defer server.Close()

	_, err := websocket.Dial(wsURL(t, server, "/"), "", server.URL)
	require.Error(t, err)
}

func TestWSHandlerTopicBroadcast(t *testing.T) {
	system := newTestSystem(t)
	registry := gateway.NewRegistry(system, log.DiscardLogger)

	handler := gateway.NewWSHandler(registry,
		gateway.WithWSIDFunc(func(r *http.Request) string { return r.URL.Query().Get("id") }),
		gateway.WithWSTopics(func(*http.Request) []string { return []string{"room-42"} }),
	)

	server := httptest.NewServer(handler)
	defer server.Close()

	ws, err := websocket.Dial(wsURL(t, server, "/?id=room-member"), "", server.URL)
	require.NoError(t, err)
	defer func() { _ = ws.Close() }()

	time.Sleep(200 * time.Millisecond)
	require.True(t, registry.Has("room-member"))

	require.NoError(t, registry.Broadcast(context.Background(), "room-42", []byte("announcement")))

	var reply []byte
	require.NoError(t, ws.SetReadDeadline(time.Now().Add(5*time.Second)))
	require.NoError(t, websocket.Message.Receive(ws, &reply))
	require.Equal(t, []byte("announcement"), reply)
}

func TestWSHandlerUnregistersOnDisconnect(t *testing.T) {
	system := newTestSystem(t)
	registry := gateway.NewRegistry(system, log.DiscardLogger)

	disconnected := make(chan string, 1)
	handler := gateway.NewWSHandler(registry,
		gateway.WithWSIDFunc(func(r *http.Request) string { return r.URL.Query().Get("id") }),
		gateway.WithWSOnDisconnect(func(id string) { disconnected <- id }),
	)

	server := httptest.NewServer(handler)
	defer server.Close()

	ws, err := websocket.Dial(wsURL(t, server, "/?id=bye"), "", server.URL)
	require.NoError(t, err)

	time.Sleep(200 * time.Millisecond)
	require.True(t, registry.Has("bye"))

	require.NoError(t, ws.Close())

	select {
	case id := <-disconnected:
		require.Equal(t, "bye", id)
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for disconnect callback")
	}
	require.False(t, registry.Has("bye"))
}
