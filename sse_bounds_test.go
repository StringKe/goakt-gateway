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
	"bufio"
	"bytes"
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/tochemey/goakt/v4/log"

	gateway "github.com/StringKe/goakt-gateway"
)

// TestSSEHandlerSlowClientDrainTerminates proves a slow reader cannot pin the streaming loop:
// once a write blocks on a client that stopped reading, the write deadline fires and Drain
// still tears the stream down promptly. Without a per-write deadline the writer goroutine
// would sit in Write forever and Drain could never unregister the connection.
func TestSSEHandlerSlowClientDrainTerminates(t *testing.T) {
	system := newTestSystem(t)
	registry := gateway.NewRegistry(system, log.DiscardLogger)

	handler := gateway.NewSSEHandler(registry,
		gateway.WithSSEIDFunc(func(r *http.Request) string { return r.URL.Query().Get("id") }),
		gateway.WithSSEKeepAlive(time.Hour),
		gateway.WithSSEWriteTimeout(200*time.Millisecond),
		gateway.WithSSESendBuffer(8),
	)

	server := httptest.NewServer(handler)
	defer server.Close()

	// A raw connection that reads the response head once and then stops reading, so the
	// server's socket send buffer fills and the next write blocks.
	conn, err := net.Dial("tcp", server.Listener.Addr().String())
	require.NoError(t, err)
	defer func() { _ = conn.Close() }()

	_, err = conn.Write([]byte("GET /?id=sse-slow HTTP/1.1\r\nHost: example\r\n\r\n"))
	require.NoError(t, err)

	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	head := make([]byte, 4096)
	_, err = conn.Read(head)
	require.NoError(t, err)

	require.Eventually(t, func() bool { return registry.Has("sse-slow") }, 3*time.Second, 20*time.Millisecond)

	// Flood payloads far larger than any loopback socket buffer into the unread socket, so a
	// write inside the streaming loop is guaranteed to block rather than being absorbed.
	big := bytes.Repeat([]byte("x"), 8<<20)
	for i := 0; i < 8; i++ {
		_ = registry.SendToConnection(context.Background(), "sse-slow", big)
	}

	handler.Drain()

	require.Eventually(t, func() bool {
		return !registry.Has("sse-slow")
	}, 3*time.Second, 50*time.Millisecond)
}

// TestSSEHandlerNegativeSendBufferDoesNotPanic guards the buffer validation: a negative buffer
// size must be clamped, not fed straight into make(chan []byte, size), which panics. The stream
// must open and deliver normally.
func TestSSEHandlerNegativeSendBufferDoesNotPanic(t *testing.T) {
	system := newTestSystem(t)
	registry := gateway.NewRegistry(system, log.DiscardLogger)

	handler := gateway.NewSSEHandler(registry,
		gateway.WithSSEIDFunc(func(r *http.Request) string { return r.URL.Query().Get("id") }),
		gateway.WithSSEKeepAlive(time.Hour),
		gateway.WithSSESendBuffer(-5),
	)

	server := httptest.NewServer(handler)
	defer server.Close()

	resp, cancel := openSSEStream(t, server.URL+"/?id=sse-negbuf", "")
	defer cancel()
	defer func() { _ = resp.Body.Close() }()

	require.Eventually(t, func() bool { return registry.Has("sse-negbuf") }, 3*time.Second, 20*time.Millisecond)
	require.NoError(t, registry.SendToConnection(context.Background(), "sse-negbuf", []byte("hello")))

	reader := bufio.NewReader(resp.Body)
	frame, err := readSSEFrame(reader)
	require.NoError(t, err)
	require.Equal(t, "hello", frame.data)
}
