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
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/tochemey/goakt/v4/log"

	gateway "github.com/StringKe/goakt-gateway"
)

func TestSSEHandlerDelivery(t *testing.T) {
	system := newTestSystem(t)
	registry := gateway.NewRegistry(system, log.DiscardLogger)

	handler := gateway.NewSSEHandler(registry,
		gateway.WithSSEIDFunc(func(r *http.Request) string { return r.URL.Query().Get("id") }),
		gateway.WithSSEKeepAlive(time.Hour),
	)

	server := httptest.NewServer(handler)
	defer server.Close()

	req, err := http.NewRequest(http.MethodGet, server.URL+"/?id=sse-1", nil)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	req = req.WithContext(ctx)

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	require.Equal(t, "text/event-stream", resp.Header.Get("Content-Type"))

	time.Sleep(200 * time.Millisecond)
	require.True(t, registry.Has("sse-1"))

	require.NoError(t, registry.SendToConnection(context.Background(), "sse-1", []byte("hello")))

	reader := bufio.NewReader(resp.Body)
	line, err := readSSEDataLine(reader)
	require.NoError(t, err)
	require.Equal(t, "hello", line)
}

func TestSSEHandlerAuthRejected(t *testing.T) {
	system := newTestSystem(t)
	registry := gateway.NewRegistry(system, log.DiscardLogger)

	handler := gateway.NewSSEHandler(registry,
		gateway.WithSSEAuthFunc(func(*http.Request) error {
			return errors.New("no token")
		}),
	)

	server := httptest.NewServer(handler)
	defer server.Close()

	resp, err := http.Get(server.URL + "/")
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	require.Equal(t, http.StatusForbidden, resp.StatusCode)
}

// TestSSEHandlerDrainTerminatesStream verifies Drain unblocks open streams promptly (so
// a graceful shutdown is not held hostage by long-lived SSE requests) and that new
// requests after Drain fail fast with 503.
func TestSSEHandlerDrainTerminatesStream(t *testing.T) {
	system := newTestSystem(t)
	registry := gateway.NewRegistry(system, log.DiscardLogger)

	handler := gateway.NewSSEHandler(registry,
		gateway.WithSSEIDFunc(func(r *http.Request) string { return r.URL.Query().Get("id") }),
		gateway.WithSSEKeepAlive(time.Hour),
	)

	server := httptest.NewServer(handler)
	defer server.Close()

	resp, err := http.Get(server.URL + "/?id=sse-drain-1")
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	time.Sleep(200 * time.Millisecond)
	require.True(t, registry.Has("sse-drain-1"))

	handler.Drain()

	// the streaming loop returns, the server ends the chunked response, and the
	// client's blocked read observes EOF instead of waiting on keepalives.
	readErrCh := make(chan error, 1)
	go func() {
		buf := make([]byte, 64)
		for {
			if _, readErr := resp.Body.Read(buf); readErr != nil {
				readErrCh <- readErr
				return
			}
		}
	}()
	select {
	case readErr := <-readErrCh:
		require.Error(t, readErr)
	case <-time.After(5 * time.Second):
		t.Fatal("stream was not terminated by Drain")
	}

	require.Eventually(t, func() bool {
		return !registry.Has("sse-drain-1")
	}, 3*time.Second, 50*time.Millisecond)

	// new streams are refused while draining.
	resp2, err := http.Get(server.URL + "/?id=sse-drain-2")
	require.NoError(t, err)
	defer func() { _ = resp2.Body.Close() }()
	require.Equal(t, http.StatusServiceUnavailable, resp2.StatusCode)
}

// TestSSEHandlerDisconnectOnBodyClose verifies that a client abandoning the response
// stream (resp.Body.Close(), without the request context ever being explicitly canceled)
// is observed through r.Context().Done() same as an explicit cancellation, triggering the
// registry Unregister/onDisconnect cleanup path.
func TestSSEHandlerDisconnectOnBodyClose(t *testing.T) {
	system := newTestSystem(t)
	registry := gateway.NewRegistry(system, log.DiscardLogger)

	disconnected := make(chan string, 1)
	handler := gateway.NewSSEHandler(registry,
		gateway.WithSSEIDFunc(func(r *http.Request) string { return r.URL.Query().Get("id") }),
		gateway.WithSSEOnDisconnect(func(id string) { disconnected <- id }),
		gateway.WithSSEKeepAlive(time.Hour),
	)

	server := httptest.NewServer(handler)
	defer server.Close()

	resp, err := http.Get(server.URL + "/?id=sse-close")
	require.NoError(t, err)

	time.Sleep(200 * time.Millisecond)
	require.True(t, registry.Has("sse-close"))

	require.NoError(t, resp.Body.Close())

	select {
	case id := <-disconnected:
		require.Equal(t, "sse-close", id)
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for disconnect callback after client body close")
	}
	require.False(t, registry.Has("sse-close"))
}

// readSSEDataLine reads lines until it finds one prefixed with "data: ", and returns its
// content.
func readSSEDataLine(reader *bufio.Reader) (string, error) {
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			return "", err
		}
		line = strings.TrimRight(line, "\r\n")
		if strings.HasPrefix(line, "data: ") {
			return strings.TrimPrefix(line, "data: "), nil
		}
	}
}
