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
	"fmt"
	"net/http"
	"net/http/httptest"
	"time"

	"github.com/tochemey/goakt/v4/actor"
	"github.com/tochemey/goakt/v4/log"

	gateway "github.com/StringKe/goakt-gateway"
)

// ExampleSSEHandler wires Last-Event-ID replay: a client whose stream drops reconnects with the
// id of the last event it saw, and the handler replays everything after it (backed by an
// SSEHistory) before the live stream resumes. This is exactly the reconnect a browser's
// EventSource performs on its own.
func ExampleSSEHandler() {
	system, err := actor.NewActorSystem("sse-example", actor.WithLogger(log.DiscardLogger))
	if err != nil {
		fmt.Println(err)
		return
	}
	if err := system.Start(context.Background()); err != nil {
		fmt.Println(err)
		return
	}
	defer func() { _ = system.Stop(context.Background()) }()

	registry := gateway.NewRegistry(system, log.DiscardLogger)
	history := gateway.NewMemorySSEHistory(16)

	handler := gateway.NewSSEHandler(registry,
		// The connection id is the replay key; bind it to an authenticated principal in real code.
		gateway.WithSSEIDFunc(func(r *http.Request) string { return r.URL.Query().Get("id") }),
		// A history is what turns an EventSource reconnect into a gap-free resume.
		gateway.WithSSEHistory(history),
		gateway.WithSSERetry(0),
		gateway.WithSSEKeepAlive(time.Hour),
	)

	server := httptest.NewServer(handler)
	defer server.Close()

	// First connection: receive two live events so the history has something to replay.
	first := openExampleStream(server.URL+"/?id=user-42", "")
	defer func() { _ = first.Body.Close() }()

	for !registry.Has("user-42") {
		time.Sleep(5 * time.Millisecond)
	}
	_ = registry.SendToConnection(context.Background(), "user-42", []byte("event-1"))
	_ = registry.SendToConnection(context.Background(), "user-42", []byte("event-2"))

	// Draining both frames off the wire guarantees the history has recorded them: the writer
	// appends to the history before it writes each event.
	reader := bufio.NewReader(first.Body)
	for range 2 {
		if _, err := readSSEFrame(reader); err != nil {
			fmt.Println(err)
			return
		}
	}

	// Reconnect echoing the id of the first event. The handler replays everything after it -
	// here just event-2 - as the takeover stream opens.
	second := openExampleStream(server.URL+"/?id=user-42", "user-42-1")
	defer func() { _ = second.Body.Close() }()

	frame, err := readSSEFrame(bufio.NewReader(second.Body))
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Printf("replayed %s as %s\n", frame.data, frame.id)

	// Output:
	// replayed event-2 as user-42-2
}

// openExampleStream opens a streaming SSE request, sending lastEventID as the header a browser's
// EventSource sends on reconnect when it is non-empty.
func openExampleStream(url, lastEventID string) *http.Response {
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		panic(err)
	}
	if lastEventID != "" {
		req.Header.Set("Last-Event-ID", lastEventID)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		panic(err)
	}
	return resp
}
