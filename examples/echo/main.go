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

// Command echo is a minimal, runnable demonstration of the gateway package:
//
//   - A WebSocket echo endpoint (/ws?id=<connection-id>) registered in a gateway.Registry.
//   - An ordinary HTTP handler (/send?id=<connection-id>&msg=<text>) that delivers a
//     message to a registered connection via Registry.SendToConnection - the "HTTP
//     handler talks to a websocket connection" pattern that motivates the whole package.
//
// This sample runs a single, non-clustered actor system so it has no external
// dependencies (no discovery backend to stand up). Registry.SendToConnection's local
// fast path (direct socket write, no actor/cluster machinery) is exactly what runs here
// regardless of cluster size; see README.md for how to extend this sample into a real
// multi-node one.
package main

import (
	"context"
	"fmt"
	"html"
	"log"
	"net/http"

	"github.com/tochemey/goakt/v4/actor"
	golog "github.com/tochemey/goakt/v4/log"

	gateway "github.com/StringKe/goakt-gateway"
)

func main() {
	ctx := context.Background()

	system, err := actor.NewActorSystem("gateway-echo", actor.WithLogger(golog.DiscardLogger))
	if err != nil {
		log.Fatal(err)
	}
	if err := system.Start(ctx); err != nil {
		log.Fatal(err)
	}
	defer func() { _ = system.Stop(ctx) }()

	registry := gateway.NewRegistry(system, golog.DiscardLogger)

	mux := http.NewServeMux()

	mux.Handle("/ws", gateway.NewWSHandler(registry,
		gateway.WithWSIDFunc(func(r *http.Request) string { return r.URL.Query().Get("id") }),
		gateway.WithWSOnMessage(func(ctx context.Context, id string, payload []byte) {
			// Echo whatever the client sends straight back to it. Because this
			// connection is always local to this process, SendToConnection takes the
			// direct-write fast path with no actor/cluster involvement.
			if err := registry.SendToConnection(ctx, id, payload); err != nil {
				log.Printf("echo to %q failed: %v", id, err)
			}
		}),
		gateway.WithWSOnConnect(func(_ context.Context, id string, _ *http.Request) {
			log.Printf("connection %q joined", id)
		}),
		gateway.WithWSOnDisconnect(func(id string) {
			log.Printf("connection %q left", id)
		}),
	))

	mux.HandleFunc("/send", func(w http.ResponseWriter, r *http.Request) {
		id := r.URL.Query().Get("id")
		msg := r.URL.Query().Get("msg")
		if id == "" || msg == "" {
			http.Error(w, "id and msg query parameters are required", http.StatusBadRequest)
			return
		}

		if err := registry.SendToConnection(r.Context(), id, []byte(msg)); err != nil {
			http.Error(w, err.Error(), http.StatusNotFound)
			return
		}

		fmt.Fprintf(w, "delivered %q to connection %q\n", html.EscapeString(msg), html.EscapeString(id))
	})

	server, err := gateway.NewServer("127.0.0.1:8080", mux)
	if err != nil {
		log.Fatal(err)
	}

	log.Println("gateway-echo listening on http://127.0.0.1:8080 (ws: /ws?id=alice, send: /send?id=alice&msg=hi)")
	log.Fatal(server.ListenAndServe(ctx))
}
