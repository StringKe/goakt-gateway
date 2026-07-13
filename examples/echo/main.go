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
	"errors"
	"fmt"
	"html"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/tochemey/goakt/v4/actor"
	golog "github.com/tochemey/goakt/v4/log"

	gateway "github.com/StringKe/goakt-gateway"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

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

	wsHandler := gateway.NewWSHandler(registry,
		// WithWSAuth is the primary identity hook in the current API: it resolves the full
		// ConnInfo (id, group, topics, meta) from the upgrade request in one pass, instead of
		// the old id/topics-only callbacks that parsed the same query string three times.
		gateway.WithWSAuth(func(r *http.Request) (*gateway.ConnInfo, error) {
			return &gateway.ConnInfo{ID: r.URL.Query().Get("id")}, nil
		}),
		gateway.WithWSOnMessage(func(ctx context.Context, info *gateway.ConnInfo, payload []byte) {
			// Echo whatever the client sends straight back to it. Because this
			// connection is always local to this process, SendToConnection takes the
			// direct-write fast path with no actor/cluster involvement.
			if err := registry.SendToConnection(ctx, info.ID, payload); err != nil {
				log.Printf("echo to %q failed: %v", info.ID, err)
			}
		}),
		gateway.WithWSOnConnect(func(_ context.Context, info *gateway.ConnInfo, _ *http.Request) {
			log.Printf("connection %q joined", info.ID)
		}),
		gateway.WithWSOnDisconnect(func(info *gateway.ConnInfo) {
			log.Printf("connection %q left", info.ID)
		}),
	)
	mux.Handle("/ws", wsHandler)
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	})

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

		if _, err := fmt.Fprintf(w, "delivered %q to connection %q\n", html.EscapeString(msg), html.EscapeString(id)); err != nil {
			log.Printf("write delivery response: %v", err)
		}
	})

	server, err := gateway.NewServer(":8080", mux, gateway.WithDrainOnShutdown(wsHandler))
	if err != nil {
		log.Fatal(err)
	}

	errCh := make(chan error, 1)
	go func() { errCh <- server.ListenAndServe(ctx) }()

	select {
	case err := <-errCh:
		if !errors.Is(err, http.ErrServerClosed) {
			log.Fatal(err)
		}
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := server.Shutdown(shutdownCtx); err != nil {
			log.Printf("gateway-echo shutdown failed: %v", err)
		}
		if err := <-errCh; !errors.Is(err, http.ErrServerClosed) {
			log.Printf("gateway-echo stopped: %v", err)
		}
	}
}
