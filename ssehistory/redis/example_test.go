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

package redis_test

import (
	"context"
	"log"
	"time"

	goredis "github.com/redis/go-redis/v9"

	ssehistoryredis "github.com/StringKe/goakt-gateway/ssehistory/redis"
)

// ExampleNew shares an SSE replay buffer across a deployment so a client reconnecting its
// EventSource to any node can be replayed from Last-Event-ID. Since with a known id
// returns only the events after it.
func ExampleNew() {
	client := goredis.NewClient(&goredis.Options{Addr: "localhost:6379"})
	defer func() { _ = client.Close() }()

	history := ssehistoryredis.New(client,
		ssehistoryredis.WithPerConn(128),
		ssehistoryredis.WithTTL(time.Hour),
	)

	ctx := context.Background()
	if err := history.Append(ctx, "conn-abc", "e-1", []byte("hello")); err != nil {
		log.Fatal(err)
	}
	events, err := history.Since(ctx, "conn-abc", "e-1")
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("events after e-1: %d", len(events))
}
