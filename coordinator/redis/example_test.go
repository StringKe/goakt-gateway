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

	coordinatorredis "github.com/StringKe/goakt-gateway/coordinator/redis"
)

// ExampleNew arbitrates certificate issuance across a deployment: TryLock elects the one
// node that issues, and Put/Get share the result with the others.
func ExampleNew() {
	client := goredis.NewClient(&goredis.Options{Addr: "localhost:6379"})
	defer func() { _ = client.Close() }()

	coordinator := coordinatorredis.New(client, coordinatorredis.WithKeyPrefix("myapp:"))

	ctx := context.Background()
	unlock, err := coordinator.TryLock(ctx, "issue:example.com", 30*time.Second)
	if err != nil {
		// Another node already holds the lock and is issuing; wait and read the result.
		return
	}
	defer func() { _ = unlock(ctx) }()

	if err := coordinator.Put(ctx, "cert:example.com", []byte("...PEM..."), time.Hour); err != nil {
		log.Fatal(err)
	}
	if _, ok, err := coordinator.Get(ctx, "cert:example.com"); err != nil || !ok {
		log.Fatal(err)
	}
}
