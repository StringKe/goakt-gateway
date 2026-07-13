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

	presenceredis "github.com/StringKe/goakt-gateway/presence/redis"
)

// ExampleNewPresence shares one view of who is online across every node: a member joined
// on one node with a lease is visible to Members on any other, and drops out when its
// lease lapses or it Leaves.
func ExampleNewPresence() {
	client := goredis.NewClient(&goredis.Options{Addr: "localhost:6379"})
	defer func() { _ = client.Close() }()

	presence := presenceredis.NewPresence(client)

	ctx := context.Background()
	if err := presence.Join(ctx, "user:42", "conn-abc", time.Minute); err != nil {
		log.Fatal(err)
	}
	members, err := presence.Members(ctx, "user:42")
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("online connections for user:42: %v", members)
}
