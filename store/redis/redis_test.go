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

// This suite requires a real Redis instance and is skipped unless TEST_REDIS_ADDR is
// set, so CI does not need a Redis daemon by default:
//
//	TEST_REDIS_ADDR=localhost:6379 go test ./store/redis/...
package redis_test

import (
	"context"
	"fmt"
	"os"
	"sync/atomic"
	"testing"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"

	gateway "github.com/StringKe/goakt-gateway"
	"github.com/StringKe/goakt-gateway/store/conformance"
	gatewayredis "github.com/StringKe/goakt-gateway/store/redis"
)

func TestRedisCertStoreConformance(t *testing.T) {
	addr := os.Getenv("TEST_REDIS_ADDR")
	if addr == "" {
		t.Skip("TEST_REDIS_ADDR not set; skipping Redis-backed CertStore conformance suite")
	}

	client := redis.NewClient(&redis.Options{Addr: addr})
	t.Cleanup(func() { _ = client.Close() })
	require.NoError(t, client.Ping(context.Background()).Err(), "failed to reach Redis at TEST_REDIS_ADDR")

	var counter atomic.Int64
	conformance.Run(t, func() gateway.CertStore {
		// each subtest gets its own key namespace so state never leaks between them
		// despite sharing one Redis instance.
		prefix := fmt.Sprintf("goakt-gateway-test-%d:", counter.Add(1))
		return gatewayredis.New(client, gatewayredis.WithKeyPrefix(prefix))
	})
}
