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

// Package redis provides a Redis-backed gateway.Coordinator, for sharing certificate
// issuance arbitration and distribution across every process in a deployment. It is a
// separate package specifically so that importing the root gateway package never pulls
// in github.com/redis/go-redis/v9 for applications that do not want it (e.g. those using
// the default gateway.MemoryCoordinator, or a different Coordinator entirely).
package redis

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	goredis "github.com/redis/go-redis/v9"

	gateway "github.com/StringKe/goakt-gateway"
)

// lockSuffix separates a lock's key from the value key of the same name, so Put/Get and
// TryLock never collide even when called with identical key strings.
const lockSuffix = ":lock"

// unlockScript performs a compare-and-delete: it only deletes the lock key if it still
// holds the exact token this Coordinator's TryLock call set, so an unlock call can never
// release a lock some other caller has since acquired after this one's ttl elapsed.
var unlockScript = goredis.NewScript(`
if redis.call("GET", KEYS[1]) == ARGV[1] then
	return redis.call("DEL", KEYS[1])
else
	return 0
end
`)

// Coordinator is a gateway.Coordinator backed by a Redis client: Put/Get use plain
// SET/GET with the given TTL, and TryLock uses SET NX PX for acquisition and the Lua
// script above for release - a real mutual exclusion primitive, unlike a best-effort
// distributed lock.
type Coordinator struct {
	client goredis.Cmdable
	prefix string
}

// Option configures a Coordinator created with New.
type Option func(*Coordinator)

// WithKeyPrefix namespaces every key this Coordinator reads or writes, so multiple
// gateway deployments (or unrelated applications) can share one Redis instance/database
// without colliding. Defaults to no prefix.
func WithKeyPrefix(prefix string) Option {
	return func(c *Coordinator) { c.prefix = prefix }
}

// New creates a Coordinator backed by client. client may be a *redis.Client,
// *redis.ClusterClient, or any other goredis.Cmdable implementation.
func New(client goredis.Cmdable, opts ...Option) *Coordinator {
	c := &Coordinator{client: client}
	for _, opt := range opts {
		opt(c)
	}
	return c
}

// enforce compilation error
var _ gateway.Coordinator = (*Coordinator)(nil)

func (c *Coordinator) key(key string) string {
	return c.prefix + key
}

// Get implements gateway.Coordinator.
func (c *Coordinator) Get(ctx context.Context, key string) ([]byte, bool, error) {
	value, err := c.client.Get(ctx, c.key(key)).Bytes()
	if errors.Is(err, goredis.Nil) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	return value, true, nil
}

// Put implements gateway.Coordinator.
func (c *Coordinator) Put(ctx context.Context, key string, value []byte, ttl time.Duration) error {
	var expiration time.Duration
	if ttl > 0 {
		expiration = ttl
	}
	return c.client.Set(ctx, c.key(key), value, expiration).Err()
}

// TryLock implements gateway.Coordinator.
func (c *Coordinator) TryLock(ctx context.Context, key string, ttl time.Duration) (func(context.Context) error, error) {
	lockKey := c.key(key) + lockSuffix
	token := uuid.NewString()

	acquired, err := c.client.SetNX(ctx, lockKey, token, ttl).Result()
	if err != nil {
		return nil, err
	}
	if !acquired {
		return nil, gateway.ErrLockNotAcquired
	}

	return func(unlockCtx context.Context) error {
		return unlockScript.Run(unlockCtx, c.client, []string{lockKey}, token).Err()
	}, nil
}
