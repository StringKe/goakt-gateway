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

// dataInfix and lockInfix place Put/Get values and TryLock locks in disjoint key
// namespaces under the same configured prefix. They differ in their first byte and are
// prepended before the caller's key, never appended after it, so no user-supplied key can
// reproduce the other namespace: a Put on any key can never overwrite a lock's token or
// clear its ttl, and acquiring a lock can never evict a stored value. A trailing suffix
// (e.g. key+":lock") does not have this property - Put("x:lock") would collide with the
// lock of TryLock("x") - which is why the discriminator is a leading infix instead.
const (
	dataInfix = "d:"
	lockInfix = "l:"
)

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

// casScript performs an atomic compare-and-swap in the "d:" data namespace: it GETs the
// current value, checks it against the caller's expectation, and only then SETs the new
// value - all inside one Redis command so no other client can observe or write between the
// compare and the swap. ARGV[1] distinguishes "expected == nil" (require absent, ARGV[1]
// == "0") from "expected == a value" (ARGV[1] == "1", compare against ARGV[2]) because Lua
// cannot otherwise tell an empty-string expectation from a nil one. A missing Redis key
// surfaces to Lua as the boolean false, never as an empty string, so it can never be
// confused with a stored empty value.
var casScript = goredis.NewScript(`
local current = redis.call("GET", KEYS[1])
if ARGV[1] == "1" then
	if current == false or current ~= ARGV[2] then
		return 0
	end
else
	if current ~= false then
		return 0
	end
end
if tonumber(ARGV[4]) > 0 then
	redis.call("SET", KEYS[1], ARGV[3], "PX", ARGV[4])
else
	redis.call("SET", KEYS[1], ARGV[3])
end
return 1
`)

// Coordinator is a gateway.Coordinator backed by a Redis client: Put/Get use plain
// SET/GET with the given TTL, TryLock uses SET NX PX for acquisition and the Lua script
// above for release, and CompareAndSwap uses the Lua script above for an atomic
// read-compare-write - real mutual exclusion and CAS primitives, unlike best-effort
// distributed locking.
//
// TryLock's mutual exclusion, and CompareAndSwap's atomicity, hold against every client
// talking to the same Redis primary. They do not survive a Redis failover: SET NX PX (and
// this CAS script) only excludes other callers on the primary that accepted the write: if
// that primary fails before the write reaches a replica and a replica is promoted, the new
// primary has no record of it and a second caller can acquire the same lock or win the same
// CAS again. This is a fundamental limitation of single-primary Redis with asynchronous
// replication, not something a client-side Coordinator can close: a deployment that needs a
// lock/CAS to survive failover without any repeat-grant window needs a consensus-backed
// store (e.g. Redlock across independent Redis primaries, or etcd/ZooKeeper), which is
// outside the scope of this package.
type Coordinator struct {
	client goredis.UniversalClient
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
// *redis.ClusterClient, *redis.Ring, or any other goredis.UniversalClient implementation.
func New(client goredis.UniversalClient, opts ...Option) *Coordinator {
	c := &Coordinator{client: client}
	for _, opt := range opts {
		opt(c)
	}
	return c
}

// enforce compilation error
var (
	_ gateway.Coordinator    = (*Coordinator)(nil)
	_ gateway.CASCoordinator = (*Coordinator)(nil)
)

func (c *Coordinator) dataKey(key string) string {
	return c.prefix + dataInfix + key
}

func (c *Coordinator) lockKey(key string) string {
	return c.prefix + lockInfix + key
}

// Get implements gateway.Coordinator.
func (c *Coordinator) Get(ctx context.Context, key string) ([]byte, bool, error) {
	value, err := c.client.Get(ctx, c.dataKey(key)).Bytes()
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
	return c.client.Set(ctx, c.dataKey(key), value, max(ttl, 0)).Err()
}

// CompareAndSwap implements gateway.CASCoordinator, in the same "d:" data namespace Get/Put
// use, via casScript.
func (c *Coordinator) CompareAndSwap(ctx context.Context, key string, expected, newValue []byte, ttl time.Duration) (bool, error) {
	hasExpected := "0"
	var expectedArg any = ""
	if expected != nil {
		hasExpected = "1"
		expectedArg = expected
	}

	// ttl.Milliseconds() rounds a sub-millisecond positive ttl down to 0, which the script
	// reads as "never expires" (tonumber(ARGV[4]) > 0 is false) - the opposite of what a
	// caller who passed ttl > 0 asked for. Floor it at 1ms instead of letting that happen.
	ttlMs := int64(0)
	if ttl > 0 {
		ttlMs = max(ttl.Milliseconds(), 1)
	}

	res, err := casScript.Run(ctx, c.client, []string{c.dataKey(key)}, hasExpected, expectedArg, newValue, ttlMs).Int()
	if err != nil {
		return false, err
	}
	return res == 1, nil
}

// TryLock implements gateway.Coordinator.
func (c *Coordinator) TryLock(ctx context.Context, key string, ttl time.Duration) (func(context.Context) error, error) {
	lockKey := c.lockKey(key)
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
