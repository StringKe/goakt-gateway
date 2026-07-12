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

// Package conformance is a shared test suite every gateway.Coordinator implementation
// must pass, so gateway.MemoryCoordinator and coordinator/redis.Coordinator (and any
// third-party implementation) are held to the exact same contract.
package conformance

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	gateway "github.com/StringKe/goakt-gateway"
)

// Run exercises factory() against the gateway.Coordinator contract. factory must return
// a fresh, empty Coordinator; Run calls it once per subtest so implementations backed by
// a shared external service (e.g. Redis) do not see state leak between subtests as long
// as factory picks a fresh key namespace or database per call.
func Run(t *testing.T, factory func(t *testing.T) gateway.Coordinator) {
	t.Helper()

	t.Run("Get on an absent key reports ok=false and no error", func(t *testing.T) {
		c := factory(t)
		value, ok, err := c.Get(context.Background(), "absent")
		require.NoError(t, err)
		require.False(t, ok)
		require.Nil(t, value)
	})

	t.Run("Put then Get round-trips the value", func(t *testing.T) {
		c := factory(t)
		ctx := context.Background()
		require.NoError(t, c.Put(ctx, "k", []byte("v"), 0))

		value, ok, err := c.Get(ctx, "k")
		require.NoError(t, err)
		require.True(t, ok)
		require.Equal(t, []byte("v"), value)
	})

	t.Run("Put overwrites a previous value for the same key", func(t *testing.T) {
		c := factory(t)
		ctx := context.Background()
		require.NoError(t, c.Put(ctx, "k", []byte("first"), 0))
		require.NoError(t, c.Put(ctx, "k", []byte("second"), 0))

		value, ok, err := c.Get(ctx, "k")
		require.NoError(t, err)
		require.True(t, ok)
		require.Equal(t, []byte("second"), value)
	})

	t.Run("a positive ttl expires the value", func(t *testing.T) {
		c := factory(t)
		ctx := context.Background()
		require.NoError(t, c.Put(ctx, "k", []byte("v"), 50*time.Millisecond))

		_, ok, err := c.Get(ctx, "k")
		require.NoError(t, err)
		require.True(t, ok)

		require.Eventually(t, func() bool {
			_, ok, err := c.Get(ctx, "k")
			return err == nil && !ok
		}, 3*time.Second, 20*time.Millisecond, "value must expire once its ttl elapses")
	})

	t.Run("TryLock excludes a second concurrent holder of the same key", func(t *testing.T) {
		c := factory(t)
		ctx := context.Background()

		unlock, err := c.TryLock(ctx, "lock", 10*time.Second)
		require.NoError(t, err)
		require.NotNil(t, unlock)

		_, err = c.TryLock(ctx, "lock", 10*time.Second)
		require.ErrorIs(t, err, gateway.ErrLockNotAcquired)

		require.NoError(t, unlock(ctx))
	})

	t.Run("unlock releases the lock for a subsequent caller", func(t *testing.T) {
		c := factory(t)
		ctx := context.Background()

		unlock, err := c.TryLock(ctx, "lock", 10*time.Second)
		require.NoError(t, err)
		require.NoError(t, unlock(ctx))

		unlock2, err := c.TryLock(ctx, "lock", 10*time.Second)
		require.NoError(t, err)
		require.NoError(t, unlock2(ctx))
	})

	t.Run("a lock expires on its own once ttl elapses", func(t *testing.T) {
		c := factory(t)
		ctx := context.Background()

		_, err := c.TryLock(ctx, "lock", 50*time.Millisecond)
		require.NoError(t, err)

		require.Eventually(t, func() bool {
			unlock, err := c.TryLock(ctx, "lock", 10*time.Second)
			if err != nil {
				return false
			}
			_ = unlock(ctx)
			return true
		}, 3*time.Second, 20*time.Millisecond, "a lock must become acquirable again once its ttl elapses")
	})

	t.Run("unlock is idempotent and safe after expiry", func(t *testing.T) {
		c := factory(t)
		ctx := context.Background()

		unlock, err := c.TryLock(ctx, "lock", 10*time.Second)
		require.NoError(t, err)
		require.NoError(t, unlock(ctx))
		require.NoError(t, unlock(ctx), "a second unlock call must not error")
	})

	t.Run("only one of many concurrent TryLock callers wins", func(t *testing.T) {
		c := factory(t)
		ctx := context.Background()

		const concurrency = 20
		var wins atomic.Int64
		var wg sync.WaitGroup
		for range concurrency {
			wg.Add(1)
			go func() {
				defer wg.Done()
				// Deliberately not unlocking here: releasing immediately would let a
				// still-racing goroutine acquire the now-free lock and "win" too,
				// defeating the assertion below. The lock is held for the whole race
				// window and only discarded along with c once the subtest ends.
				if _, err := c.TryLock(ctx, "race", 5*time.Second); err == nil {
					wins.Add(1)
				}
			}()
		}
		wg.Wait()

		require.EqualValues(t, 1, wins.Load(), "exactly one concurrent TryLock caller must win the race")
	})
}
