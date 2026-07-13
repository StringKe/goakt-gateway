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

	t.Run("unlock never steals a lock a later holder acquired after this holder's ttl expired", func(t *testing.T) {
		c := factory(t)
		ctx := context.Background()

		firstUnlock, err := c.TryLock(ctx, "k", 30*time.Millisecond)
		require.NoError(t, err)

		require.Eventually(t, func() bool {
			secondUnlock, err := c.TryLock(ctx, "k", 10*time.Second)
			if err != nil {
				return false
			}
			t.Cleanup(func() { _ = secondUnlock(ctx) })
			return true
		}, 3*time.Second, 10*time.Millisecond, "the lock must become acquirable once the first holder's ttl elapses")

		// The first holder's belated unlock call must be a no-op: the second holder's
		// lock must still be in effect. A bare DEL-based unlock would incorrectly
		// release it here regardless of which holder set it.
		require.NoError(t, firstUnlock(ctx))

		_, err = c.TryLock(ctx, "k", 10*time.Second)
		require.ErrorIs(t, err, gateway.ErrLockNotAcquired, "the second holder's lock must survive the first holder's stale unlock")
	})

	t.Run("only one of many concurrent TryLock callers wins", func(t *testing.T) {
		c := factory(t)
		ctx := context.Background()

		const concurrency = 20
		var wins atomic.Int64
		var wg sync.WaitGroup
		for range concurrency {
			wg.Go(func() {
				// Deliberately not unlocking here: releasing immediately would let a
				// still-racing goroutine acquire the now-free lock and "win" too,
				// defeating the assertion below. The lock is held for the whole race
				// window and only discarded along with c once the subtest ends.
				if _, err := c.TryLock(ctx, "race", 5*time.Second); err == nil {
					wins.Add(1)
				}
			})
		}
		wg.Wait()

		require.EqualValues(t, 1, wins.Load(), "exactly one concurrent TryLock caller must win the race")
	})

	// The next two subtests are adversarial: they prove Put/Get and TryLock keep fully
	// disjoint namespaces, so no user-constructable key can make a data write disturb a lock
	// or a lock disturb a stored value. A naive implementation that derives a lock key by
	// suffixing the data key (data=prefix+key, lock=prefix+key+":lock") fails both: a caller
	// that Puts key+":lock" lands on the very key another caller's TryLock(key) uses.

	t.Run("a data value can never occupy or corrupt a lock's key space", func(t *testing.T) {
		c := factory(t)
		ctx := context.Background()

		// Store a value under a key crafted to alias the lock of "resource" in a suffix scheme.
		require.NoError(t, c.Put(ctx, "resource:lock", []byte("data"), 0))

		// The lock for "resource" must still be free: a stored value must never read as a lock.
		unlock, err := c.TryLock(ctx, "resource", 10*time.Second)
		require.NoError(t, err, "a data value must not occupy the lock namespace")
		require.NotNil(t, unlock)

		// The value must still be intact and readable as data, undisturbed by the lock.
		value, ok, err := c.Get(ctx, "resource:lock")
		require.NoError(t, err)
		require.True(t, ok, "acquiring a lock must not evict a value under a look-alike key")
		require.Equal(t, []byte("data"), value)

		// Releasing the lock must not delete the look-alike data value either.
		require.NoError(t, unlock(ctx))
		value, ok, err = c.Get(ctx, "resource:lock")
		require.NoError(t, err)
		require.True(t, ok, "unlock must not delete a value under a look-alike key")
		require.Equal(t, []byte("data"), value)
	})

	t.Run("a data write can never release or wedge a held lock, whatever key it targets", func(t *testing.T) {
		c := factory(t)
		ctx := context.Background()

		unlock, err := c.TryLock(ctx, "resource", 10*time.Second)
		require.NoError(t, err)
		require.NotNil(t, unlock)

		// No data write, including ones crafted to alias the lock's internal key, may touch the
		// lock's token or ttl.
		for _, k := range []string{"resource", "resource:lock", "l:resource", "d:resource"} {
			require.NoError(t, c.Put(ctx, k, []byte("x"), 0))
		}

		_, err = c.TryLock(ctx, "resource", 10*time.Second)
		require.ErrorIs(t, err, gateway.ErrLockNotAcquired, "a data write must not release a held lock")

		// The true holder's token must survive intact, so its unlock still releases the lock
		// (a Put that overwrote the token would wedge the lock permanently).
		require.NoError(t, unlock(ctx))
		unlock2, err := c.TryLock(ctx, "resource", 10*time.Second)
		require.NoError(t, err, "after the true holder unlocks, the lock must be acquirable again")
		require.NoError(t, unlock2(ctx))
	})
}

// RunCAS exercises factory() against the gateway.CASCoordinator contract, in addition to
// the base Coordinator contract Run already covers. Every CASCoordinator implementation
// (gateway.MemoryCoordinator, coordinator/redis.Coordinator, and any third party) must pass
// both Run and RunCAS.
func RunCAS(t *testing.T, factory func(t *testing.T) gateway.CASCoordinator) {
	t.Helper()

	t.Run("CompareAndSwap with expected=nil succeeds on an absent key", func(t *testing.T) {
		c := factory(t)
		ctx := context.Background()

		ok, err := c.CompareAndSwap(ctx, "k", nil, []byte("v1"), 0)
		require.NoError(t, err)
		require.True(t, ok)

		value, exists, err := c.Get(ctx, "k")
		require.NoError(t, err)
		require.True(t, exists)
		require.Equal(t, []byte("v1"), value)
	})

	t.Run("CompareAndSwap with expected=nil fails once the key already exists", func(t *testing.T) {
		c := factory(t)
		ctx := context.Background()
		require.NoError(t, c.Put(ctx, "k", []byte("v1"), 0))

		ok, err := c.CompareAndSwap(ctx, "k", nil, []byte("v2"), 0)
		require.NoError(t, err)
		require.False(t, ok)

		value, _, err := c.Get(ctx, "k")
		require.NoError(t, err)
		require.Equal(t, []byte("v1"), value, "a failed CompareAndSwap must leave the existing value untouched")
	})

	t.Run("CompareAndSwap with a matching expected value swaps and returns true", func(t *testing.T) {
		c := factory(t)
		ctx := context.Background()
		require.NoError(t, c.Put(ctx, "k", []byte("v1"), 0))

		ok, err := c.CompareAndSwap(ctx, "k", []byte("v1"), []byte("v2"), 0)
		require.NoError(t, err)
		require.True(t, ok)

		value, _, err := c.Get(ctx, "k")
		require.NoError(t, err)
		require.Equal(t, []byte("v2"), value)
	})

	t.Run("CompareAndSwap with a mismatched expected value fails and leaves the value untouched", func(t *testing.T) {
		c := factory(t)
		ctx := context.Background()
		require.NoError(t, c.Put(ctx, "k", []byte("v1"), 0))

		ok, err := c.CompareAndSwap(ctx, "k", []byte("wrong"), []byte("v2"), 0)
		require.NoError(t, err)
		require.False(t, ok)

		value, _, err := c.Get(ctx, "k")
		require.NoError(t, err)
		require.Equal(t, []byte("v1"), value)
	})

	t.Run("CompareAndSwap with a non-nil expected value fails against an absent key", func(t *testing.T) {
		c := factory(t)
		ctx := context.Background()

		ok, err := c.CompareAndSwap(ctx, "absent", []byte("anything"), []byte("v"), 0)
		require.NoError(t, err)
		require.False(t, ok)

		_, exists, err := c.Get(ctx, "absent")
		require.NoError(t, err)
		require.False(t, exists, "a failed CompareAndSwap must not create the key")
	})

	t.Run("CompareAndSwap respects the given ttl", func(t *testing.T) {
		c := factory(t)
		ctx := context.Background()

		ok, err := c.CompareAndSwap(ctx, "k", nil, []byte("v1"), 50*time.Millisecond)
		require.NoError(t, err)
		require.True(t, ok)

		require.Eventually(t, func() bool {
			_, exists, err := c.Get(ctx, "k")
			return err == nil && !exists
		}, 3*time.Second, 20*time.Millisecond, "a CompareAndSwap-written value must expire once its ttl elapses")
	})

	t.Run("CompareAndSwap treats an expired value as absent, so expected=nil succeeds again", func(t *testing.T) {
		c := factory(t)
		ctx := context.Background()
		ok, err := c.CompareAndSwap(ctx, "k", nil, []byte("v1"), 50*time.Millisecond)
		require.NoError(t, err)
		require.True(t, ok)

		require.Eventually(t, func() bool {
			ok, err := c.CompareAndSwap(ctx, "k", nil, []byte("v2"), 0)
			return err == nil && ok
		}, 3*time.Second, 20*time.Millisecond, "expected=nil must succeed again once the previous value has expired")

		value, _, err := c.Get(ctx, "k")
		require.NoError(t, err)
		require.Equal(t, []byte("v2"), value)
	})

	t.Run("CompareAndSwap shares its key space with Put/Get", func(t *testing.T) {
		c := factory(t)
		ctx := context.Background()
		require.NoError(t, c.Put(ctx, "k", []byte("from-put"), 0))

		ok, err := c.CompareAndSwap(ctx, "k", []byte("from-put"), []byte("from-cas"), 0)
		require.NoError(t, err)
		require.True(t, ok, "CompareAndSwap must compare against the value a prior Put wrote")

		value, _, err := c.Get(ctx, "k")
		require.NoError(t, err)
		require.Equal(t, []byte("from-cas"), value, "Get must observe the value a prior CompareAndSwap wrote")
	})

	t.Run("only one of many concurrent CompareAndSwap callers wins a brand-new key", func(t *testing.T) {
		c := factory(t)
		ctx := context.Background()

		const concurrency = 20
		var wins atomic.Int64
		var wg sync.WaitGroup
		for range concurrency {
			wg.Go(func() {
				if ok, err := c.CompareAndSwap(ctx, "race", nil, []byte("v"), 10*time.Second); err == nil && ok {
					wins.Add(1)
				}
			})
		}
		wg.Wait()

		require.EqualValues(t, 1, wins.Load(), "exactly one concurrent CompareAndSwap(expected=nil) caller must win a brand-new key")
	})
}
