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

package gateway_test

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/tochemey/goakt/v4/log"

	gateway "github.com/StringKe/goakt-gateway"
)

// noopObserver satisfies gateway.Observer with no behaviour, so a test only has to override
// the one method it cares about by embedding it.
type noopObserver struct{}

func (noopObserver) ConnectionRegistered(string, string)   {}
func (noopObserver) ConnectionUnregistered(string, string) {}
func (noopObserver) ConnectionReplaced(string, string)     {}
func (noopObserver) DeliveryDropped(string, string)        {}
func (noopObserver) DeliveryFailed(string, error)          {}
func (noopObserver) BroadcastFanout(string, int)           {}

// offlineDropObserver counts overloaded-fallback reports so a test can assert how many
// fallbacks were shed once the concurrency bound saturated.
type offlineDropObserver struct {
	noopObserver
	drops atomic.Int32
}

func (o *offlineDropObserver) OfflineFallback(_ string, err error) {
	if errors.Is(err, gateway.ErrOfflineFallbackOverloaded) {
		o.drops.Add(1)
	}
}

// blockingOffline holds every Deliver call until release is closed, and records the peak
// number of concurrent in-flight deliveries so a test can prove the bound was enforced.
type blockingOffline struct {
	started    chan struct{}
	release    chan struct{}
	concurrent atomic.Int32
	maxSeen    atomic.Int32
}

func (b *blockingOffline) Deliver(_ context.Context, _ string, _ []byte) error {
	n := b.concurrent.Add(1)
	for {
		old := b.maxSeen.Load()
		if n <= old || b.maxSeen.CompareAndSwap(old, n) {
			break
		}
	}
	b.started <- struct{}{}
	<-b.release
	b.concurrent.Add(-1)
	return nil
}

// TestOfflineFallbackConcurrencyBounded proves a storm of offline identities cannot spawn
// unbounded fallback goroutines: with a bound of 3, only 3 deliveries run at once and the rest
// are dropped and reported through OfflineObserver.
func TestOfflineFallbackConcurrencyBounded(t *testing.T) {
	const bound = 3
	const targets = 20

	system := newTestSystem(t)
	blocking := &blockingOffline{
		started: make(chan struct{}, targets),
		release: make(chan struct{}),
	}
	obs := &offlineDropObserver{}
	registry := gateway.NewRegistry(system, log.DiscardLogger,
		gateway.WithPresence(gateway.NewMemoryPresence()),
		gateway.WithObserver(obs),
		gateway.WithOfflineChannel(blocking, gateway.WithOfflineMaxConcurrent(bound)),
	)
	ctx := context.Background()
	t.Cleanup(func() { _ = registry.Close(ctx) })

	// The bound is taken synchronously on the SendToGroup goroutine before the fallback is
	// spawned, so firing sequentially makes the outcome deterministic: the first `bound` calls
	// take the slots (and block in Deliver), every later call finds the semaphore full and is
	// dropped.
	for i := 0; i < targets; i++ {
		result, err := registry.SendToGroup(ctx, fmt.Sprintf("user:absent-%d", i), []byte("m"))
		require.NoError(t, err)
		require.True(t, result.None())
	}

	for i := 0; i < bound; i++ {
		select {
		case <-blocking.started:
		case <-time.After(2 * time.Second):
			t.Fatalf("only %d of %d bounded deliveries started", i, bound)
		}
	}

	require.LessOrEqual(t, blocking.maxSeen.Load(), int32(bound))
	require.Equal(t, int32(targets-bound), obs.drops.Load())

	// No further delivery may have slipped past the bound while the slots were held.
	require.Equal(t, int32(bound), blocking.concurrent.Load())

	close(blocking.release)
}

// gatedOffline blocks in Deliver until proceed is closed, then reports the payload bytes it
// observed. It lets a test mutate the caller's buffer after SendToGroup returns but before the
// async Deliver reads it, which is exactly the window a payload copy must close.
type gatedOffline struct {
	proceed chan struct{}
	got     chan []byte
}

type cancellableOffline struct {
	started chan struct{}
}

func (c *cancellableOffline) Deliver(ctx context.Context, _ string, _ []byte) error {
	c.started <- struct{}{}
	<-ctx.Done()
	return ctx.Err()
}

func TestRegistryCloseCancelsOfflineFallback(t *testing.T) {
	system := newTestSystem(t)
	offline := &cancellableOffline{started: make(chan struct{}, 1)}
	registry := gateway.NewRegistry(system, log.DiscardLogger,
		gateway.WithPresence(gateway.NewMemoryPresence()),
		gateway.WithOfflineChannel(offline),
	)

	result, err := registry.SendToGroup(context.Background(), "user:absent", []byte("m"))
	require.NoError(t, err)
	require.True(t, result.None())
	select {
	case <-offline.started:
	case <-time.After(2 * time.Second):
		t.Fatal("offline fallback did not start")
	}

	closed := make(chan struct{})
	go func() {
		_ = registry.Close(context.Background())
		close(closed)
	}()
	select {
	case <-closed:
	case <-time.After(2 * time.Second):
		t.Fatal("registry close did not cancel the offline fallback")
	}
}

func (g *gatedOffline) Deliver(_ context.Context, _ string, payload []byte) error {
	<-g.proceed
	seen := make([]byte, len(payload))
	copy(seen, payload)
	g.got <- seen
	return nil
}

// TestOfflineFallbackCopiesPayload verifies the fallback copies the payload before handing it
// to the async goroutine, so a caller reusing its buffer the moment SendToGroup returns cannot
// corrupt the offline delivery.
func TestOfflineFallbackCopiesPayload(t *testing.T) {
	system := newTestSystem(t)
	gated := &gatedOffline{
		proceed: make(chan struct{}),
		got:     make(chan []byte, 1),
	}
	registry := gateway.NewRegistry(system, log.DiscardLogger,
		gateway.WithPresence(gateway.NewMemoryPresence()),
		gateway.WithOfflineChannel(gated),
	)
	ctx := context.Background()
	t.Cleanup(func() { _ = registry.Close(ctx) })

	payload := []byte("original")
	result, err := registry.SendToGroup(ctx, "user:absent", payload)
	require.NoError(t, err)
	require.True(t, result.None())

	// The caller owns and reuses its buffer as soon as SendToGroup returns.
	copy(payload, []byte("mangled!"))
	close(gated.proceed)

	select {
	case seen := <-gated.got:
		require.Equal(t, "original", string(seen))
	case <-time.After(2 * time.Second):
		t.Fatal("offline delivery never ran")
	}
}
