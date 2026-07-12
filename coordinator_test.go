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
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	gateway "github.com/StringKe/goakt-gateway"
	"github.com/StringKe/goakt-gateway/coordinator/conformance"
)

func TestMemoryCoordinatorConformance(t *testing.T) {
	conformance.Run(t, func(*testing.T) gateway.Coordinator {
		return gateway.NewMemoryCoordinator()
	})
}

// TestMemoryCoordinatorUnlockNeverStealsALaterHolder verifies the ownership-token
// contract explicitly: an unlock call from a holder whose ttl has already elapsed must
// not release a lock some other caller has since acquired.
func TestMemoryCoordinatorUnlockNeverStealsALaterHolder(t *testing.T) {
	c := gateway.NewMemoryCoordinator()
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

	// The first holder's belated unlock call must be a no-op: the second holder's lock
	// must still be in effect.
	require.NoError(t, firstUnlock(ctx))

	_, err = c.TryLock(ctx, "k", 10*time.Second)
	require.ErrorIs(t, err, gateway.ErrLockNotAcquired, "the second holder's lock must survive the first holder's stale unlock")
}
