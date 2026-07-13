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

// Package conformance is a shared test suite every gateway.Presence implementation must
// pass, so gateway.MemoryPresence and presence/redis.Presence (and any third-party
// implementation) are held to the exact same contract - most importantly the lease
// contract, because an implementation that keeps a dead node's connections online forever
// makes IsOnline permanently true and silently disables the offline delivery channel.
package conformance

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	gateway "github.com/StringKe/goakt-gateway"
)

// Run exercises factory() against the gateway.Presence contract. factory must return a
// fresh, empty Presence; Run calls it once per subtest so implementations backed by a
// shared external service (e.g. Redis) do not see state leak between subtests as long as
// factory picks a fresh key namespace or database per call.
func Run(t *testing.T, factory func(t *testing.T) gateway.Presence) {
	t.Helper()

	t.Run("Members of an unknown group is empty", func(t *testing.T) {
		p := factory(t)
		members, err := p.Members(context.Background(), "user:absent")
		require.NoError(t, err)
		require.Empty(t, members)
	})

	t.Run("Online reports false for an unknown group", func(t *testing.T) {
		p := factory(t)
		online, err := p.Online(context.Background(), "user:absent")
		require.NoError(t, err)
		require.False(t, online)
	})

	t.Run("Join then Members and Online see the member", func(t *testing.T) {
		p := factory(t)
		ctx := context.Background()
		require.NoError(t, p.Join(ctx, "user:1", "conn-a", time.Minute))

		members, err := p.Members(ctx, "user:1")
		require.NoError(t, err)
		require.Equal(t, []string{"conn-a"}, members)

		online, err := p.Online(ctx, "user:1")
		require.NoError(t, err)
		require.True(t, online)
	})

	t.Run("a group holds every device of the same identity", func(t *testing.T) {
		p := factory(t)
		ctx := context.Background()
		require.NoError(t, p.Join(ctx, "user:1", "conn-a", time.Minute))
		require.NoError(t, p.Join(ctx, "user:1", "conn-b", time.Minute))

		members, err := p.Members(ctx, "user:1")
		require.NoError(t, err)
		require.ElementsMatch(t, []string{"conn-a", "conn-b"}, members)
	})

	t.Run("joining the same connection id twice does not duplicate it", func(t *testing.T) {
		p := factory(t)
		ctx := context.Background()
		require.NoError(t, p.Join(ctx, "user:1", "conn-a", time.Minute))
		require.NoError(t, p.Join(ctx, "user:1", "conn-a", time.Minute))

		members, err := p.Members(ctx, "user:1")
		require.NoError(t, err)
		require.Equal(t, []string{"conn-a"}, members)
	})

	t.Run("groups are isolated from one another", func(t *testing.T) {
		p := factory(t)
		ctx := context.Background()
		require.NoError(t, p.Join(ctx, "user:1", "conn-a", time.Minute))
		require.NoError(t, p.Join(ctx, "user:2", "conn-b", time.Minute))

		members, err := p.Members(ctx, "user:1")
		require.NoError(t, err)
		require.Equal(t, []string{"conn-a"}, members)

		members, err = p.Members(ctx, "user:2")
		require.NoError(t, err)
		require.Equal(t, []string{"conn-b"}, members)
	})

	t.Run("Leave removes only the named member", func(t *testing.T) {
		p := factory(t)
		ctx := context.Background()
		require.NoError(t, p.Join(ctx, "user:1", "conn-a", time.Minute))
		require.NoError(t, p.Join(ctx, "user:1", "conn-b", time.Minute))
		require.NoError(t, p.Leave(ctx, "user:1", "conn-a"))

		members, err := p.Members(ctx, "user:1")
		require.NoError(t, err)
		require.Equal(t, []string{"conn-b"}, members)

		online, err := p.Online(ctx, "user:1")
		require.NoError(t, err)
		require.True(t, online)
	})

	t.Run("Online reports false once the last member leaves", func(t *testing.T) {
		p := factory(t)
		ctx := context.Background()
		require.NoError(t, p.Join(ctx, "user:1", "conn-a", time.Minute))
		require.NoError(t, p.Leave(ctx, "user:1", "conn-a"))

		online, err := p.Online(ctx, "user:1")
		require.NoError(t, err)
		require.False(t, online)

		members, err := p.Members(ctx, "user:1")
		require.NoError(t, err)
		require.Empty(t, members)
	})

	t.Run("Leave of an absent member or group is a no-op", func(t *testing.T) {
		p := factory(t)
		ctx := context.Background()
		require.NoError(t, p.Leave(ctx, "user:absent", "conn-absent"))

		require.NoError(t, p.Join(ctx, "user:1", "conn-a", time.Minute))
		require.NoError(t, p.Leave(ctx, "user:1", "conn-absent"))
		require.NoError(t, p.Leave(ctx, "user:1", "conn-a"))
		require.NoError(t, p.Leave(ctx, "user:1", "conn-a"), "a second Leave must not error")
	})

	t.Run("a lapsed lease drops the member without anyone calling Leave", func(t *testing.T) {
		// This is the whole point of the lease: the node holding conn-a has died, so no
		// Leave will ever arrive, and the identity must stop reading as online.
		p := factory(t)
		ctx := context.Background()
		require.NoError(t, p.Join(ctx, "user:1", "conn-a", 100*time.Millisecond))

		online, err := p.Online(ctx, "user:1")
		require.NoError(t, err)
		require.True(t, online)

		require.Eventually(t, func() bool {
			online, err := p.Online(ctx, "user:1")
			return err == nil && !online
		}, 5*time.Second, 20*time.Millisecond, "a member must go offline once its lease lapses")

		members, err := p.Members(ctx, "user:1")
		require.NoError(t, err)
		require.Empty(t, members, "Members must not report a member whose lease has lapsed")
	})

	t.Run("Members filters lapsed leases and keeps the live ones", func(t *testing.T) {
		p := factory(t)
		ctx := context.Background()
		require.NoError(t, p.Join(ctx, "user:1", "conn-dead", 100*time.Millisecond))
		require.NoError(t, p.Join(ctx, "user:1", "conn-live", time.Minute))

		require.Eventually(t, func() bool {
			members, err := p.Members(ctx, "user:1")
			if err != nil {
				return false
			}
			return len(members) == 1 && members[0] == "conn-live"
		}, 5*time.Second, 20*time.Millisecond, "the lapsed member must disappear while the live one stays")

		online, err := p.Online(ctx, "user:1")
		require.NoError(t, err)
		require.True(t, online)
	})

	t.Run("Refresh keeps a member alive past its original lease", func(t *testing.T) {
		p := factory(t)
		ctx := context.Background()
		require.NoError(t, p.Join(ctx, "user:1", "conn-a", 300*time.Millisecond))

		// Renew well inside the lease, the way Registry's renewal loop does (ttl/3).
		deadline := time.Now().Add(900 * time.Millisecond)
		for time.Now().Before(deadline) {
			time.Sleep(100 * time.Millisecond)
			require.NoError(t, p.Refresh(ctx, "user:1", "conn-a", 300*time.Millisecond))

			online, err := p.Online(ctx, "user:1")
			require.NoError(t, err)
			require.True(t, online, "a refreshed member must never drop out")
		}

		members, err := p.Members(ctx, "user:1")
		require.NoError(t, err)
		require.Equal(t, []string{"conn-a"}, members)
	})

	t.Run("Refresh of a lapsed member re-records it", func(t *testing.T) {
		// The caller is the node that actually holds the socket, so its view wins over an
		// expired lease: a renewal that arrives late must restore the member, not fail.
		p := factory(t)
		ctx := context.Background()
		require.NoError(t, p.Join(ctx, "user:1", "conn-a", 50*time.Millisecond))

		require.Eventually(t, func() bool {
			online, err := p.Online(ctx, "user:1")
			return err == nil && !online
		}, 5*time.Second, 20*time.Millisecond)

		require.NoError(t, p.Refresh(ctx, "user:1", "conn-a", time.Minute))

		members, err := p.Members(ctx, "user:1")
		require.NoError(t, err)
		require.Equal(t, []string{"conn-a"}, members)
	})

	t.Run("Refresh of one member does not disturb the others", func(t *testing.T) {
		p := factory(t)
		ctx := context.Background()
		require.NoError(t, p.Join(ctx, "user:1", "conn-a", time.Minute))
		require.NoError(t, p.Join(ctx, "user:1", "conn-b", time.Minute))
		require.NoError(t, p.Refresh(ctx, "user:1", "conn-a", 2*time.Minute))

		members, err := p.Members(ctx, "user:1")
		require.NoError(t, err)
		require.ElementsMatch(t, []string{"conn-a", "conn-b"}, members)
	})

	t.Run("a non-positive ttl records a member that never expires", func(t *testing.T) {
		p := factory(t)
		ctx := context.Background()
		require.NoError(t, p.Join(ctx, "user:1", "conn-a", 0))

		time.Sleep(200 * time.Millisecond)

		members, err := p.Members(ctx, "user:1")
		require.NoError(t, err)
		require.Equal(t, []string{"conn-a"}, members)

		online, err := p.Online(ctx, "user:1")
		require.NoError(t, err)
		require.True(t, online)
	})

	t.Run("a never-expiring member survives alongside a lapsing one", func(t *testing.T) {
		p := factory(t)
		ctx := context.Background()
		require.NoError(t, p.Join(ctx, "user:1", "conn-forever", 0))
		require.NoError(t, p.Join(ctx, "user:1", "conn-dead", 100*time.Millisecond))

		require.Eventually(t, func() bool {
			members, err := p.Members(ctx, "user:1")
			if err != nil {
				return false
			}
			return len(members) == 1 && members[0] == "conn-forever"
		}, 5*time.Second, 20*time.Millisecond, "sweeping the lapsed member must not evict the never-expiring one")
	})

	t.Run("concurrent Join and Leave leave a consistent membership", func(t *testing.T) {
		p := factory(t)
		ctx := context.Background()

		const connections = 30
		var wg sync.WaitGroup
		for i := range connections {
			wg.Go(func() {
				connID := fmt.Sprintf("conn-%d", i)
				require.NoError(t, p.Join(ctx, "user:1", connID, time.Minute))
				// Half the connections disconnect again: whichever way the two operations
				// interleave, the surviving membership must be exactly the other half.
				if i%2 == 0 {
					require.NoError(t, p.Leave(ctx, "user:1", connID))
				}
			})
		}
		wg.Wait()

		want := make([]string, 0, connections/2)
		for i := 1; i < connections; i += 2 {
			want = append(want, fmt.Sprintf("conn-%d", i))
		}

		members, err := p.Members(ctx, "user:1")
		require.NoError(t, err)
		require.ElementsMatch(t, want, members)
	})

	t.Run("concurrent Join and Refresh of the same member stay idempotent", func(t *testing.T) {
		p := factory(t)
		ctx := context.Background()

		var wg sync.WaitGroup
		for range 20 {
			wg.Go(func() {
				require.NoError(t, p.Join(ctx, "user:1", "conn-a", time.Minute))
				require.NoError(t, p.Refresh(ctx, "user:1", "conn-a", time.Minute))
			})
		}
		wg.Wait()

		members, err := p.Members(ctx, "user:1")
		require.NoError(t, err)
		require.Equal(t, []string{"conn-a"}, members)
	})
}
