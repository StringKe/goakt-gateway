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

// This file is a white-box (package gateway) test file so it can reach the unexported
// registerSpawnBarrier test seam and drive the Register/Unregister TOCTOU race
// deterministically. See registry_test.go (package gateway_test) for the black-box
// Registry tests.
package gateway

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/tochemey/goakt/v4/actor"
	"github.com/tochemey/goakt/v4/log"
)

// newRaceTestSystem starts a minimal actor system for the Register/Unregister race
// tests in this file.
func newRaceTestSystem(t *testing.T, opts ...actor.Option) actor.ActorSystem {
	t.Helper()
	ctx := context.Background()
	all := append([]actor.Option{actor.WithLogger(log.DiscardLogger)}, opts...)
	system, err := actor.NewActorSystem(strings.ReplaceAll(t.Name(), "/", "-"), all...)
	require.NoError(t, err)
	require.NoError(t, system.Start(ctx))
	t.Cleanup(func() {
		_ = system.Stop(context.Background())
	})
	time.Sleep(100 * time.Millisecond)
	return system
}

func TestConfirmRemoteGroupReturnsContextCancellationBeforeFanout(t *testing.T) {
	ctx := context.Background()
	presence := NewMemoryPresence()
	for i := 0; i < maxConcurrentConfirmAsks+1; i++ {
		require.NoError(t, presence.Join(ctx, "cancelled-group", fmt.Sprintf("remote-%d", i), time.Minute))
	}
	registry := NewRegistry(newRaceTestSystem(t), log.DiscardLogger,
		WithPresence(presence),
		WithDeliveryConfirmation(),
	)
	cancelled, cancel := context.WithCancel(ctx)
	cancel()

	_, err := registry.confirmRemoteGroup(cancelled, "cancelled-group", []byte("payload"))
	require.ErrorIs(t, err, context.Canceled)
}

// stubFailingPresence lets a test force a Register's finalize step to fail on demand, so the
// registration is steered onto its rollback path. Its Join returns an error while failJoin
// is set; every other method is a no-op.
type stubFailingPresence struct {
	failJoin atomic.Bool
}

func (p *stubFailingPresence) Join(_ context.Context, _, _ string, _ time.Duration) error {
	if p.failJoin.Load() {
		return errors.New("stub: forced presence join failure")
	}
	return nil
}

func (p *stubFailingPresence) Leave(context.Context, string, string) error { return nil }

func (p *stubFailingPresence) Refresh(context.Context, string, string, time.Duration) error {
	return nil
}

func (p *stubFailingPresence) Members(context.Context, string) ([]string, error) { return nil, nil }

func (p *stubFailingPresence) Online(context.Context, string) (bool, error) { return false, nil }

// TestRegisterUnregisterTOCTOU pins the exact interleaving the audited bug relied on:
// Register reserves id, then - before its actor finishes spawning - Unregister for the
// same id arrives. Prior to the fix, Unregister would find no entry, report success, and
// then the in-flight Register would still publish its entry afterward, leaking a
// connection the caller believed gone. The fix must make Register roll back instead.
func TestRegisterUnregisterTOCTOU(t *testing.T) {
	system := newRaceTestSystem(t)
	registry := NewRegistry(system, log.DiscardLogger)
	const id = "toctou-conn"

	reached := make(chan struct{})
	proceed := make(chan struct{})
	registerSpawnBarrier = func(gotID string) {
		if gotID != id {
			return
		}
		close(reached)
		<-proceed
	}
	t.Cleanup(func() { registerSpawnBarrier = nil })

	var regErr error
	done := make(chan struct{})
	go func() {
		defer close(done)
		regErr = registry.Register(context.Background(), id, func([]byte) error { return nil })
	}()

	<-reached
	// Unregister races in while Register is still spawning its actor; it must observe
	// nothing to clean up and claim success immediately.
	require.NoError(t, registry.Unregister(context.Background(), id))
	close(proceed)
	<-done

	require.ErrorIs(t, regErr, ErrConnectionClosed)
	require.False(t, registry.Has(id), "the race must not leave a connection the caller believes unregistered still in the table")
	require.Equal(t, 0, registry.Len())
}

// TestRegisterUnregisterTOCTOU_ConcurrentRegisterGetsAlreadyRegistered verifies that a
// second Register call landing while the first is still reserved (spawning its actor)
// is rejected with the typed already-registered error rather than racing past the
// reservation.
func TestRegisterUnregisterTOCTOU_ConcurrentRegisterGetsAlreadyRegistered(t *testing.T) {
	system := newRaceTestSystem(t)
	registry := NewRegistry(system, log.DiscardLogger)
	const id = "toctou-dup"

	reached := make(chan struct{})
	proceed := make(chan struct{})
	registerSpawnBarrier = func(gotID string) {
		if gotID != id {
			return
		}
		close(reached)
		<-proceed
	}
	t.Cleanup(func() { registerSpawnBarrier = nil })

	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = registry.Register(context.Background(), id, func([]byte) error { return nil })
	}()

	<-reached
	err := registry.Register(context.Background(), id, func([]byte) error { return nil })
	require.ErrorIs(t, err, ErrConnectionExists)

	close(proceed)
	<-done
	require.NoError(t, registry.Unregister(context.Background(), id))
}

// TestRegisterRollbackDoesNotClobberTakeover pins the ABA hazard the generation/identity
// fix closes. An old registration is steered into its rollback path, then parked in the
// exact window between clearing its reservation and rolling back. While it is parked a
// takeover unregisters the old entry and publishes a fresh owner under the same id. When the
// old registration finally rolls back, it must key the teardown on entry identity and leave
// the takeover's newer owner untouched; the pre-fix rollback keyed only on id and would tear
// the new owner down.
func TestRegisterRollbackDoesNotClobberTakeover(t *testing.T) {
	system := newRaceTestSystem(t, actor.WithPubSub())
	presence := &stubFailingPresence{}
	// The old registration belongs to a group, so its finalize calls the presence Join we
	// force to fail, which is what drives it onto the rollback path deterministically.
	presence.failJoin.Store(true)
	registry := NewRegistry(system, log.DiscardLogger, WithPresence(presence))
	t.Cleanup(func() { _ = registry.Close(context.Background()) })
	const id = "aba-conn"

	reached := make(chan struct{})
	proceed := make(chan struct{})
	registerRollbackBarrier = func(gotID string) {
		if gotID != id {
			return
		}
		close(reached)
		<-proceed
	}
	t.Cleanup(func() { registerRollbackBarrier = nil })

	var oldMu sync.Mutex
	oldReceived := 0
	oldSend := func([]byte) error {
		oldMu.Lock()
		oldReceived++
		oldMu.Unlock()
		return nil
	}

	var oldErr error
	done := make(chan struct{})
	go func() {
		defer close(done)
		oldErr = registry.Register(context.Background(), id, oldSend, WithConnGroup("squad"))
	}()

	// The old registration is now parked: its reservation is cleared, its entry is still the
	// current owner of id, and it is about to roll back.
	<-reached

	// A takeover slips into that window: it evicts the old entry and installs a fresh owner
	// under the same id.
	presence.failJoin.Store(false)
	var newMu sync.Mutex
	newReceived := 0
	newSend := func([]byte) error {
		newMu.Lock()
		newReceived++
		newMu.Unlock()
		return nil
	}
	require.NoError(t, registry.Register(context.Background(), id, newSend, WithReplaceExisting()))

	// Release the old registration into its now-stale rollback.
	close(proceed)
	<-done

	require.Error(t, oldErr, "the old registration's finalize failed, so it must return an error")
	require.True(t, registry.Has(id), "the takeover's new owner must survive the stale rollback")
	require.Equal(t, 1, registry.Len())
	require.NoError(t, registry.SendToConnection(context.Background(), id, []byte("ping")))

	newMu.Lock()
	require.Equal(t, 1, newReceived, "delivery must reach the surviving new owner")
	newMu.Unlock()
	oldMu.Lock()
	require.Equal(t, 0, oldReceived, "the evicted old connection must receive nothing")
	oldMu.Unlock()
}

// TestJoinLeaveNoBridgeLeak hammers Join/Leave for the same topic across many connections so
// -race and the final bridge check surface the member-less bridge leak that a Join recording
// membership before taking its bridge reference allowed: a concurrent Leave could release a
// reference the Join had not yet taken and the Join would then re-create an orphaned bridge.
func TestJoinLeaveNoBridgeLeak(t *testing.T) {
	system := newRaceTestSystem(t, actor.WithPubSub())
	registry := NewRegistry(system, log.DiscardLogger)
	ctx := context.Background()
	const (
		topic = "leak-room"
		n     = 200
	)

	for i := range n {
		id := fmt.Sprintf("leak-conn-%d", i)
		require.NoError(t, registry.Register(ctx, id, func([]byte) error { return nil }))
	}

	var wg sync.WaitGroup
	for i := range n {
		id := fmt.Sprintf("leak-conn-%d", i)
		wg.Go(func() { _ = registry.Join(ctx, id, topic) })
		wg.Go(func() { _ = registry.Leave(ctx, id, topic) })
	}
	wg.Wait()

	// Whoever won each per-id Join/Leave race, a final Leave for every id must drain the
	// topic completely: no bridge reference may outlive its last member.
	for i := range n {
		_ = registry.Leave(ctx, fmt.Sprintf("leak-conn-%d", i), topic)
	}
	require.False(t, registry.hasBridge(topic), "no member-less bridge may remain after every member left")
}

// TestCloseRejectsInFlightBridge races Register against Close and asserts Close leaves no
// bridge behind: an in-flight Register whose finalize tries to install a bridge after Close
// drained the bridge map must be refused rather than leak an orphan subscription.
func TestCloseRejectsInFlightBridge(t *testing.T) {
	system := newRaceTestSystem(t, actor.WithPubSub())
	registry := NewRegistry(system, log.DiscardLogger)
	ctx := context.Background()

	const n = 50
	var wg sync.WaitGroup
	for i := range n {
		id := fmt.Sprintf("close-conn-%d", i)
		wg.Go(func() {
			_ = registry.Register(ctx, id, func([]byte) error { return nil }, WithConnTopics("close-room"))
		})
	}

	_ = registry.Close(ctx)
	wg.Wait()

	registry.bridgeMu.Lock()
	remaining := len(registry.bridges)
	registry.bridgeMu.Unlock()
	require.Equal(t, 0, remaining, "Close must drain every bridge and reject any created by an in-flight Register")
}

// TestRegisterUnregisterRace_Stress hammers Register/Unregister for the same set of ids
// from many goroutines with no deterministic barrier, so -race can surface any lock
// misuse in the reservation bookkeeping across a large number of interleavings.
func TestRegisterUnregisterRace_Stress(t *testing.T) {
	system := newRaceTestSystem(t)
	registry := NewRegistry(system, log.DiscardLogger)
	ctx := context.Background()

	const n = 200
	var wg sync.WaitGroup
	for i := range n {
		id := fmt.Sprintf("stress-conn-%d", i)
		wg.Go(func() {
			_ = registry.Register(ctx, id, func([]byte) error { return nil })
		})
		wg.Go(func() {
			_ = registry.Unregister(ctx, id)
		})
	}
	wg.Wait()

	// Whichever side "won" each per-id race, nothing should be left dangling: sweep once
	// more so any connection that ended up registered is cleanly torn down.
	for i := range n {
		_ = registry.Unregister(ctx, fmt.Sprintf("stress-conn-%d", i))
	}
	require.Equal(t, 0, registry.Len())
}
