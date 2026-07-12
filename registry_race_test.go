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
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/tochemey/goakt/v4/actor"
	"github.com/tochemey/goakt/v4/log"
)

// newRaceTestSystem starts a minimal actor system for the Register/Unregister race
// tests in this file.
func newRaceTestSystem(t *testing.T) actor.ActorSystem {
	t.Helper()
	ctx := context.Background()
	system, err := actor.NewActorSystem(t.Name(), actor.WithLogger(log.DiscardLogger))
	require.NoError(t, err)
	require.NoError(t, system.Start(ctx))
	t.Cleanup(func() {
		_ = system.Stop(context.Background())
	})
	time.Sleep(100 * time.Millisecond)
	return system
}

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

// TestRegisterUnregisterRace_Stress hammers Register/Unregister for the same set of ids
// from many goroutines with no deterministic barrier, so -race can surface any lock
// misuse in the reservation bookkeeping across a large number of interleavings.
func TestRegisterUnregisterRace_Stress(t *testing.T) {
	system := newRaceTestSystem(t)
	registry := NewRegistry(system, log.DiscardLogger)
	ctx := context.Background()

	const n = 200
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		id := fmt.Sprintf("stress-conn-%d", i)
		wg.Add(2)
		go func() {
			defer wg.Done()
			_ = registry.Register(ctx, id, func([]byte) error { return nil })
		}()
		go func() {
			defer wg.Done()
			_ = registry.Unregister(ctx, id)
		}()
	}
	wg.Wait()

	// Whichever side "won" each per-id race, nothing should be left dangling: sweep once
	// more so any connection that ended up registered is cleanly torn down.
	for i := 0; i < n; i++ {
		_ = registry.Unregister(ctx, fmt.Sprintf("stress-conn-%d", i))
	}
	require.Equal(t, 0, registry.Len())
}
