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
	"net"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/tochemey/goakt/v4/actor"
	"github.com/tochemey/goakt/v4/log"
)

// newTestSystem starts a non-clustered actor system for the black-box test suite.
func newTestSystem(t *testing.T, opts ...actor.Option) actor.ActorSystem {
	t.Helper()
	ctx := context.Background()
	allOpts := append([]actor.Option{actor.WithLogger(log.DiscardLogger)}, opts...)
	// t.Name() contains '/' for subtests, which ActorSystem names reject; sanitize it so
	// this helper works from t.Run subtests too.
	name := strings.ReplaceAll(t.Name(), "/", "-")
	system, err := actor.NewActorSystem(name, allOpts...)
	require.NoError(t, err)
	require.NoError(t, system.Start(ctx))
	t.Cleanup(func() {
		_ = system.Stop(context.Background())
	})
	time.Sleep(100 * time.Millisecond)
	return system
}

// freePort reserves an ephemeral TCP port on 127.0.0.1 and immediately releases it, for
// tests that need to bind a real listener at a known port before the test's server is
// actually up (e.g. to build a dial address ahead of time). There is an inherent, narrow
// race between releasing and rebinding the port; it is the standard workaround absent a
// dedicated port-allocation service and is acceptable for test use.
func freePort(t *testing.T) int {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	port := ln.Addr().(*net.TCPAddr).Port
	require.NoError(t, ln.Close())
	return port
}
