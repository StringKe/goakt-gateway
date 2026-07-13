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

	"github.com/stretchr/testify/require"

	gateway "github.com/StringKe/goakt-gateway"
)

// TestMemorySSEHistorySinceReturnsIndependentCopy guards the snapshot ownership rule on the
// read side: Since must hand back a fully independent copy, so a caller mutating a replayed
// payload cannot corrupt the retained buffer that a later Since (or a concurrent reader) reads.
func TestMemorySSEHistorySinceReturnsIndependentCopy(t *testing.T) {
	ctx := context.Background()
	history := gateway.NewMemorySSEHistory(10)

	require.NoError(t, history.Append(ctx, "c1", "c1-1", []byte("original")))

	events, err := history.Since(ctx, "c1", "")
	require.NoError(t, err)
	require.Len(t, events, 1)

	// A caller that owns the returned events mutates the payload in place.
	copy(events[0].Payload, []byte("mangled!"))

	// The retained buffer must be untouched by that mutation.
	again, err := history.Since(ctx, "c1", "")
	require.NoError(t, err)
	require.Len(t, again, 1)
	require.Equal(t, []byte("original"), again[0].Payload)
}
