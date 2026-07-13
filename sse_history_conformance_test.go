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
	"testing"

	gateway "github.com/StringKe/goakt-gateway"
	"github.com/StringKe/goakt-gateway/ssehistory/conformance"
)

// TestMemorySSEHistoryConformance holds gateway.MemorySSEHistory to the same Last-Event-ID
// contract the Redis backend is held to, so the two cannot drift apart. perConn is set well
// above the suite's 8-event ceiling, as the suite's factory contract requires.
func TestMemorySSEHistoryConformance(t *testing.T) {
	conformance.Run(t, func() gateway.SSEHistory {
		return gateway.NewMemorySSEHistory(32)
	})
}
