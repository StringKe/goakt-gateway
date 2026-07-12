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

package gateway

import (
	"context"
	"sync"
	"time"

	"github.com/google/uuid"
)

// Coordinator is the storage-agnostic backend Manager uses to arbitrate certificate
// issuance and distribute the result across every process that shares it. It is owned
// by this library rather than by GoAkt: GoAkt's cluster KV store is PA/EC with
// last-write-wins semantics and documents its own lock as "recommended for efficiency,
// not correctness", which is the wrong substrate for a certificate issuance lock - a
// duplicated issuance burns CA rate limit.
//
// A Coordinator does not have to be cluster-shared: NewMemoryCoordinator (the default)
// is process-local and gives Manager the same single-flight-only behavior a
// non-clustered deployment needs. Share a Coordinator implementation across processes
// (e.g. coordinator/redis) to get cluster-wide arbitration and distribution.
//
// Implementations must be safe for concurrent use.
type Coordinator interface {
	// Get returns the value stored for key. ok is false if key is absent or has
	// expired; a missing key is not an error.
	Get(ctx context.Context, key string) (value []byte, ok bool, err error)
	// Put stores value under key. A ttl of zero or less means the value never expires
	// on its own (it is overwritten or explicitly replaced instead).
	Put(ctx context.Context, key string, value []byte, ttl time.Duration) error
	// TryLock attempts to acquire an exclusive, TTL-bounded lock on key. On success it
	// returns an unlock function that releases the lock; unlock is safe to call more
	// than once and safe to call after ttl has already elapsed (a no-op in that case,
	// since the lock is already gone). On failure it returns ErrLockNotAcquired.
	//
	// The lock is a real mutual exclusion primitive: implementations must guarantee
	// that at most one caller holds a given key at a time, and that a caller's unlock
	// never releases a lock some other caller has since acquired (e.g. after the
	// original holder's TTL expired).
	TryLock(ctx context.Context, key string, ttl time.Duration) (unlock func(context.Context) error, err error)
}

// MemoryCoordinator is the default, in-process Coordinator. It is appropriate for a
// single-node deployment, local development, and tests: TryLock/Put/Get only coordinate
// callers within this process, so it gives Manager exactly the "issue once per process"
// behavior a non-clustered gateway needs. Share a cluster-backed Coordinator (e.g.
// coordinator/redis) across processes to get cluster-wide arbitration.
type MemoryCoordinator struct {
	mu      sync.Mutex
	entries map[string]memEntry
	locks   map[string]memLock
}

type memEntry struct {
	value     []byte
	expiresAt time.Time // zero means no expiry
}

type memLock struct {
	token     string
	expiresAt time.Time
}

// enforce compilation error
var _ Coordinator = (*MemoryCoordinator)(nil)

// NewMemoryCoordinator creates an empty MemoryCoordinator.
func NewMemoryCoordinator() *MemoryCoordinator {
	return &MemoryCoordinator{
		entries: make(map[string]memEntry),
		locks:   make(map[string]memLock),
	}
}

// Get implements Coordinator.
func (m *MemoryCoordinator) Get(_ context.Context, key string) ([]byte, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	entry, ok := m.entries[key]
	if !ok {
		return nil, false, nil
	}
	if !entry.expiresAt.IsZero() && time.Now().After(entry.expiresAt) {
		delete(m.entries, key)
		return nil, false, nil
	}
	return append([]byte(nil), entry.value...), true, nil
}

// Put implements Coordinator.
func (m *MemoryCoordinator) Put(_ context.Context, key string, value []byte, ttl time.Duration) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	var expiresAt time.Time
	if ttl > 0 {
		expiresAt = time.Now().Add(ttl)
	}
	m.entries[key] = memEntry{value: append([]byte(nil), value...), expiresAt: expiresAt}
	return nil
}

// TryLock implements Coordinator. Ownership is tracked with a per-acquisition token so
// that an unlock call can never release a lock some other caller has since acquired
// after this one's ttl elapsed.
func (m *MemoryCoordinator) TryLock(_ context.Context, key string, ttl time.Duration) (func(context.Context) error, error) {
	m.mu.Lock()

	if existing, ok := m.locks[key]; ok && time.Now().Before(existing.expiresAt) {
		m.mu.Unlock()
		return nil, ErrLockNotAcquired
	}

	token := uuid.NewString()
	m.locks[key] = memLock{token: token, expiresAt: time.Now().Add(ttl)}
	m.mu.Unlock()

	return func(context.Context) error {
		m.mu.Lock()
		defer m.mu.Unlock()
		if current, ok := m.locks[key]; ok && current.token == token {
			delete(m.locks, key)
		}
		return nil
	}, nil
}
