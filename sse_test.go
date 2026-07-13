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
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/tochemey/goakt/v4/actor"
	"github.com/tochemey/goakt/v4/log"
	"github.com/tochemey/goakt/v4/testkit"

	gateway "github.com/StringKe/goakt-gateway"
)

func TestSSEHandlerDelivery(t *testing.T) {
	system := newTestSystem(t)
	registry := gateway.NewRegistry(system, log.DiscardLogger)

	handler := gateway.NewSSEHandler(registry,
		gateway.WithSSEIDFunc(func(r *http.Request) string { return r.URL.Query().Get("id") }),
		gateway.WithSSEKeepAlive(time.Hour),
	)

	server := httptest.NewServer(handler)
	defer server.Close()

	resp, cancel := openSSEStream(t, server.URL+"/?id=sse-1", "")
	defer cancel()
	defer func() { _ = resp.Body.Close() }()
	require.Equal(t, "text/event-stream", resp.Header.Get("Content-Type"))

	require.Eventually(t, func() bool { return registry.Has("sse-1") }, 3*time.Second, 20*time.Millisecond)
	require.NoError(t, registry.SendToConnection(context.Background(), "sse-1", []byte("hello")))

	reader := bufio.NewReader(resp.Body)
	frame, err := readSSEFrame(reader)
	require.NoError(t, err)
	require.Equal(t, "hello", frame.data)
	require.Equal(t, "sse-1-1", frame.id)
	require.Empty(t, frame.event)
}

// TestSSEHandlerAuthResolvesConnInfo verifies the identity resolved by SSEAuthFunc is what
// gets registered (group, meta) and what the callbacks receive, so the token is parsed once.
func TestSSEHandlerAuthResolvesConnInfo(t *testing.T) {
	system := newTestSystem(t, actor.WithPubSub())
	registry := gateway.NewRegistry(system, log.DiscardLogger)

	connected := make(chan *gateway.ConnInfo, 1)
	disconnected := make(chan *gateway.ConnInfo, 1)

	handler := gateway.NewSSEHandler(registry,
		gateway.WithSSEAuth(func(*http.Request) (*gateway.ConnInfo, error) {
			return &gateway.ConnInfo{
				ID:    "sse-auth-1",
				Group: "user:7",
				Meta:  map[string]string{"role": "admin"},
			}, nil
		}),
		gateway.WithSSEOnConnect(func(_ context.Context, info *gateway.ConnInfo, _ *http.Request) { connected <- info }),
		gateway.WithSSEOnDisconnect(func(info *gateway.ConnInfo) { disconnected <- info }),
		gateway.WithSSEKeepAlive(time.Hour),
	)

	server := httptest.NewServer(handler)
	defer server.Close()

	resp, cancel := openSSEStream(t, server.URL+"/", "")
	defer cancel()

	select {
	case info := <-connected:
		require.Equal(t, "sse-auth-1", info.ID)
		require.Equal(t, "user:7", info.Group)
		require.Equal(t, "admin", info.Meta["role"])
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for onConnect")
	}

	require.Equal(t, []string{"sse-auth-1"}, registry.LocalConnectionsOf("user:7"))

	result, err := registry.SendToGroup(context.Background(), "user:7", []byte("grouped"))
	require.NoError(t, err)
	require.Equal(t, 1, result.Delivered)

	reader := bufio.NewReader(resp.Body)
	frame, err := readSSEFrame(reader)
	require.NoError(t, err)
	require.Equal(t, "grouped", frame.data)

	require.NoError(t, resp.Body.Close())
	select {
	case info := <-disconnected:
		require.Equal(t, "sse-auth-1", info.ID)
		require.Equal(t, "user:7", info.Group)
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for onDisconnect")
	}
}

func TestSSEHandlerAuthRejected(t *testing.T) {
	system := newTestSystem(t)
	registry := gateway.NewRegistry(system, log.DiscardLogger)

	handler := gateway.NewSSEHandler(registry,
		gateway.WithSSEAuth(func(*http.Request) (*gateway.ConnInfo, error) {
			return nil, errors.New("no token")
		}),
	)

	server := httptest.NewServer(handler)
	defer server.Close()

	resp, err := http.Get(server.URL + "/")
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	require.Equal(t, http.StatusForbidden, resp.StatusCode)
	require.Equal(t, 0, registry.Len())
}

// TestSSEHandlerFraming pins the wire format: the retry field is announced once when the
// stream opens, every event carries an id, a configured event name is emitted, and a payload
// with newlines becomes one data line per line, as the SSE spec requires.
func TestSSEHandlerFraming(t *testing.T) {
	system := newTestSystem(t)
	registry := gateway.NewRegistry(system, log.DiscardLogger)

	handler := gateway.NewSSEHandler(registry,
		gateway.WithSSEIDFunc(func(r *http.Request) string { return r.URL.Query().Get("id") }),
		gateway.WithSSERetry(7*time.Second),
		gateway.WithSSEEventName(func([]byte) string { return "notification" }),
		gateway.WithSSEKeepAlive(time.Hour),
	)

	server := httptest.NewServer(handler)
	defer server.Close()

	resp, cancel := openSSEStream(t, server.URL+"/?id=sse-frame", "")
	defer cancel()
	defer func() { _ = resp.Body.Close() }()

	reader := bufio.NewReader(resp.Body)
	require.Equal(t, "retry: 7000", readLine(t, reader))
	require.Equal(t, "", readLine(t, reader))

	require.Eventually(t, func() bool { return registry.Has("sse-frame") }, 3*time.Second, 20*time.Millisecond)
	require.NoError(t, registry.SendToConnection(context.Background(), "sse-frame", []byte("line1\nline2")))

	require.Equal(t, "id: sse-frame-1", readLine(t, reader))
	require.Equal(t, "event: notification", readLine(t, reader))
	require.Equal(t, "data: line1", readLine(t, reader))
	require.Equal(t, "data: line2", readLine(t, reader))
	require.Equal(t, "", readLine(t, reader))
}

// TestSSEHandlerLastEventIDReplay is the reconnect case a browser produces on its own: the
// socket dies without the server noticing, three events are written into the void, and the
// EventSource comes back with a Last-Event-ID. All three must arrive, in order, before the
// live stream resumes.
func TestSSEHandlerLastEventIDReplay(t *testing.T) {
	system := newTestSystem(t)
	registry := gateway.NewRegistry(system, log.DiscardLogger)
	history := gateway.NewMemorySSEHistory(16)

	handler := gateway.NewSSEHandler(registry,
		gateway.WithSSEIDFunc(func(r *http.Request) string { return r.URL.Query().Get("id") }),
		gateway.WithSSEHistory(history),
		gateway.WithSSERetry(0),
		gateway.WithSSEKeepAlive(time.Hour),
	)

	server := httptest.NewServer(handler)
	defer server.Close()

	first, cancelFirst := openSSEStream(t, server.URL+"/?id=replay-1", "")
	defer cancelFirst()

	require.Eventually(t, func() bool { return registry.Has("replay-1") }, 3*time.Second, 20*time.Millisecond)
	require.NoError(t, registry.SendToConnection(context.Background(), "replay-1", []byte("e1")))

	reader := bufio.NewReader(first.Body)
	frame, err := readSSEFrame(reader)
	require.NoError(t, err)
	require.Equal(t, "e1", frame.data)
	require.Equal(t, "replay-1-1", frame.id)

	// From here the client is gone but its socket is not: the server keeps writing, and what
	// it writes is what the reconnect must replay.
	for _, payload := range []string{"e2", "e3", "e4"} {
		require.NoError(t, registry.SendToConnection(context.Background(), "replay-1", []byte(payload)))
	}
	require.Eventually(t, func() bool {
		events, err := history.Since(context.Background(), "replay-1", "")
		return err == nil && len(events) == 4
	}, 3*time.Second, 20*time.Millisecond)

	second, cancelSecond := openSSEStream(t, server.URL+"/?id=replay-1", "replay-1-1")
	defer cancelSecond()
	defer func() { _ = second.Body.Close() }()

	replayed := bufio.NewReader(second.Body)
	for i, want := range []string{"e2", "e3", "e4"} {
		frame, err := readSSEFrame(replayed)
		require.NoError(t, err)
		require.Equal(t, want, frame.data)
		require.Equal(t, fmt.Sprintf("replay-1-%d", i+2), frame.id)
		require.NotEqual(t, gateway.SSEGapEventName, frame.event)
	}

	// the takeover left the registry pointing at the new stream, and its ids continue past
	// the replayed ones instead of colliding with them.
	require.NoError(t, registry.SendToConnection(context.Background(), "replay-1", []byte("e5")))
	frame, err = readSSEFrame(replayed)
	require.NoError(t, err)
	require.Equal(t, "e5", frame.data)
	require.Equal(t, "replay-1-5", frame.id)

	// the replaced stream was terminated rather than left hanging.
	require.NoError(t, first.Body.Close())
	require.Equal(t, 1, registry.Len())
}

// TestSSEHandlerTeardownUsesEntryGuardedUnregister pins the fix for a naked, id-scoped
// Unregister: SSEHandler now tears a session's registration down through the entry-guarded
// ConnHandle RegisterHandle returned, not a bare Unregister(id). An id-scoped call resolves
// whatever is currently registered under an id at the moment it runs, so once a genuine
// cross-node takeover has installed a new registration under the same id, the evicted stream's
// own, asynchronously-triggered teardown must not be able to remove it.
//
// This needs a real cluster: a non-clustered actor.ActorSystem's Spawn silently hands back the
// existing PID for a name that is already running instead of erring, so a second, same-process
// Registry sharing that system would never drive spawnConnActor's ErrActorAlreadyExists retry
// loop - the takeover path that actually evicts the SSE stream through its close hook. Only in
// cluster mode does that precondition check run, which is why this uses testkit.NewMultiNodes
// (the same technique registry_evict_race_test.go uses for the analogous Registry-level test)
// instead of the single-process newTestSystem every other test in this file uses.
func TestSSEHandlerTeardownUsesEntryGuardedUnregister(t *testing.T) {
	ctx := context.Background()

	multi := testkit.NewMultiNodes(t, log.DiscardLogger, []actor.Actor{&clusterKindActor{}}, nil)
	multi.Start()
	t.Cleanup(multi.Stop)

	nodeA := multi.StartNode(ctx, "sse-entry-guard-node")
	nodeB := multi.StartNode(ctx, "sse-entry-guard-node")

	registry := gateway.NewRegistry(nodeA.ActorSystem(), log.DiscardLogger)
	takeoverRegistry := gateway.NewRegistry(nodeB.ActorSystem(), log.DiscardLogger)
	t.Cleanup(func() { _ = takeoverRegistry.Close(context.Background()) })

	handler := gateway.NewSSEHandler(registry,
		gateway.WithSSEIDFunc(func(r *http.Request) string { return r.URL.Query().Get("id") }),
		gateway.WithSSEKeepAlive(time.Hour),
	)

	server := httptest.NewServer(handler)
	defer server.Close()

	const id = "entry-guard-sse"
	resp, cancel := openSSEStream(t, server.URL+"/?id="+id, "")
	defer cancel()
	defer func() { _ = resp.Body.Close() }()

	require.Eventually(t, func() bool { return registry.Has(id) }, 3*time.Second, 20*time.Millisecond)

	// Let the cluster directory propagate node A's ownership of the actor name to node B before
	// the takeover, exactly as TestGatewayMultiNodesCrossNodeTakeover does: without this, node
	// B's Spawn can race ahead of the directory and succeed as if the name were unclaimed,
	// never driving the ErrActorAlreadyExists retry loop this test needs to exercise.
	time.Sleep(2 * time.Second)

	var newReceived atomic.Int64
	require.NoError(t, takeoverRegistry.Register(context.Background(), id, func([]byte) error {
		newReceived.Add(1)
		return nil
	}, gateway.WithReplaceExisting()))

	// The evicted stream's own teardown runs asynchronously once it observes the takeover's
	// disconnect; wait for the client to see the response end before checking what was left
	// behind.
	readErrCh := make(chan error, 1)
	go func() {
		buf := make([]byte, 64)
		for {
			if _, err := resp.Body.Read(buf); err != nil {
				readErrCh <- err
				return
			}
		}
	}()
	select {
	case <-readErrCh:
	case <-time.After(10 * time.Second):
		t.Fatal("evicted SSE stream did not terminate")
	}

	require.Eventually(t, func() bool { return !registry.Has(id) }, 5*time.Second, 20*time.Millisecond,
		"the evicted stream's own node must end up with no local entry for id")
	require.True(t, takeoverRegistry.Has(id), "the evicted stream's own teardown must not remove the takeover's registration")
	require.Equal(t, 0, registry.Len())
	require.Equal(t, 1, takeoverRegistry.Len())
	require.NoError(t, takeoverRegistry.SendToConnection(context.Background(), id, []byte("ping")))
	require.EqualValues(t, 1, newReceived.Load(), "delivery for id must reach the takeover's registration, not a resurrected old one")
}

// TestSSEHandlerDrainWaitsForRegistryUnregisterBeforeReturning pins the fix for Drain returning
// as soon as it closed the shutdown channel, without waiting for every open stream's Registry
// teardown to actually finish: a caller that proceeds to kill the process right after Drain
// returns must not be able to observe a connection's registration - its cluster-wide actor name
// and, with WithOwnerLease, its owner lease - still present.
func TestSSEHandlerDrainWaitsForRegistryUnregisterBeforeReturning(t *testing.T) {
	system := newTestSystem(t)
	registry := gateway.NewRegistry(system, log.DiscardLogger)

	handler := gateway.NewSSEHandler(registry,
		gateway.WithSSEIDFunc(func(r *http.Request) string { return r.URL.Query().Get("id") }),
		gateway.WithSSEKeepAlive(time.Hour),
	)

	server := httptest.NewServer(handler)
	defer server.Close()

	resp, cancel := openSSEStream(t, server.URL+"/?id=sse-drain-sync", "")
	defer cancel()
	defer func() { _ = resp.Body.Close() }()

	require.Eventually(t, func() bool { return registry.Has("sse-drain-sync") }, 3*time.Second, 20*time.Millisecond)

	handler.Drain()

	require.False(t, registry.Has("sse-drain-sync"), "Drain must not return until the drained stream has actually unregistered, not merely ended its HTTP response")
}

// generationalHistory wraps a real SSEHistory and additionally implements
// gateway.GenerationalHistory, so a test can drive SSEHandler's owner-lease generation
// fencing path (AppendGenerational) instead of the plain Append every other test in this file
// exercises. staleAt, when it matches the eventID of an AppendGenerational call, rejects that
// one call with gateway.ErrStaleGeneration without recording it - standing in for a backend
// that has detected the writer's generation was superseded by a takeover.
type generationalHistory struct {
	inner gateway.SSEHistory

	staleAt string

	mu          sync.Mutex
	generations []uint64
	advances    []uint64
	seq         uint64
}

func (h *generationalHistory) Append(ctx context.Context, connID, eventID string, payload []byte) error {
	return h.inner.Append(ctx, connID, eventID, payload)
}

func (h *generationalHistory) Since(ctx context.Context, connID, lastEventID string) ([]gateway.SSEEvent, error) {
	return h.inner.Since(ctx, connID, lastEventID)
}

func (h *generationalHistory) AppendGenerational(ctx context.Context, connID, eventID string, payload []byte, generation uint64) (uint64, error) {
	h.mu.Lock()
	h.generations = append(h.generations, generation)
	h.mu.Unlock()
	if eventID == h.staleAt {
		return 0, gateway.ErrStaleGeneration
	}
	if err := h.inner.Append(ctx, connID, eventID, payload); err != nil {
		return 0, err
	}
	h.mu.Lock()
	h.seq++
	seq := h.seq
	h.mu.Unlock()
	return seq, nil
}

func (h *generationalHistory) AdvanceGeneration(_ context.Context, _ string, generation uint64) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.advances = append(h.advances, generation)
	return nil
}

func (h *generationalHistory) recordedGenerations() []uint64 {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]uint64(nil), h.generations...)
}

func (h *generationalHistory) recordedAdvances() []uint64 {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]uint64(nil), h.advances...)
}

// TestSSEHandlerGenerationFencedHistoryAppendStopsStream is the P0-4 handler-side regression:
// with WithOwnerLease configured and a history backend implementing GenerationalHistory, an
// Append rejected with ErrStaleGeneration (simulating a takeover that superseded this stream's
// generation) must stop the stream instead of writing the event to the client or leaving the
// connection registered.
func TestSSEHandlerGenerationFencedHistoryAppendStopsStream(t *testing.T) {
	system := newTestSystem(t)
	coord := gateway.NewMemoryCoordinator()
	registry := gateway.NewRegistry(system, log.DiscardLogger, gateway.WithOwnerLease(coord))

	const id = "gen-fence-1"
	history := &generationalHistory{inner: gateway.NewMemorySSEHistory(16), staleAt: id + "-2"}

	handler := gateway.NewSSEHandler(registry,
		gateway.WithSSEIDFunc(func(r *http.Request) string { return r.URL.Query().Get("id") }),
		gateway.WithSSEHistory(history),
		gateway.WithSSEKeepAlive(time.Hour),
	)

	server := httptest.NewServer(handler)
	defer server.Close()

	resp, cancel := openSSEStream(t, server.URL+"/?id="+id, "")
	defer cancel()
	defer func() { _ = resp.Body.Close() }()

	require.Eventually(t, func() bool { return registry.Has(id) }, 3*time.Second, 20*time.Millisecond)

	reader := bufio.NewReader(resp.Body)

	require.NoError(t, registry.SendToConnection(context.Background(), id, []byte("e1")))
	frame, err := readSSEFrame(reader)
	require.NoError(t, err)
	require.Equal(t, "e1", frame.data)

	// This second event's AppendGenerational is configured to fail with ErrStaleGeneration, as
	// if a takeover had superseded this stream's owner-lease generation between the two writes.
	require.NoError(t, registry.SendToConnection(context.Background(), id, []byte("e2")))

	readErrCh := make(chan error, 1)
	go func() {
		buf := make([]byte, 64)
		for {
			if _, err := resp.Body.Read(buf); err != nil {
				readErrCh <- err
				return
			}
		}
	}()
	select {
	case <-readErrCh:
	case <-time.After(5 * time.Second):
		t.Fatal("stream did not stop after a stale-owner history append")
	}

	require.Eventually(t, func() bool { return !registry.Has(id) }, 3*time.Second, 20*time.Millisecond,
		"a stale-owner rejected append must tear the connection down, not just stop writing")

	events, err := history.Since(context.Background(), id, "")
	require.NoError(t, err)
	for _, e := range events {
		require.NotEqual(t, []byte("e2"), e.Payload, "an event AppendGenerational rejected as stale must never be recorded")
	}

	generations := history.recordedGenerations()
	require.NotEmpty(t, generations)
	for _, g := range generations {
		require.NotZero(t, g, "every AppendGenerational call must carry the stream's non-zero owner-lease generation once WithOwnerLease is configured")
	}
}

// TestSSEHandlerOpenAdvancesHistoryGenerationOnTakeover is the P0-3/P0-5 regression: open() must
// call GenerationalHistory.AdvanceGeneration itself, on every registration including a takeover,
// rather than leaving the shared history's generation floor to be raised only by this stream's
// own first AppendGenerational call. Before this was wired, a still-draining previous owner's
// queued write - already in flight before its takeover eviction landed - could land in the
// shared history at the old generation for as long as the new owner had not yet appended
// anything of its own, exactly the interleaved/duplicate write generation fencing exists to
// prevent.
func TestSSEHandlerOpenAdvancesHistoryGenerationOnTakeover(t *testing.T) {
	system := newTestSystem(t)
	coord := gateway.NewMemoryCoordinator()
	registry := gateway.NewRegistry(system, log.DiscardLogger, gateway.WithOwnerLease(coord))

	const id = "advance-gen-1"
	history := &generationalHistory{inner: gateway.NewMemorySSEHistory(16)}

	handler := gateway.NewSSEHandler(registry,
		gateway.WithSSEIDFunc(func(r *http.Request) string { return r.URL.Query().Get("id") }),
		gateway.WithSSEHistory(history),
		gateway.WithSSEKeepAlive(time.Hour),
	)

	server := httptest.NewServer(handler)
	defer server.Close()

	firstResp, firstCancel := openSSEStream(t, server.URL+"/?id="+id, "")
	defer firstCancel()
	defer func() { _ = firstResp.Body.Close() }()
	require.Eventually(t, func() bool { return registry.Has(id) }, 3*time.Second, 20*time.Millisecond)

	firstAdvances := history.recordedAdvances()
	require.Len(t, firstAdvances, 1, "open() must advance the history's generation floor once for a fresh registration")
	require.NotZero(t, firstAdvances[0], "the advanced generation must be the owner lease's, not zero, once WithOwnerLease is configured")

	// A second stream for the same id takes over: SSEHandler always registers with
	// WithReplaceExisting, so this is exactly the reconnect-takeover path a cross-node takeover
	// also drives.
	secondResp, secondCancel := openSSEStream(t, server.URL+"/?id="+id, "")
	defer secondCancel()
	defer func() { _ = secondResp.Body.Close() }()

	require.Eventually(t, func() bool {
		return len(history.recordedAdvances()) >= 2
	}, 3*time.Second, 20*time.Millisecond, "a takeover's open() must advance the history's generation floor again")

	advances := history.recordedAdvances()
	require.Greater(t, advances[1], advances[0], "a takeover must advance the history floor to a strictly higher generation")
}

// ttlTrackingHistory wraps a real SSEHistory and also implements the optional RefreshTTL
// capability the SSEHandler probes for on a time-based backend, so a test can observe that a
// keepalive re-arms retention. It is the in-package stand-in for ssehistory/redis, whose live
// but idle streams would otherwise expire mid-connection and answer a reconnect with a false
// gap - the divergence from MemorySSEHistory this wiring closes.
type ttlTrackingHistory struct {
	inner    gateway.SSEHistory
	refresh  atomic.Int64
	lastConn atomic.Value // string
}

func (h *ttlTrackingHistory) Append(ctx context.Context, connID, eventID string, payload []byte) error {
	return h.inner.Append(ctx, connID, eventID, payload)
}

func (h *ttlTrackingHistory) Since(ctx context.Context, connID, lastEventID string) ([]gateway.SSEEvent, error) {
	return h.inner.Since(ctx, connID, lastEventID)
}

func (h *ttlTrackingHistory) RefreshTTL(_ context.Context, connID string) error {
	h.refresh.Add(1)
	h.lastConn.Store(connID)
	return nil
}

// TestSSEHandlerKeepAliveRefreshesHistoryTTL pins the fix for a time-based SSEHistory
// (ssehistory/redis): a live but low-traffic stream that emits no real event still sends
// keepalives, and every keepalive must re-arm the backend's retention so its buffer does not
// expire under a still-connected client and produce a false gap on the next reconnect.
func TestSSEHandlerKeepAliveRefreshesHistoryTTL(t *testing.T) {
	system := newTestSystem(t)
	registry := gateway.NewRegistry(system, log.DiscardLogger)
	history := &ttlTrackingHistory{inner: gateway.NewMemorySSEHistory(16)}

	handler := gateway.NewSSEHandler(registry,
		gateway.WithSSEIDFunc(func(r *http.Request) string { return r.URL.Query().Get("id") }),
		gateway.WithSSEHistory(history),
		gateway.WithSSEKeepAlive(30*time.Millisecond),
	)

	server := httptest.NewServer(handler)
	defer server.Close()

	resp, cancel := openSSEStream(t, server.URL+"/?id=keepalive-1", "")
	defer cancel()
	defer func() { _ = resp.Body.Close() }()

	require.Eventually(t, func() bool {
		return history.refresh.Load() >= 2
	}, 3*time.Second, 20*time.Millisecond, "each keepalive must re-arm a time-based history's retention")
	require.Equal(t, "keepalive-1", history.lastConn.Load(), "RefreshTTL must name the connection being kept alive")
}

// TestSSEHandlerReplayGap covers a Last-Event-ID the history no longer retains: the client is
// told about the hole with a gateway-gap event and then gets everything that survived, rather
// than silently resuming an incomplete stream.
func TestSSEHandlerReplayGap(t *testing.T) {
	system := newTestSystem(t)
	registry := gateway.NewRegistry(system, log.DiscardLogger)
	history := gateway.NewMemorySSEHistory(2)

	handler := gateway.NewSSEHandler(registry,
		gateway.WithSSEIDFunc(func(r *http.Request) string { return r.URL.Query().Get("id") }),
		gateway.WithSSEHistory(history),
		gateway.WithSSERetry(0),
		gateway.WithSSEKeepAlive(time.Hour),
	)

	server := httptest.NewServer(handler)
	defer server.Close()

	first, cancelFirst := openSSEStream(t, server.URL+"/?id=gap-1", "")
	defer cancelFirst()

	require.Eventually(t, func() bool { return registry.Has("gap-1") }, 3*time.Second, 20*time.Millisecond)
	for _, payload := range []string{"e1", "e2", "e3", "e4"} {
		require.NoError(t, registry.SendToConnection(context.Background(), "gap-1", []byte(payload)))
	}
	require.Eventually(t, func() bool {
		events, err := history.Since(context.Background(), "gap-1", "")
		return err == nil && len(events) == 2 && events[0].ID == "gap-1-3"
	}, 3*time.Second, 20*time.Millisecond)
	require.NoError(t, first.Body.Close())

	// gap-1-1 was evicted by the two-event ring buffer.
	second, cancelSecond := openSSEStream(t, server.URL+"/?id=gap-1", "gap-1-1")
	defer cancelSecond()
	defer func() { _ = second.Body.Close() }()

	reader := bufio.NewReader(second.Body)
	frame, err := readSSEFrame(reader)
	require.NoError(t, err)
	require.Equal(t, gateway.SSEGapEventName, frame.event)
	require.Equal(t, "gap-1-1", frame.data)
	require.Empty(t, frame.id)

	for i, want := range []string{"e3", "e4"} {
		frame, err := readSSEFrame(reader)
		require.NoError(t, err)
		require.Equal(t, want, frame.data)
		require.Equal(t, fmt.Sprintf("gap-1-%d", i+3), frame.id)
	}

	// the live stream resumes after the last retained id, not on top of it.
	require.NoError(t, registry.SendToConnection(context.Background(), "gap-1", []byte("e5")))
	frame, err = readSSEFrame(reader)
	require.NoError(t, err)
	require.Equal(t, "e5", frame.data)
	require.Equal(t, "gap-1-5", frame.id)
}

// TestSSEHandlerBackpressureDrop verifies that a full outbound buffer costs the message, not
// the connection: the sender sees ErrBackpressure and the stream stays registered.
func TestSSEHandlerBackpressureDrop(t *testing.T) {
	system := newTestSystem(t)
	registry := gateway.NewRegistry(system, log.DiscardLogger)

	handler := gateway.NewSSEHandler(registry,
		gateway.WithSSEIDFunc(func(*http.Request) string { return "bp-drop" }),
		gateway.WithSSESendBuffer(1),
		gateway.WithSSEBackpressurePolicy(gateway.BackpressureDrop),
		gateway.WithSSERetry(0),
		gateway.WithSSEKeepAlive(0),
	)

	writer, done, cancel := serveBlockedSSE(t, handler)
	defer cancel()

	require.Eventually(t, func() bool { return registry.Has("bp-drop") }, 3*time.Second, 20*time.Millisecond)

	// the writer takes the first payload and blocks inside its Write, leaving the buffer to
	// absorb exactly one more.
	require.NoError(t, registry.SendToConnection(context.Background(), "bp-drop", []byte("e1")))
	writer.awaitWriting(t)
	require.NoError(t, registry.SendToConnection(context.Background(), "bp-drop", []byte("e2")))

	err := registry.SendToConnection(context.Background(), "bp-drop", []byte("e3"))
	require.ErrorIs(t, err, gateway.ErrBackpressure)
	require.True(t, registry.Has("bp-drop"))

	writer.release()
	cancel()
	<-done
	require.False(t, registry.Has("bp-drop"))
}

// TestSSEHandlerBackpressureClose verifies the opposite trade: a client that cannot keep up
// loses its stream instead of quietly losing events.
func TestSSEHandlerBackpressureClose(t *testing.T) {
	system := newTestSystem(t)
	registry := gateway.NewRegistry(system, log.DiscardLogger)

	handler := gateway.NewSSEHandler(registry,
		gateway.WithSSEIDFunc(func(*http.Request) string { return "bp-close" }),
		gateway.WithSSESendBuffer(1),
		gateway.WithSSEBackpressurePolicy(gateway.BackpressureClose),
		gateway.WithSSERetry(0),
		gateway.WithSSEKeepAlive(0),
	)

	writer, done, cancel := serveBlockedSSE(t, handler)
	defer cancel()

	require.Eventually(t, func() bool { return registry.Has("bp-close") }, 3*time.Second, 20*time.Millisecond)

	require.NoError(t, registry.SendToConnection(context.Background(), "bp-close", []byte("e1")))
	writer.awaitWriting(t)
	require.NoError(t, registry.SendToConnection(context.Background(), "bp-close", []byte("e2")))

	err := registry.SendToConnection(context.Background(), "bp-close", []byte("e3"))
	require.ErrorIs(t, err, gateway.ErrConnectionClosed)

	// the stream ends on its own, without the request context being canceled.
	writer.release()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("stream was not closed by the backpressure policy")
	}
	require.False(t, registry.Has("bp-close"))
}

// TestSSEHandlerDrainTerminatesStream verifies Drain unblocks open streams promptly (so a
// graceful shutdown is not held hostage by long-lived SSE requests) and that new requests
// after Drain fail fast with 503.
func TestSSEHandlerDrainTerminatesStream(t *testing.T) {
	system := newTestSystem(t)
	registry := gateway.NewRegistry(system, log.DiscardLogger)

	handler := gateway.NewSSEHandler(registry,
		gateway.WithSSEIDFunc(func(r *http.Request) string { return r.URL.Query().Get("id") }),
		gateway.WithSSEKeepAlive(time.Hour),
	)

	server := httptest.NewServer(handler)
	defer server.Close()

	resp, err := http.Get(server.URL + "/?id=sse-drain-1")
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	require.Eventually(t, func() bool { return registry.Has("sse-drain-1") }, 3*time.Second, 20*time.Millisecond)

	handler.Drain()

	// the streaming loop returns, the server ends the chunked response, and the client's
	// blocked read observes EOF instead of waiting on keepalives.
	readErrCh := make(chan error, 1)
	go func() {
		buf := make([]byte, 64)
		for {
			if _, readErr := resp.Body.Read(buf); readErr != nil {
				readErrCh <- readErr
				return
			}
		}
	}()
	select {
	case readErr := <-readErrCh:
		require.Error(t, readErr)
	case <-time.After(5 * time.Second):
		t.Fatal("stream was not terminated by Drain")
	}

	require.Eventually(t, func() bool {
		return !registry.Has("sse-drain-1")
	}, 3*time.Second, 50*time.Millisecond)

	// new streams are refused while draining.
	resp2, err := http.Get(server.URL + "/?id=sse-drain-2")
	require.NoError(t, err)
	defer func() { _ = resp2.Body.Close() }()
	require.Equal(t, http.StatusServiceUnavailable, resp2.StatusCode)
}

// TestSSEHandlerDisconnectOnBodyClose verifies that a client abandoning the response stream
// (resp.Body.Close(), without the request context ever being explicitly canceled) is observed
// through r.Context().Done() same as an explicit cancellation, triggering the registry
// Unregister/onDisconnect cleanup path.
func TestSSEHandlerDisconnectOnBodyClose(t *testing.T) {
	system := newTestSystem(t)
	registry := gateway.NewRegistry(system, log.DiscardLogger)

	disconnected := make(chan *gateway.ConnInfo, 1)
	handler := gateway.NewSSEHandler(registry,
		gateway.WithSSEIDFunc(func(r *http.Request) string { return r.URL.Query().Get("id") }),
		gateway.WithSSEOnDisconnect(func(info *gateway.ConnInfo) { disconnected <- info }),
		gateway.WithSSEKeepAlive(time.Hour),
	)

	server := httptest.NewServer(handler)
	defer server.Close()

	resp, err := http.Get(server.URL + "/?id=sse-close")
	require.NoError(t, err)

	require.Eventually(t, func() bool { return registry.Has("sse-close") }, 3*time.Second, 20*time.Millisecond)

	require.NoError(t, resp.Body.Close())

	select {
	case info := <-disconnected:
		require.Equal(t, "sse-close", info.ID)
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for disconnect callback after client body close")
	}
	require.False(t, registry.Has("sse-close"))
}

// TestSSEHandlerReauthFailureTerminates verifies a periodic reauthentication that starts
// failing ends the stream with a terminating comment and unregisters the connection.
func TestSSEHandlerReauthFailureTerminates(t *testing.T) {
	system := newTestSystem(t)
	registry := gateway.NewRegistry(system, log.DiscardLogger)

	handler := gateway.NewSSEHandler(registry,
		gateway.WithSSEIDFunc(func(r *http.Request) string { return r.URL.Query().Get("id") }),
		gateway.WithSSEKeepAlive(time.Hour),
		gateway.WithSSEReauth(100*time.Millisecond, func(*http.Request) (*gateway.ConnInfo, error) {
			return nil, errors.New("token expired")
		}),
	)

	server := httptest.NewServer(handler)
	defer server.Close()

	resp, cancel := openSSEStream(t, server.URL+"/?id=revoked", "")
	defer cancel()
	defer func() { _ = resp.Body.Close() }()

	require.Eventually(t, func() bool { return registry.Has("revoked") }, 3*time.Second, 20*time.Millisecond)

	reader := bufio.NewReader(resp.Body)
	sawDisconnect := false
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			break
		}
		if strings.HasPrefix(line, ": disconnect") {
			sawDisconnect = true
		}
	}
	require.True(t, sawDisconnect, "expected a terminating disconnect comment before EOF")
	require.Eventually(t, func() bool { return !registry.Has("revoked") }, 3*time.Second, 20*time.Millisecond)
}

// TestSSEHandlerDisconnectTerminates verifies Registry.Disconnect ends a locally held stream
// with a terminating comment carrying the reason, and that the stream unregisters.
func TestSSEHandlerDisconnectTerminates(t *testing.T) {
	system := newTestSystem(t)
	registry := gateway.NewRegistry(system, log.DiscardLogger)

	handler := gateway.NewSSEHandler(registry,
		gateway.WithSSEIDFunc(func(r *http.Request) string { return r.URL.Query().Get("id") }),
		gateway.WithSSEKeepAlive(time.Hour),
	)

	server := httptest.NewServer(handler)
	defer server.Close()

	resp, cancel := openSSEStream(t, server.URL+"/?id=kickme", "")
	defer cancel()
	defer func() { _ = resp.Body.Close() }()

	require.Eventually(t, func() bool { return registry.Has("kickme") }, 3*time.Second, 20*time.Millisecond)
	require.NoError(t, registry.Disconnect(context.Background(), "kickme", "policy update"))

	reader := bufio.NewReader(resp.Body)
	var reason string
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			break
		}
		if after, ok := strings.CutPrefix(line, ": disconnect "); ok {
			reason = strings.TrimRight(after, "\r\n")
		}
	}
	require.Equal(t, "policy update", reason)
	require.Eventually(t, func() bool { return !registry.Has("kickme") }, 3*time.Second, 20*time.Millisecond)
}

// sseFrame is one parsed SSE event.
type sseFrame struct {
	id    string
	event string
	data  string
}

// readSSEFrame reads lines until a complete event (terminated by a blank line) is assembled,
// skipping comments and the retry field.
func readSSEFrame(reader *bufio.Reader) (sseFrame, error) {
	var (
		frame sseFrame
		data  []string
		open  bool
	)
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			return sseFrame{}, err
		}
		line = strings.TrimRight(line, "\r\n")
		switch {
		case line == "":
			if !open {
				continue // separator of a frame we skipped (e.g. retry)
			}
			frame.data = strings.Join(data, "\n")
			return frame, nil
		case strings.HasPrefix(line, ":"):
			continue
		case strings.HasPrefix(line, "retry: "):
			continue
		case strings.HasPrefix(line, "id: "):
			frame.id = strings.TrimPrefix(line, "id: ")
			open = true
		case strings.HasPrefix(line, "event: "):
			frame.event = strings.TrimPrefix(line, "event: ")
			open = true
		case strings.HasPrefix(line, "data: "):
			data = append(data, strings.TrimPrefix(line, "data: "))
			open = true
		}
	}
}

// readLine reads one raw line of the stream, without the trailing newline.
func readLine(t *testing.T, reader *bufio.Reader) string {
	t.Helper()
	line, err := reader.ReadString('\n')
	require.NoError(t, err)
	return strings.TrimRight(line, "\r\n")
}

// openSSEStream opens a streaming request whose context the caller cancels on cleanup, so an
// abandoned stream does not leak. A non-empty lastEventID is sent as the header a browser's
// EventSource sends on reconnect.
func openSSEStream(t *testing.T, url, lastEventID string) (*http.Response, context.CancelFunc) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		cancel()
		require.NoError(t, err)
	}
	if lastEventID != "" {
		req.Header.Set("Last-Event-ID", lastEventID)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		cancel()
		require.NoError(t, err)
	}
	require.Equal(t, http.StatusOK, resp.StatusCode)
	return resp, cancel
}

// blockingResponseWriter is an http.ResponseWriter whose Write blocks until released. It is
// the only way to hold the writer goroutine still long enough for the outbound buffer to fill
// deterministically, which is what the backpressure policies act on.
type blockingResponseWriter struct {
	header  http.Header
	writing chan struct{}
	gate    chan struct{}
	gateOne sync.Once

	mu   sync.Mutex
	body bytes.Buffer
}

func newBlockingResponseWriter() *blockingResponseWriter {
	return &blockingResponseWriter{
		header:  make(http.Header),
		writing: make(chan struct{}, 1),
		gate:    make(chan struct{}),
	}
}

func (w *blockingResponseWriter) Header() http.Header { return w.header }
func (w *blockingResponseWriter) WriteHeader(int)     {}
func (w *blockingResponseWriter) Flush()              {}

func (w *blockingResponseWriter) Write(p []byte) (int, error) {
	select {
	case w.writing <- struct{}{}:
	default:
	}
	<-w.gate

	w.mu.Lock()
	defer w.mu.Unlock()
	return w.body.Write(p)
}

// awaitWriting blocks until the handler's writer goroutine has entered a Write.
func (w *blockingResponseWriter) awaitWriting(t *testing.T) {
	t.Helper()
	select {
	case <-w.writing:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for the SSE writer to start writing")
	}
}

func (w *blockingResponseWriter) release() {
	w.gateOne.Do(func() { close(w.gate) })
}

// serveBlockedSSE runs handler.ServeHTTP against a response writer whose writes are gated by
// the test. The returned channel is closed once ServeHTTP returns.
func serveBlockedSSE(t *testing.T, handler *gateway.SSEHandler) (*blockingResponseWriter, <-chan struct{}, context.CancelFunc) {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	req := httptest.NewRequest(http.MethodGet, "/", nil).WithContext(ctx)
	writer := newBlockingResponseWriter()

	done := make(chan struct{})
	go func() {
		defer close(done)
		handler.ServeHTTP(writer, req)
	}()

	t.Cleanup(func() {
		cancel()
		writer.release()
		<-done
	})
	return writer, done, cancel
}
