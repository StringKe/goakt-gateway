# delivery-confirm example

## Why this exists

`Registry.SendToConnection` and `Registry.SendToGroup` both have a cross-node delivery path:
when the target connection is not held by the node handling the request, the Registry
resolves it through the cluster actor directory and hands the payload to a `connActor` on
whatever node actually holds the socket. By default that hand-off is `actor.Tell` -
fire-and-forget. The sending node learns nothing beyond "the message was queued for
delivery"; it does not know whether the owning node's socket write actually succeeded, or
even whether the owning node is still alive.

`gateway.WithDeliveryConfirmation()` changes that hand-off to `actor.Ask`: the sending node
waits for the owning node's `connActor` to reply with whether it wrote the payload to the
socket's outbound buffer. This buys real information - a failed write comes back as
`gateway.ErrConnectionClosed`, an unresponsive owner comes back as
`gateway.ErrConfirmationTimeout` - at the cost of one extra network round trip per remote
delivery, held open for up to `WithConfirmationTimeout` (default 5s) before giving up.

That is a real latency/certainty trade-off, and it is opt-in for exactly that reason: an
application that broadcasts thousands of chat messages a second wants the default
fire-and-forget path, and an application that must know a push actually reached a device -
say, a "kick this session" admin action - wants the confirmed one. This example puts both
paths side by side against the same connection so the difference is a number you can read,
not just a paragraph you have to take on faith.

## How the demo is built

One OS process runs two real GoAkt cluster members:

- **Node A** (`http://127.0.0.1:18087`) accepts a browser's WebSocket connection and holds
  it. It never sends anything on its own; it only receives.
- **Node B** (`http://127.0.0.1:18088`) never accepts a WebSocket connection. Every payload
  it sends targets a connection id that lives on node A, so `SendToConnection` always takes
  the remote path (`Registry.deliverRemote`) - there is no local fast path to fall back to
  and mask the comparison.

Node B holds **two** `*gateway.Registry` values wired to the same actor system: one built
with the package default, one built with `gateway.WithDeliveryConfirmation()` (and a
`WithConfirmationTimeout` of 3s). Every request handler on node B picks between them by a
`confirm` flag, so a plain call and a confirmed call to the same connection differ in
exactly one thing: which Registry made them.

The two nodes also share a single `gateway.MemoryPresence` instance (an in-process
shortcut for what a real deployment would run as `presence/redis`) purely so the
`SendToGroup` demo (section 3 below) can exercise `Registry.confirmRemoteGroup` and show
`DeliveryResult.Remote` actually change meaning, not just `SendToConnection`'s return value.

## What each section of the page shows

- **1. Single send** - one `SendToConnection` call in each mode, immediate JSON result.
  Confirms both modes deliver the message (watch node A's page log) and shows a real, if
  small, per-call timing.
- **2. Compare latency** - the same burst of `SendToConnection` calls (200 by default) run
  back to back through the plain Registry, then through the confirmed one, timed on the Go
  side (`time.Since` wrapping the call), and reported as an average microseconds-per-call
  table. A single call's latency on a loopback socket is too close to Go's own scheduling
  noise to read; averaging over a couple hundred is not.
- **3. SendToGroup** - one call to `Registry.SendToGroup` in each mode, showing
  `DeliveryResult.Remote` and what it means under each mode (see below).

## Reading the numbers

A real run against a single remote connection, on a MacBook, loopback only:

```
plain (fire-and-forget):        68  microseconds/call  (100 calls)
confirmed (WithDeliveryConfirmation): 106 microseconds/call  (100 calls)
```

The absolute numbers are meaningless (they are dominated by Go scheduling and loopback
syscall overhead, not anything resembling production network latency) - what matters is
that the confirmed path is consistently and measurably slower, by roughly the cost of one
extra actor mailbox round trip. In a real cluster spread across availability zones, that gap
is one real network round trip: single-digit milliseconds within a region, tens of
milliseconds across regions. `WithConfirmationTimeout` bounds the worst case, not the
typical one - budget for the p99 round trip to the slowest node you expect to run.

`DeliveryResult.Remote` from the `SendToGroup` section reads `1` in both modes for this
demo (there is exactly one remote group member), but what it *means* changes:

- **Plain**: `Remote` counts fan-outs this node published to the group's cluster topic. It
  is 1 as soon as the publish succeeds, regardless of what the owning node did with it -
  including if that node crashed the instant before delivery.
- **Confirmed**: `Remote` counts members whose owning node acknowledged the socket write.
  If the owning node is gone, `Remote` is 0 (not 1), and `DeliveryResult.None()` correctly
  reports the group as unreachable instead of over-counting a delivery that never happened.

Send to a connection id nobody holds (any id you have not opened node A's page with) and
both modes return `gateway.ErrConnectionNotFound` identically fast - that error is raised
before the plain/confirmed fork (the cluster actor directory has no entry to resolve at
all), so it is not part of what this demo is contrasting. The contrast starts once the
target connection genuinely exists somewhere in the cluster: only then does WithDeliveryConfirmation's actor.Ask round trip actually happen, which is why sections 1-3
require you to hold node A's page open first.

## Running it

```
go run ./examples/delivery-confirm
```

Startup takes a few seconds: two cluster members need to gossip-converge (mirroring
`examples/cluster`'s same pause) before node B's actor directory lookups can resolve a
connection registered on node A.

1. Open `http://127.0.0.1:18087/` in a browser. Note the connection id it displays (also
   persisted in `localStorage`, so reloading the tab keeps the same id).
2. Open `http://127.0.0.1:18088/` in a second tab - node A's page links to it with the id
   pre-filled via `?id=`.
3. On node B's page: paste the id if it was not carried over, then use sections 1-3. Watch
   node A's page log fill in as messages arrive.

Or drive it with `curl` once you have a connection id from node A's page (`alice` below is
whatever id node A's page shows you):

```bash
# Section 1 equivalent: a single send in each mode.
curl -s -X POST http://127.0.0.1:18088/api/send \
  -d '{"id":"alice","msg":"hi","confirm":false,"calls":1}'
curl -s -X POST http://127.0.0.1:18088/api/send \
  -d '{"id":"alice","msg":"hi","confirm":true,"calls":1}'

# Section 2 equivalent: the latency comparison.
curl -s -X POST http://127.0.0.1:18088/api/compare \
  -d '{"id":"alice","msg":"bench","calls":200}'

# Section 3 equivalent: SendToGroup under each mode.
curl -s -X POST http://127.0.0.1:18088/api/broadcast \
  -d '{"id":"alice","msg":"group message","confirm":false}'
curl -s -X POST http://127.0.0.1:18088/api/broadcast \
  -d '{"id":"alice","msg":"group message","confirm":true}'

# A target nobody holds: both modes fail identically, before the fork this demo compares.
curl -s -X POST http://127.0.0.1:18088/api/send \
  -d '{"id":"nobody","msg":"x","confirm":false,"calls":1}'
# {"mode":"plain (fire-and-forget)","calls":1,"succeeded":0,"totalMicros":...,"avgMicros":...,"lastError":"gateway: connection not found"}
```

## Success criteria

- `go vet ./examples/delivery-confirm/...` is clean.
- With node A's page open, `/api/send` in both modes returns `succeeded:1` and node A's page
  log gains a line for each; the confirmed call's `avgMicros` is close to or larger than the
  plain call's (small individual calls are noisy - `/api/compare` is the reliable signal).
- `/api/compare` returns `plain.avgMicros` consistently lower than `confirm.avgMicros` across
  repeated runs, and both `succeeded` fields equal `calls`.
- `/api/broadcast` returns `remote:1, none:false` in both modes while node A's connection is
  open, and node A's page log gains one line per call.
- `/api/send` against an id nobody holds returns `gateway.ErrConnectionNotFound`'s message in
  `lastError` for both modes, with `succeeded:0`.

## What this does NOT demonstrate

A failure genuinely local to the connection-owning node (e.g. its socket write itself
failing under backpressure) is not staged here: reliably forcing that without touching
library internals would need either a very small `WithWSSendBuffer` and a precisely timed
burst, or a client that stops reading mid-stream, and getting either deterministic enough for
a "run it and see" demo (rather than a flaky race) was out of scope. What the demo does show
- the round-trip cost, and `DeliveryResult.Remote`'s change in meaning - is exactly what
`WithDeliveryConfirmation`'s doc comment promises; a genuinely dead owning node is better
demonstrated by killing one of `examples/cluster`'s three node processes outright.
