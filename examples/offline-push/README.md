# offline-push example

## Why this exists

`examples/notification` shows the pattern every caller of `SendToGroup` used to have to
implement by hand: call `SendToGroup`, inspect the returned `DeliveryResult`, and if
`result.None()` is true, remember to call your own offline transport yourself. That
worked, but it put a correctness burden on every call site - forget the `if result.None()`
branch once, in one handler, and that one notification path silently never falls back to
push. There was no way for the library to catch the omission because the library never
saw the decision at all.

`Registry.WithOfflineChannel` removes that burden. You configure one `gateway.OfflineChannel`
on the `Registry` once, and `SendToGroup` makes the online/offline decision itself:

- A group with a live socket somewhere gets the payload written directly. The offline
  channel is never touched.
- A group with `DeliveryResult.None()` - no live socket anywhere in the cluster - is
  handed to the configured `OfflineChannel` automatically, on the Registry's own
  goroutine, without the caller's code branching on the result at all.

This example wires that up for real: `Registry.SendToGroup` is configured with
`gateway.WithOfflineChannel(offlineChannel)`, and `offlineChannel` is a genuine
`offline/webpush.Channel` - real VAPID signing, real RFC 8291 encryption, a real HTTP POST
- talking to a local fake push service that stands in for FCM/Mozilla autopush. Nothing in
`/notify`'s handler ever calls the push channel; it only calls `SendToGroup` and reports
back what came out.

## What to look at

- `main.go`'s `notifyHandler` - it is a plain `SendToGroup` call and a JSON encode of the
  result. Compare it to `examples/notification/main.go`'s `notifyHandler`, which has an
  explicit `if result.None() { fakeWebPush(...) }` branch. That branch is gone here
  because the Registry does it internally now.
- `main.go`'s `fallbackObserver` - it implements the optional `gateway.OfflineObserver`
  extension (`OfflineFallback(group string, err error)`), which is how an application
  observes the outcome of a fallback that `SendToGroup` fired asynchronously and cannot
  report through its own return value. `/status` polls a buffer this observer fills.
- `generateDemoSubscriptionKeys` - since there is no real browser in this demo to call
  `PushManager.subscribe()`, this function mints a fresh P-256 key pair and auth secret in
  exactly the shape `PushSubscription.getKey('p256dh'/'auth')` would hand an application
  server. `/subscribe` uses it to register a subscription the same way a real
  `/api/push-subscribe` endpoint would persist a browser-supplied one.
- `newFakePushService` - an `httptest.Server` standing in for FCM/Mozilla autopush. It
  cannot decrypt the payload (only the "browser" holding the subscription's private key
  could), so it only proves the POST was genuine - non-empty encrypted body, `TTL` header,
  VAPID `Authorization` header - before answering `201 Created`. Server logs show every
  push it receives.

## What this does NOT demonstrate

Single `actor.ActorSystem`, no real cluster - identical scope caveat to
`examples/notification`. `DeliveryResult.None()` here is exact because there is exactly
one node and `gateway.NewMemoryPresence()` gives it a complete local view; it says nothing
new about `Remote`/cross-node accuracy, which is `examples/cluster`'s territory.

## Running it

```
go run ./examples/offline-push/
```

Listens on `http://127.0.0.1:8085`. Open that URL in a browser for the demo page, or drive
it with `curl`:

```bash
# 1. Notify a user with no connection and no push subscription: SendToGroup still reports
#    None and still routes to the OfflineChannel - it has no way to know in advance that
#    alice has zero subscriptions - but webpush.Channel.Deliver is then a no-op for her
#    group (empty subscription list), so no HTTP call ever leaves the process.
curl -s -X POST http://127.0.0.1:8085/notify -d '{"User":"alice","Msg":"hi"}'
# {"delivered":0,"dropped":0,"remote":0,"none":true}
# The server log gets an [offline-fallback] success=true line (the no-op "succeeded"
# trivially) but no [push-service] line, since there was nothing to POST to.

# 2. Register a (simulated) push subscription for alice, then notify again.
curl -s -X POST http://127.0.0.1:8085/subscribe -d '{"User":"alice"}'
curl -s -X POST http://127.0.0.1:8085/notify -d '{"User":"alice","Msg":"hello offline"}'
# {"delivered":0,"dropped":0,"remote":0,"none":true}
# none=true again - SendToGroup still found no live socket - but this time the Registry's
# configured OfflineChannel fires in the background. Watch the server's stdout:
#   [push-service] received encrypted push for /push/alice/<uuid>: NNNN bytes, ttl=60, auth=true
#   [offline-fallback] group="user:alice" success=true err=<nil>
# or poll it over HTTP:
curl -s http://127.0.0.1:8085/status
# [{"group":"user:alice","success":true,"at":"...")}]

# 3. Connect alice over WebSocket (open http://127.0.0.1:8085/ in a browser, section 1,
#    user "alice"), then notify her again:
curl -s -X POST http://127.0.0.1:8085/notify -d '{"User":"alice","Msg":"hello online"}'
# {"delivered":1,"dropped":0,"remote":0,"none":false}
# delivered=1, none=false: the message reached alice's socket directly. No new
# [push-service] or [offline-fallback] log line appears - the offline channel was never
# touched, exactly as the doc comment on WithOfflineChannel says it should be.
```

## Success criteria

- Step 1 returns `none:true`, produces an `[offline-fallback] success=true` line (the
  no-op Deliver call), but no `[push-service]` line (no subscription registered yet, so
  the channel has nothing to POST to).
- Step 2 returns `none:true` and, within about a second, both a `[push-service]` line
  (proving a real encrypted POST left the process) and an `[offline-fallback]
  success=true` line (proving `SendToGroup` routed it there on its own) appear in the
  server log and in `GET /status`.
- Step 3 returns `delivered:1, none:false` and produces neither log line, and the
  connected WebSocket client (browser demo page section "WebSocket log", or any `ws`
  client dialing `/ws?user=alice`) receives the literal message text.

`go vet ./examples/offline-push/...` is clean.
