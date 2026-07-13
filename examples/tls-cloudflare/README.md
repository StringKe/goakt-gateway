# examples/tls-cloudflare

## What this demonstrates

This example wires `gateway.Manager` for the two TLS deployment shapes that show up
behind Cloudflare, and runs both of them end to end in one process:

1. **Per-domain Cloudflare Origin CA issuance, shared across a cluster.** Several
   processes each hold their own `gateway.Manager`; all of them point at the same
   `coordinator/redis` `Coordinator`. When they race to `EnsureCertificate` the same
   domain, `Manager`'s `Coordinator.TryLock` arbitration ensures only one of them ever
   calls the certificate issuer - the others get the winner's certificate back through
   the Coordinator. `WithDomainPolicy` gates which domains may be issued for at all,
   standing in for a "which custom domains has a tenant bound" database lookup.
2. **A single catch-all certificate behind Authenticated Origin Pulls mTLS.** One
   `Manager` configured with nothing but `WithFallbackCertificate` serves the same
   certificate regardless of SNI - the shape a Cloudflare-terminated edge needs at the
   origin. The certificate itself comes from `NewReloadingCertificate`, which polls a
   certificate/key file pair on disk and hot-reloads it, the same mechanism a
   Kubernetes-mounted TLS secret rotation relies on. `WithAuthenticatedOriginPulls` layers
   mTLS on top, so only connections presenting a client certificate that chains to the
   configured CA are accepted - the origin no longer has to trust "whoever connected to
   port 443" once its IP leaks.

Concretely, running it prints, and checks with `log.Fatalf` on any deviation:

- Three simulated processes resolving the same domain to byte-identical certificates,
  and the certificate issuer's call counter staying at 1.
- A freshly built `Manager` with **no issuer at all**, sharing the same `CertStore`,
  serving that same certificate by reading it back from the store - the cold-start case a
  process hits after the domain was already issued elsewhere. With `store/redis` this
  survives a full deployment restart, because the certificate lives on the server rather
  than in one process's memory.
- A domain that was never bound being refused, twice, without the issuer's call counter
  moving - proving `WithDomainPolicy` gates issuance, and that a refusal is served from
  the negative cache on the second lookup rather than re-consulting the policy.
- A TLS connection with no client certificate being rejected.
- A TLS connection with a client certificate from an unrelated CA being rejected (for a
  different, correctly reported reason: "unknown certificate authority" rather than "no
  certificate", proving the mTLS check actually inspected the certificate rather than
  just noticing its absence).
- A TLS connection with a client certificate signed by the configured Origin Pulls CA
  being accepted.
- The catch-all certificate changing file content on disk and being served under the new
  fingerprint without the process restarting.

## What this does not demonstrate

- **A real Cloudflare Origin CA issuance.** Without `CLOUDFLARE_ORIGIN_CA_KEY` set, the
  issuer is a `gateway.StaticIssuer` serving a certificate this example generated and
  self-signed locally. The `CloudflareOriginCAIssuer` wiring (`buildIssuer` in main.go)
  is real and unconditional once the token is set - this example just cannot exercise
  Cloudflare's actual API without an account and network access to it.
- **A handshake accepted by Cloudflare's real Authenticated Origin Pulls CA.** Only
  Cloudflare holds the private key for that CA; nobody outside Cloudflare can mint a
  certificate that chains to it. The example generates its own demo CA and its own demo
  client certificate to prove the *mechanism* (mTLS enforcement via
  `AuthenticatedOriginPulls`) works, then separately shows, in `runOriginPullsAndFallbackDemo`'s comments, how to point `CAPEM` at Cloudflare's actual bundle
  in production.
- **Renewal.** No `Manager.Start`/`Stop` is called anywhere in this example: nothing here
  exercises the cron-driven renewal schedule. That is unrelated to the sharing/policy/
  mTLS/fallback behavior this example is about, and is already exercised by the
  package's own tests.
- **A publicly trusted origin certificate.** See the warning below - do not use Origin CA
  output as if it were a certificate a browser would accept directly.

## Which shape should I use?

| | Per-domain Origin CA issuance | Catch-all + Authenticated Origin Pulls |
|---|---|---|
| Certificate identity | One certificate per hosted domain, correct SAN per tenant | One certificate for the whole fleet; SNI is not even consulted |
| Where TLS terminates | This process, per domain, dynamically by SNI | This process, but the *interesting* trust decision (which domain is this?) already happened at Cloudflare's edge before the request reached here |
| Where identity of the caller is checked | Nowhere - anyone who can reach the origin's IP and send the right SNI gets served | At the origin, via mTLS: only Cloudflare's edge (or anything else holding a certificate signed by the Origin Pulls CA) can complete a handshake at all |
| Best fit | Direct-to-origin TLS termination, or a Cloudflare Custom Hostname setup where each tenant's own domain must present its own certificate | Cloudflare SaaS Custom Hostnames / any setup where Cloudflare's edge is the only intended caller and the origin should refuse everyone else outright, even someone who discovers the origin IP |
| What this example wires it with | `WithCertIssuer` + `WithDomainPolicy`/`WithAllowedDomains` + a shared `Coordinator` | `WithFallbackCertificate` (backed by `NewReloadingCertificate`) + `WithAuthenticatedOriginPulls` |

They are not mutually exclusive - a `Manager` can hold a `CertIssuer` for domains it
recognizes *and* a `WithFallbackCertificate` for everything else, and either can sit
behind `WithAuthenticatedOriginPulls`. This example keeps them in two separate `Manager`s
purely so each demo's output is easy to attribute to one mechanism, mirroring the
`edge_tls.go`-style "Cloudflare edge + single catch-all + Authenticated Origin Pulls"
shape a real Cloudflare-fronted deployment (mip-aio) uses in production - that specific
deployment does not use per-domain Origin CA issuance at all, which is why the first
shape is demonstrated here on its own, independent of the second.

## Warning: Origin CA certificates are not publicly trusted

A certificate issued by Cloudflare's Origin CA (or, in this example, the local demo CA
that stands in for it when `CLOUDFLARE_ORIGIN_CA_KEY` is not set) is only ever trusted by
Cloudflare's edge, because Cloudflare is the only party configured to trust that CA's
root. A browser, curl, or any other client connecting directly to the origin - bypassing
Cloudflare - will reject it as untrusted (or, for the demo CA, as outright unknown). Do
not deploy an Origin CA certificate as if it were a publicly trusted one; it is only
correct when every connection to the origin is guaranteed to have come through
Cloudflare's proxy, which is precisely what `WithAuthenticatedOriginPulls` in this
example is there to enforce rather than merely assume.

## Running it

```sh
go run ./examples/tls-cloudflare
```

No environment variables, no Redis, and no Cloudflare account are required: it falls
back to a locally generated demo certificate authority, an in-process
`gateway.NewMemoryCoordinator`, and an on-disk `FileCertStore` in a temp directory, and
still runs every demo to completion.

### With a real Redis or Valkey, to see the Coordinator and CertStore actually shared

```sh
docker compose up -d                                   # repo root: redis :6399, valkey :6400
REDIS_ADDR=127.0.0.1:6399 go run ./examples/tls-cloudflare  # or :6400 for Valkey
```

The "coordinator:" and "cert store:" lines printed at startup say which backends it
picked. When `REDIS_ADDR` is set and reachable this run uses **two** of the shared
Redis / Valkey backends against one instance: `coordinator/redis` for the issuance lock and
distribution, and `store/redis` for certificate persistence (both under the same
`goakt-gateway-example:` key prefix - the store's keys carry a `cert:` infix so they never
collide with the coordinator's). The Redis-backed run is what makes the shared-issuance
demo a real cross-client test rather than one shared in-process value, and it makes the
cold-start demo a genuine "read it back from the server" rather than "read it back from a
temp file". Either a Redis or a Valkey server works identically; that is the point of the
two docker-compose services.

### With a real Cloudflare Origin CA account

```sh
CLOUDFLARE_ORIGIN_CA_KEY=<your Origin CA key> go run ./examples/tls-cloudflare
```

The "issuer:" line printed at startup confirms which issuer is in use. With a real key,
the shared-issuance demo performs one genuine call to Cloudflare's Origin CA API for
`app.example.com` (`RequestCertificate: gateway.NewRSACertificateRequest(2048)`) and
serves that certificate to all three simulated processes.

## What success looks like

The program prints one line per step and exits 0 with a final `done` line. Every
assertion in it is a `log.Fatalf` on failure, so **the process exiting non-zero, or
stopping before printing `done`, means something is broken** - there is no separate pass/
fail summary to check.

Two categories of stderr noise are expected and are not failures:

- `http: TLS handshake error from 127.0.0.1:PORT: tls: client didn't provide a
  certificate` / `... x509: certificate signed by unknown authority` - these are
  `net/http`'s own logging of the two rejected handshakes the example deliberately
  triggers to prove Authenticated Origin Pulls works. The corresponding
  `connection with ... correctly rejected: ...` line from the example itself is the one
  that matters.
- Nothing about Redis, if `REDIS_ADDR` was not set or is unreachable - the example's own
  `coordinator: in-process MemoryCoordinator (Redis at ... unreachable: ...)` line reports
  that once, and go-redis's own background connection logging is disabled
  (`logging.Disable()`) so it does not also spam stderr independently.

A representative successful run (no Redis, no `CLOUDFLARE_ORIGIN_CA_KEY`):

```
coordinator: in-process MemoryCoordinator (Redis at localhost:6379 unreachable: context deadline exceeded)
cert store: on-disk FileCertStore at /tmp/goakt-gateway-tls-cloudflare-certs-3882034732 (Redis at localhost:6379 unreachable: context deadline exceeded)
issuer: static demo certificate (CLOUDFLARE_ORIGIN_CA_KEY not set)
=== shared issuance across simulated processes ===
3 processes resolved "app.example.com" to the identical certificate (sha256 f221c8831641...), issuer.Issue called 1 time(s)
EnsureCertificate("unbound.example.com") refused as expected: gateway: domain not allowed
EnsureCertificate("unbound.example.com") refused as expected: gateway: domain not allowed
unbound domain requested twice: domain policy invoked 1 time(s) for it (second lookup served from the negative cache), issuer.Issue still called 1 time(s) total
=== cold start from the persistent cert store (no issuer) ===
cold-started Manager served "app.example.com" from the store (sha256 f221c8831641...) with no issuer configured
=== fallback certificate + Authenticated Origin Pulls ===
http: TLS handshake error from 127.0.0.1:54794: tls: client didn't provide a certificate
http: TLS handshake error from 127.0.0.1:54795: tls: client didn't provide a certificate
connection with no client certificate correctly rejected: Get "https://127.0.0.1:54792/": remote error: tls: certificate required
http: TLS handshake error from 127.0.0.1:54796: tls: failed to verify certificate: x509: certificate signed by unknown authority
connection with client certificate from an untrusted CA correctly rejected: Get "https://127.0.0.1:54792/": remote error: tls: unknown certificate authority
connection with client certificate signed by the configured Origin Pulls CA correctly accepted
catch-all certificate hot-reloaded: sha256 ea2eae1d11a9... -> ea21a19a5880... (no restart)
done
```

## Files

- `main.go` - the runnable demo: coordinator/cert-store/issuer selection (`buildCoordinator`,
  `buildCertStore`, `buildIssuer`), the shared-issuance + domain policy walkthrough
  (`runSharedIssuanceDemo`), the cold-start-from-store walkthrough (`runColdStartDemo`), and
  the fallback-certificate + Authenticated Origin Pulls walkthrough (including the hot-reload
  proof).
- `demo_certs.go` - self-contained certificate generation (`newDemoCA`, `newDemoLeaf`)
  used everywhere this example needs a certificate authority or a signed certificate that
  it cannot obtain from Cloudflare offline. Nothing it produces is meant to be trusted by
  anything other than this example's own client dials.
