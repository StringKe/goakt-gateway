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
	"crypto/tls"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/net/websocket"

	"github.com/tochemey/goakt/v4/log"

	gateway "github.com/StringKe/goakt-gateway"
)

// TestNewServerRequiresTLSManagerForOriginPulls verifies that WithAuthenticatedOriginPulls
// without WithTLSManager is rejected at construction time rather than silently serving
// plaintext with no mTLS verification.
func TestNewServerRequiresTLSManagerForOriginPulls(t *testing.T) {
	pulls := &gateway.AuthenticatedOriginPulls{CAPEM: []byte("irrelevant")}
	_, err := gateway.NewServer(":0", http.NewServeMux(), gateway.WithAuthenticatedOriginPulls(pulls))
	require.Error(t, err)
}

// TestNewServerOriginPullRequiresValidCA verifies that an invalid Authenticated Origin
// Pulls CA is rejected at construction time even when a TLS manager is configured.
func TestNewServerOriginPullRequiresValidCA(t *testing.T) {
	system := newTestSystem(t)
	manager := gateway.NewManager(system, log.DiscardLogger, gateway.WithRenewInterval(""))
	pulls := &gateway.AuthenticatedOriginPulls{CAPEM: []byte("not a cert")}

	_, err := gateway.NewServer(":0", http.NewServeMux(),
		gateway.WithTLSManager(manager),
		gateway.WithAuthenticatedOriginPulls(pulls),
	)
	require.Error(t, err)
}

// TestNewServerWithTLSManagerAndOriginPulls verifies the valid combination succeeds.
func TestNewServerWithTLSManagerAndOriginPulls(t *testing.T) {
	system := newTestSystem(t)
	manager := gateway.NewManager(system, log.DiscardLogger, gateway.WithRenewInterval(""))
	ca := newTestCA()
	pulls := &gateway.AuthenticatedOriginPulls{CAPEM: ca.certPEM}

	srv, err := gateway.NewServer(":0", http.NewServeMux(),
		gateway.WithTLSManager(manager),
		gateway.WithAuthenticatedOriginPulls(pulls),
	)
	require.NoError(t, err)
	require.NotNil(t, srv)
}

// TestServerListenAndServeTLSShutdown verifies Shutdown's actual, documented scope: it
// stops accepting new connections and returns http.ErrServerClosed from ListenAndServe,
// and any new connection attempt after that point is rejected.
//
// It deliberately does NOT assert that an already-established WebSocket connection gets
// closed by Shutdown: golang.org/x/net/websocket accepts the connection via
// http.Hijacker, and the standard library documents that "Shutdown does not attempt to
// close nor wait for hijacked connections such as WebSockets" - confirmed against this
// exact handler by a standalone experiment before writing this test. gateway's
// ws.go/sse.go handlers also never select on the request context in their blocking read
// loops, so nothing in this package would notice a Shutdown-driven cancellation even if
// the stdlib did propagate one. Draining already-open connections on Shutdown is not
// implemented; this test documents the real behavior instead of asserting a guarantee
// the code does not provide.
func TestServerListenAndServeTLSShutdown(t *testing.T) {
	system := newTestSystem(t)
	registry := gateway.NewRegistry(system, log.DiscardLogger)

	// crypto/tls' client never sends SNI for an IP-literal ServerName (RFC 6066), so the
	// domain must be a hostname - "localhost" resolves to the loopback address the
	// listener binds to just like "127.0.0.1" would.
	const domain = "localhost"
	certPEM, keyPEM := generateTestCertificate(domain, time.Now().Add(time.Hour))
	issuer := gateway.NewStaticIssuer(&gateway.Certificate{
		CertPEM:  certPEM,
		KeyPEM:   keyPEM,
		NotAfter: time.Now().Add(time.Hour),
	}, domain)
	manager := gateway.NewManager(system, log.DiscardLogger,
		gateway.WithCertIssuer(issuer),
		gateway.WithAllowedDomains(domain),
		gateway.WithRenewInterval(""),
	)

	wsHandler := gateway.NewWSHandler(registry,
		gateway.WithWSIDFunc(func(r *http.Request) string { return r.URL.Query().Get("id") }),
	)

	port := freePort(t)
	bindAddr := fmt.Sprintf("127.0.0.1:%d", port)
	dialAddr := fmt.Sprintf("%s:%d", domain, port)
	srv, err := gateway.NewServer(bindAddr, wsHandler,
		gateway.WithTLSManager(manager),
		gateway.WithDrainOnShutdown(wsHandler),
	)
	require.NoError(t, err)

	serveErrCh := make(chan error, 1)
	go func() { serveErrCh <- srv.ListenAndServe(context.Background()) }()
	time.Sleep(300 * time.Millisecond)

	dial := func() (*websocket.Conn, error) {
		cfg, cfgErr := websocket.NewConfig(fmt.Sprintf("wss://%s/?id=drain-1", dialAddr), fmt.Sprintf("https://%s", dialAddr))
		if cfgErr != nil {
			return nil, cfgErr
		}
		cfg.TlsConfig = &tls.Config{InsecureSkipVerify: true, ServerName: domain} //nolint:gosec // test-only self-signed cert
		return websocket.DialConfig(cfg)
	}

	ws, err := dial()
	require.NoError(t, err)
	defer func() { _ = ws.Close() }()

	time.Sleep(200 * time.Millisecond)
	require.True(t, registry.Has("drain-1"))

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, srv.Shutdown(shutdownCtx))

	select {
	case serveErr := <-serveErrCh:
		require.ErrorIs(t, serveErr, http.ErrServerClosed)
	case <-time.After(5 * time.Second):
		t.Fatal("ListenAndServe did not return after Shutdown")
	}

	// the drained handler must have evicted the established connection: the client's
	// blocked read unblocks with an error instead of hanging until the peer goes away.
	var payload []byte
	require.Error(t, websocket.Message.Receive(ws, &payload))

	// eviction runs the normal disconnect path, so the registry entry disappears.
	require.Eventually(t, func() bool {
		return !registry.Has("drain-1")
	}, 3*time.Second, 50*time.Millisecond)

	// a new connection attempt must be rejected once the listener is down.
	_, err = dial()
	require.Error(t, err)
}
