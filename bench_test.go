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
	"net"
	"testing"
	"time"

	"github.com/tochemey/goakt/v4/actor"
	"github.com/tochemey/goakt/v4/log"

	gateway "github.com/StringKe/goakt-gateway"
)

// BenchmarkManagerGetCertificateHot measures the SNI hot-path lookup cost once a
// certificate has already been issued and cached - the steady-state cost paid on every
// TLS handshake.
func BenchmarkManagerGetCertificateHot(b *testing.B) {
	system := newBenchSystem(b)
	issuer := &fakeIssuer{ttl: time.Hour}
	manager := gateway.NewManager(system, log.DiscardLogger,
		gateway.WithCertIssuer(issuer),
		gateway.WithAllowedDomains("bench.example.com"),
		gateway.WithRenewInterval(""),
		gateway.WithRenewBefore(time.Minute),
	)

	hello := &tls.ClientHelloInfo{ServerName: "bench.example.com"}
	if _, err := manager.GetCertificate(hello); err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if _, err := manager.GetCertificate(hello); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkTLSHandshake measures a full TLS 1.3 handshake between an in-process client
// and server, with the server side terminating TLS through Manager.GetCertificate - the
// same code path a real gateway.Server uses.
func BenchmarkTLSHandshake(b *testing.B) {
	system := newBenchSystem(b)
	issuer := &fakeIssuer{ttl: time.Hour}
	manager := gateway.NewManager(system, log.DiscardLogger,
		gateway.WithCertIssuer(issuer),
		gateway.WithAllowedDomains("bench.example.com"),
		gateway.WithRenewInterval(""),
		gateway.WithRenewBefore(time.Minute),
	)

	serverCfg := manager.TLSConfig()
	clientCfg := &tls.Config{
		InsecureSkipVerify: true, //nolint:gosec // benchmark-only, no real network exposure
		ServerName:         "bench.example.com",
	}

	ln, err := tls.Listen("tcp", "127.0.0.1:0", serverCfg)
	if err != nil {
		b.Fatal(err)
	}
	defer func() { _ = ln.Close() }()

	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go func() {
				defer func() { _ = conn.Close() }()
				buf := make([]byte, 1)
				_, _ = conn.Read(buf)
			}()
		}
	}()

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		conn, err := net.Dial("tcp", ln.Addr().String())
		if err != nil {
			b.Fatal(err)
		}
		client := tls.Client(conn, clientCfg)
		if err := client.Handshake(); err != nil {
			b.Fatal(err)
		}
		_ = client.Close()
	}
}

// BenchmarkRegistrySendToConnectionLocal measures the local, actor-free hot path of
// Registry.SendToConnection - the delivery shape used whenever the target connection is
// held by the very node handling the request.
func BenchmarkRegistrySendToConnectionLocal(b *testing.B) {
	system := newBenchSystem(b)
	registry := gateway.NewRegistry(system, log.DiscardLogger)

	send := func([]byte) error { return nil }
	if err := registry.Register(context.Background(), "bench-conn", send); err != nil {
		b.Fatal(err)
	}

	payload := []byte("benchmark-payload")

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if err := registry.SendToConnection(context.Background(), "bench-conn", payload); err != nil {
			b.Fatal(err)
		}
	}
}

// newBenchSystem starts a non-clustered actor system for benchmark use.
func newBenchSystem(b *testing.B) actor.ActorSystem {
	b.Helper()
	system, err := actor.NewActorSystem(b.Name(), actor.WithLogger(log.DiscardLogger))
	if err != nil {
		b.Fatal(err)
	}
	if err := system.Start(context.Background()); err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = system.Stop(context.Background()) })
	time.Sleep(100 * time.Millisecond)
	return system
}
