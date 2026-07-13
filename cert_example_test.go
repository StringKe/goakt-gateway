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
	"errors"
	"fmt"
	"time"

	gateway "github.com/StringKe/goakt-gateway"
)

// ExampleManager shows a Manager that admits domains dynamically with WithDomainPolicy
// and serves a WithFallbackCertificate for connections it has no per-domain certificate
// for. A multi-tenant gateway uses this shape when the set of servable hostnames lives in
// a database rather than in a static allow list.
//
// GetCertificate is driven directly here; in production it is assigned to
// tls.Config.GetCertificate (see Manager.TLSConfig). Neither GetCertificate nor the
// renewal schedule is exercised, so no ActorSystem is needed for the example to run.
func ExampleManager() {
	// The fallback is served both when the ClientHello carries no SNI and when an admitted
	// domain has no CertIssuer configured to obtain a per-domain certificate.
	certPEM, keyPEM := generateTestCertificate("fallback.example.com", time.Now().Add(24*time.Hour))
	fallback, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		panic(err)
	}

	// The policy is the dynamic admission check: only "tenant.example.com" is servable.
	// A real deployment would consult its own source of truth (e.g. a tenant lookup).
	policy := func(_ context.Context, domain string) (bool, error) {
		return domain == "tenant.example.com", nil
	}

	manager := gateway.NewManager(nil, nil,
		gateway.WithDomainPolicy(policy),
		gateway.WithFallbackCertificate(func() (*tls.Certificate, error) { return &fallback, nil }),
	)

	// No SNI: nothing to look a certificate up by, so the fallback is served.
	noSNI, err := manager.GetCertificate(&tls.ClientHelloInfo{})
	fmt.Println("no SNI served:", err == nil && noSNI != nil)

	// An admitted domain with no CertIssuer also falls back to the fallback certificate.
	admitted, err := manager.GetCertificate(&tls.ClientHelloInfo{ServerName: "tenant.example.com"})
	fmt.Println("admitted domain served:", err == nil && admitted != nil)

	// A refused domain is never handed the fallback: admission wins, so the handshake
	// fails with ErrDomainNotAllowed instead of leaking a valid certificate.
	_, err = manager.GetCertificate(&tls.ClientHelloInfo{ServerName: "blocked.example.com"})
	fmt.Println("refused domain blocked:", errors.Is(err, gateway.ErrDomainNotAllowed))

	// Output:
	// no SNI served: true
	// admitted domain served: true
	// refused domain blocked: true
}
