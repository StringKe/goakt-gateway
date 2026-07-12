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
	"crypto/tls"
	"crypto/x509"
	"fmt"
)

// AuthenticatedOriginPulls configures mutual TLS verification of inbound connections
// against a trusted CA - the shape Cloudflare's "Authenticated Origin Pulls" feature
// requires of an origin server: only connections presenting a client certificate that
// chains to Cloudflare's published origin-pull CA are accepted.
//
// The CA is supplied by the caller (CAPEM) rather than embedded, since a real deployment
// should track Cloudflare's currently published origin-pull CA bundle rather than trust
// a copy vendored into this library.
type AuthenticatedOriginPulls struct {
	// CAPEM is the PEM-encoded CA certificate (or bundle) that inbound client
	// certificates must chain to.
	CAPEM []byte
}

// Apply overlays mTLS verification onto cfg: it sets ClientAuth to
// tls.RequireAndVerifyClientCert and ClientCAs to a pool built from CAPEM. It returns an
// error if CAPEM does not contain a valid PEM-encoded certificate.
func (a *AuthenticatedOriginPulls) Apply(cfg *tls.Config) error {
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(a.CAPEM) {
		return fmt.Errorf("gateway: invalid Authenticated Origin Pulls CA certificate")
	}
	cfg.ClientCAs = pool
	cfg.ClientAuth = tls.RequireAndVerifyClientCert
	return nil
}

// Verify checks that the peer certificates presented in state chain to the configured
// CA. tls.Config.ClientAuth = tls.RequireAndVerifyClientCert already performs this
// verification during the handshake itself; Verify exists for callers that want to
// re-check an already-established connection's state explicitly (e.g. in an HTTP
// handler, via *http.Request.TLS) rather than solely trust the handshake outcome.
func (a *AuthenticatedOriginPulls) Verify(state tls.ConnectionState) error {
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(a.CAPEM) {
		return fmt.Errorf("gateway: invalid Authenticated Origin Pulls CA certificate")
	}

	if len(state.PeerCertificates) == 0 {
		return ErrOriginPullVerificationFailed
	}

	opts := x509.VerifyOptions{
		Roots:         pool,
		Intermediates: x509.NewCertPool(),
		KeyUsages:     []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	}
	for _, cert := range state.PeerCertificates[1:] {
		opts.Intermediates.AddCert(cert)
	}

	if _, err := state.PeerCertificates[0].Verify(opts); err != nil {
		return fmt.Errorf("%w: %v", ErrOriginPullVerificationFailed, err)
	}
	return nil
}
