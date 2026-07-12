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
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"testing"

	"github.com/stretchr/testify/require"

	gateway "github.com/StringKe/goakt-gateway"
)

func TestAuthenticatedOriginPullsVerify(t *testing.T) {
	ca := newTestCA()
	trustedCertPEM, _ := ca.issueClientCert("cloudflare-edge")

	other := newTestCA()
	untrustedCertPEM, _ := other.issueClientCert("impersonator")

	pulls := &gateway.AuthenticatedOriginPulls{CAPEM: ca.certPEM}

	trustedBlock := parsePEMCert(t, trustedCertPEM)
	untrustedBlock := parsePEMCert(t, untrustedCertPEM)

	t.Run("accepts a certificate chaining to the configured CA", func(t *testing.T) {
		state := tls.ConnectionState{PeerCertificates: []*x509.Certificate{trustedBlock}}
		require.NoError(t, pulls.Verify(state))
	})

	t.Run("rejects a certificate from another CA", func(t *testing.T) {
		state := tls.ConnectionState{PeerCertificates: []*x509.Certificate{untrustedBlock}}
		err := pulls.Verify(state)
		require.ErrorIs(t, err, gateway.ErrOriginPullVerificationFailed)
	})

	t.Run("rejects a connection with no peer certificate", func(t *testing.T) {
		err := pulls.Verify(tls.ConnectionState{})
		require.ErrorIs(t, err, gateway.ErrOriginPullVerificationFailed)
	})

	t.Run("Apply configures RequireAndVerifyClientCert", func(t *testing.T) {
		cfg := &tls.Config{}
		require.NoError(t, pulls.Apply(cfg))
		require.Equal(t, tls.RequireAndVerifyClientCert, cfg.ClientAuth)
		require.NotNil(t, cfg.ClientCAs)
	})

	t.Run("Apply rejects invalid PEM", func(t *testing.T) {
		bad := &gateway.AuthenticatedOriginPulls{CAPEM: []byte("not a cert")}
		require.Error(t, bad.Apply(&tls.Config{}))
	})

	t.Run("rejects a certificate that chains to the CA but has already expired", func(t *testing.T) {
		expiredCertPEM, _ := ca.issueExpiredClientCert("expired-edge")
		expiredBlock := parsePEMCert(t, expiredCertPEM)
		state := tls.ConnectionState{PeerCertificates: []*x509.Certificate{expiredBlock}}
		err := pulls.Verify(state)
		require.ErrorIs(t, err, gateway.ErrOriginPullVerificationFailed)
	})
}

func parsePEMCert(t *testing.T, certPEM []byte) *x509.Certificate {
	t.Helper()
	block, _ := pem.Decode(certPEM)
	require.NotNil(t, block)
	cert, err := x509.ParseCertificate(block.Bytes)
	require.NoError(t, err)
	return cert
}
