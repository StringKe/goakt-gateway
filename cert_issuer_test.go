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
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/tochemey/goakt/v4/log"

	gateway "github.com/StringKe/goakt-gateway"
)

// fakeRequestCertificate is a CloudflareOriginCAIssuer.RequestCertificate stand-in that
// returns fixed, recognizable CSR/key bytes so tests can assert on exactly what Issue
// sent in the request body without paying for a real key generation per call.
func fakeRequestCertificate(domain string) (csrPEM, keyPEM []byte, err error) {
	return []byte("-----BEGIN CERTIFICATE REQUEST-----\n" + domain + "\n-----END CERTIFICATE REQUEST-----\n"),
		[]byte("-----BEGIN RSA PRIVATE KEY-----\nfake-key-for-" + domain + "\n-----END RSA PRIVATE KEY-----\n"),
		nil
}

// TestCloudflareOriginCAIssuerRequestShape verifies the HTTP request Issue builds: method,
// path, the service-key auth header, and the JSON body's hostnames/csr fields.
func TestCloudflareOriginCAIssuerRequestShape(t *testing.T) {
	var gotMethod, gotPath, gotAuthHeader, gotContentType string
	var gotBody cloudflareOriginCARequestMirror

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod = r.Method
		gotPath = r.URL.Path
		gotAuthHeader = r.Header.Get("X-Auth-User-Service-Key")
		gotContentType = r.Header.Get("Content-Type")
		require.NoError(t, json.NewDecoder(r.Body).Decode(&gotBody))

		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(cloudflareOriginCAResponseMirror{
			Success: true,
			Result: cloudflareOriginCAResultMirror{
				Certificate: "-----BEGIN CERTIFICATE-----\nissued\n-----END CERTIFICATE-----\n",
				ExpiresOn:   time.Now().Add(90 * 24 * time.Hour),
			},
		})
	}))
	defer server.Close()

	issuer := &gateway.CloudflareOriginCAIssuer{
		APIToken:              "test-service-key",
		BaseURL:               server.URL,
		RequestedValidityDays: 90,
		RequestCertificate:    fakeRequestCertificate,
	}

	cert, err := issuer.Issue(context.Background(), "origin.example.com")
	require.NoError(t, err)
	require.NotNil(t, cert)

	require.Equal(t, http.MethodPost, gotMethod)
	require.Equal(t, "/certificates", gotPath)
	require.Equal(t, "test-service-key", gotAuthHeader)
	require.Equal(t, "application/json", gotContentType)
	require.Equal(t, []string{"origin.example.com"}, gotBody.Hostnames)
	require.Equal(t, "origin-rsa", gotBody.RequestType)
	require.Equal(t, 90, gotBody.RequestValidity)
	require.Contains(t, gotBody.CSR, "origin.example.com")

	require.Equal(t, "origin.example.com", cert.Domain)
	require.Contains(t, string(cert.CertPEM), "issued")
	require.Contains(t, string(cert.KeyPEM), "origin.example.com", "the locally generated key must be returned, not anything from the API response")
}

// TestCloudflareOriginCAIssuerSuccessFalse verifies that a 200 response with
// "success": false surfaces as an error rather than a certificate.
func TestCloudflareOriginCAIssuerSuccessFalse(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(cloudflareOriginCAResponseMirror{
			Success: false,
			Errors: []cloudflareOriginCAErrorMirror{
				{Code: 1000, Message: "invalid request"},
			},
		})
	}))
	defer server.Close()

	issuer := &gateway.CloudflareOriginCAIssuer{
		BaseURL:            server.URL,
		RequestCertificate: fakeRequestCertificate,
	}

	_, err := issuer.Issue(context.Background(), "origin.example.com")
	require.Error(t, err)
	require.Contains(t, err.Error(), "origin.example.com")
}

// TestCloudflareOriginCAIssuerHTTP5xx verifies that a server error response surfaces as
// an error - the body, even if it happens to be well-formed JSON with success:false-like
// shape, must not be silently treated as a successful issuance.
func TestCloudflareOriginCAIssuerHTTP5xx(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(`{"success": false, "errors": [{"code": 500, "message": "internal error"}]}`))
	}))
	defer server.Close()

	issuer := &gateway.CloudflareOriginCAIssuer{
		BaseURL:            server.URL,
		RequestCertificate: fakeRequestCertificate,
	}

	_, err := issuer.Issue(context.Background(), "origin.example.com")
	require.Error(t, err)
}

// TestCloudflareOriginCAIssuerMalformedJSON verifies that a response body that is not
// valid JSON is reported as an error rather than panicking or silently succeeding.
func TestCloudflareOriginCAIssuerMalformedJSON(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("not json at all"))
	}))
	defer server.Close()

	issuer := &gateway.CloudflareOriginCAIssuer{
		BaseURL:            server.URL,
		RequestCertificate: fakeRequestCertificate,
	}

	_, err := issuer.Issue(context.Background(), "origin.example.com")
	require.Error(t, err)
}

// TestCloudflareOriginCAIssuerMissingCertificate verifies that a success:true response
// with no certificate field still yields a Certificate (Cloudflare's contract - Issue does
// not itself validate the certificate is non-empty; that is caught downstream by
// Manager.issue's parseAndVerify) rather than panicking on a nil/missing field.
func TestCloudflareOriginCAIssuerMissingCertificate(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(cloudflareOriginCAResponseMirror{Success: true})
	}))
	defer server.Close()

	issuer := &gateway.CloudflareOriginCAIssuer{
		BaseURL:            server.URL,
		RequestCertificate: fakeRequestCertificate,
	}

	var cert *gateway.Certificate
	var err error
	require.NotPanics(t, func() {
		cert, err = issuer.Issue(context.Background(), "origin.example.com")
	})
	require.NoError(t, err)
	require.Empty(t, cert.CertPEM)
}

// cloudflareOriginCARequestMirror/cloudflareOriginCAResponseMirror mirror the unexported
// wire types CloudflareOriginCAIssuer.Issue encodes/decodes, so this black-box test can
// assert on the request/response JSON shape without reaching into the package.
type cloudflareOriginCARequestMirror struct {
	Hostnames       []string `json:"hostnames"`
	RequestType     string   `json:"request_type"`
	RequestValidity int      `json:"requested_validity"`
	CSR             string   `json:"csr"`
}

type cloudflareOriginCAErrorMirror struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

type cloudflareOriginCAResultMirror struct {
	Certificate string    `json:"certificate"`
	ExpiresOn   time.Time `json:"expires_on"`
}

type cloudflareOriginCAResponseMirror struct {
	Success bool                            `json:"success"`
	Errors  []cloudflareOriginCAErrorMirror `json:"errors"`
	Result  cloudflareOriginCAResultMirror  `json:"result"`
}

// TestStaticIssuer covers StaticIssuer's actual surface: it serves a fixed certificate for
// exactly the domains it was configured with, and performs no validation of its own - a
// mismatched cert/key pair is only caught downstream, by Manager.issue's parseAndVerify.
func TestStaticIssuer(t *testing.T) {
	certPEM, keyPEM := generateTestCertificate("a.example.com", time.Now().Add(time.Hour))
	cert := &gateway.Certificate{CertPEM: certPEM, KeyPEM: keyPEM, NotAfter: time.Now().Add(time.Hour)}
	issuer := gateway.NewStaticIssuer(cert, "a.example.com", "b.example.com")

	t.Run("serves every configured domain with a domain-scoped clone", func(t *testing.T) {
		got, err := issuer.Issue(context.Background(), "a.example.com")
		require.NoError(t, err)
		require.Equal(t, "a.example.com", got.Domain)

		got2, err := issuer.Issue(context.Background(), "b.example.com")
		require.NoError(t, err)
		require.Equal(t, "b.example.com", got2.Domain)
	})

	t.Run("rejects a domain it was not configured for", func(t *testing.T) {
		_, err := issuer.Issue(context.Background(), "unconfigured.example.com")
		require.Error(t, err)
	})

	t.Run("mismatched cert/key material surfaces as an error through Manager", func(t *testing.T) {
		otherCertPEM, _ := generateTestCertificate("mismatch.example.com", time.Now().Add(time.Hour))
		_, unrelatedKeyPEM := generateTestCertificate("other-key.example.com", time.Now().Add(time.Hour))
		mismatched := &gateway.Certificate{CertPEM: otherCertPEM, KeyPEM: unrelatedKeyPEM, NotAfter: time.Now().Add(time.Hour)}
		badIssuer := gateway.NewStaticIssuer(mismatched, "mismatch.example.com")

		system := newTestSystem(t)
		manager := gateway.NewManager(system, log.DiscardLogger,
			gateway.WithCertIssuer(badIssuer),
			gateway.WithAllowedDomains("mismatch.example.com"),
			gateway.WithRenewInterval(""),
		)
		_, err := manager.EnsureCertificate(context.Background(), "mismatch.example.com")
		require.Error(t, err)
	})
}
