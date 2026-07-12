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
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

// Certificate is the PEM-encoded issuance result CertIssuer.Issue returns. It is what
// Manager stores in a CertStore and hands to tls.Config.GetCertificate after parsing.
type Certificate struct {
	// Domain is the domain the certificate was issued for.
	Domain string
	// CertPEM is the PEM-encoded leaf certificate (and, where applicable, intermediate
	// chain) as returned by the issuer.
	CertPEM []byte
	// KeyPEM is the PEM-encoded private key matching CertPEM.
	KeyPEM []byte
	// NotAfter is the certificate's expiry, used by Manager to decide when to renew.
	NotAfter time.Time
}

// CertIssuer obtains a certificate for a domain. Manager arbitrates calls to Issue so
// that, when using a cluster-shared Coordinator, at most one node calls it for a given
// domain at a time (see Manager.EnsureCertificate).
//
// Implementations must be safe for concurrent use.
type CertIssuer interface {
	// Issue returns a new Certificate for domain. It is called by Manager both for the
	// first issuance of a domain and for renewal ahead of expiry.
	Issue(ctx context.Context, domain string) (*Certificate, error)
}

// StaticIssuer is a CertIssuer that serves a fixed, pre-provisioned certificate for one
// or more domains, e.g. loaded from files at startup. It never renews: Certificate.NotAfter
// reflects whatever was supplied, and Manager's renewal schedule will simply keep
// re-"issuing" (returning) the same static material.
type StaticIssuer struct {
	certs map[string]*Certificate
}

// enforce compilation error
var _ CertIssuer = (*StaticIssuer)(nil)

// NewStaticIssuer creates a StaticIssuer that serves cert for every domain in domains.
func NewStaticIssuer(cert *Certificate, domains ...string) *StaticIssuer {
	issuer := &StaticIssuer{certs: make(map[string]*Certificate, len(domains))}
	for _, domain := range domains {
		clone := *cert
		clone.Domain = domain
		issuer.certs[domain] = &clone
	}
	return issuer
}

// Issue implements CertIssuer.
func (s *StaticIssuer) Issue(_ context.Context, domain string) (*Certificate, error) {
	cert, ok := s.certs[domain]
	if !ok {
		return nil, fmt.Errorf("gateway: no static certificate configured for domain %q", domain)
	}
	return cert, nil
}

// CloudflareOriginCAIssuer issues certificates through the Cloudflare Origin CA REST
// API (https://developers.cloudflare.com/ssl/origin-configuration/origin-ca/), using
// only net/http - no Cloudflare SDK dependency. It signs a certificate request created
// locally (CertRequestPEM) and returns whatever Cloudflare hands back.
//
// Cloudflare-issued origin certificates are trusted by Cloudflare's edge but not by
// public clients, which is the intended shape for a service that only ever receives
// traffic proxied through Cloudflare.
type CloudflareOriginCAIssuer struct {
	// APIToken authenticates against the Origin CA API. Origin CA calls use an API
	// Token/Key dedicated to Origin CA rather than a general Cloudflare API token; see
	// the Cloudflare documentation for how to obtain one.
	APIToken string
	// BaseURL overrides the Cloudflare API base URL. Defaults to
	// "https://api.cloudflare.com/client/v4" when empty; tests point this at a local
	// httptest.Server.
	BaseURL string
	// RequestedValidityDays is the requested certificate lifetime in days, one of the
	// values Cloudflare's API accepts (7, 30, 90, 365, 730, 1095, or 5475). Defaults to
	// 365 when zero.
	RequestedValidityDays int
	// RequestCertificate builds the PEM-encoded PKCS#10 certificate signing request and
	// matching private key for domain. It is a function rather than a fixed
	// implementation so callers can plug in their preferred key type/size; see
	// NewRSACertificateRequest for a ready-made RSA implementation.
	RequestCertificate func(domain string) (csrPEM, keyPEM []byte, err error)
	// HTTPClient performs the API request. Defaults to http.DefaultClient when nil.
	HTTPClient *http.Client
}

// enforce compilation error
var _ CertIssuer = (*CloudflareOriginCAIssuer)(nil)

const cloudflareOriginCADefaultBaseURL = "https://api.cloudflare.com/client/v4"

type cloudflareOriginCARequest struct {
	Hostnames       []string `json:"hostnames"`
	RequestType     string   `json:"request_type"`
	RequestValidity int      `json:"requested_validity"`
	CSR             string   `json:"csr"`
}

type cloudflareOriginCAResponse struct {
	Success bool `json:"success"`
	Errors  []struct {
		Code    int    `json:"code"`
		Message string `json:"message"`
	} `json:"errors"`
	Result struct {
		Certificate string    `json:"certificate"`
		ExpiresOn   time.Time `json:"expires_on"`
	} `json:"result"`
}

// Issue implements CertIssuer by calling the Cloudflare Origin CA "create certificate"
// endpoint (POST /certificates).
func (c *CloudflareOriginCAIssuer) Issue(ctx context.Context, domain string) (*Certificate, error) {
	if c.RequestCertificate == nil {
		return nil, fmt.Errorf("gateway: CloudflareOriginCAIssuer.RequestCertificate is required")
	}

	csrPEM, keyPEM, err := c.RequestCertificate(domain)
	if err != nil {
		return nil, fmt.Errorf("gateway: failed to build certificate request for %q: %w", domain, err)
	}

	validity := c.RequestedValidityDays
	if validity == 0 {
		validity = 365
	}

	reqBody, err := json.Marshal(cloudflareOriginCARequest{
		Hostnames:       []string{domain},
		RequestType:     "origin-rsa",
		RequestValidity: validity,
		CSR:             string(csrPEM),
	})
	if err != nil {
		return nil, fmt.Errorf("gateway: failed to encode Cloudflare Origin CA request: %w", err)
	}

	baseURL := c.BaseURL
	if baseURL == "" {
		baseURL = cloudflareOriginCADefaultBaseURL
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, baseURL+"/certificates", bytes.NewReader(reqBody))
	if err != nil {
		return nil, err
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("X-Auth-User-Service-Key", c.APIToken)

	client := c.HTTPClient
	if client == nil {
		client = http.DefaultClient
	}

	httpResp, err := client.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("gateway: Cloudflare Origin CA request failed: %w", err)
	}
	defer func() { _ = httpResp.Body.Close() }()

	body, err := io.ReadAll(httpResp.Body)
	if err != nil {
		return nil, fmt.Errorf("gateway: failed to read Cloudflare Origin CA response: %w", err)
	}

	var apiResp cloudflareOriginCAResponse
	if err := json.Unmarshal(body, &apiResp); err != nil {
		return nil, fmt.Errorf("gateway: failed to decode Cloudflare Origin CA response: %w", err)
	}

	if !apiResp.Success {
		return nil, fmt.Errorf("gateway: Cloudflare Origin CA issuance for %q failed: %+v", domain, apiResp.Errors)
	}

	return &Certificate{
		Domain:   domain,
		CertPEM:  []byte(apiResp.Result.Certificate),
		KeyPEM:   keyPEM,
		NotAfter: apiResp.Result.ExpiresOn,
	}, nil
}
