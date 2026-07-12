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
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
)

// NewRSACertificateRequest builds a PEM-encoded PKCS#10 certificate signing request and
// matching RSA private key for domain, suitable for use as
// CloudflareOriginCAIssuer.RequestCertificate. It uses only the standard library, so it
// adds no dependency beyond what go.mod already carries.
func NewRSACertificateRequest(bits int) func(domain string) (csrPEM, keyPEM []byte, err error) {
	if bits <= 0 {
		bits = 2048
	}
	return func(domain string) (csrPEM, keyPEM []byte, err error) {
		key, err := rsa.GenerateKey(rand.Reader, bits)
		if err != nil {
			return nil, nil, fmt.Errorf("gateway: failed to generate RSA key for %q: %w", domain, err)
		}

		template := &x509.CertificateRequest{
			Subject:  pkix.Name{CommonName: domain},
			DNSNames: []string{domain},
		}

		csrDER, err := x509.CreateCertificateRequest(rand.Reader, template, key)
		if err != nil {
			return nil, nil, fmt.Errorf("gateway: failed to create certificate request for %q: %w", domain, err)
		}

		csrPEM = pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE REQUEST", Bytes: csrDER})
		keyPEM = pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)})
		return csrPEM, keyPEM, nil
	}
}
