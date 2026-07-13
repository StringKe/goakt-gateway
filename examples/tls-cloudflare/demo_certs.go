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

package main

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"time"
)

// demoCert bundles a generated RSA key pair with both its PEM and parsed forms. The
// parsed *x509.Certificate/*rsa.PrivateKey are kept around so a demoCert can act as the
// issuer for a child certificate (see newDemoLeaf), and the PEM forms are what gets
// handed to gateway APIs (gateway.Certificate, AuthenticatedOriginPulls.CAPEM).
type demoCert struct {
	certPEM []byte
	keyPEM  []byte
	cert    *x509.Certificate
	key     *rsa.PrivateKey
}

// newDemoCA creates a self-signed CA. This example has no access to the real trust
// roots it stands in for (Cloudflare's Origin CA root, Cloudflare's Authenticated Origin
// Pulls CA bundle) - only Cloudflare holds those private keys - so a locally generated CA
// is what lets the mTLS accept/reject paths run end to end without a network call.
func newDemoCA(commonName string) (*demoCert, error) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return nil, fmt.Errorf("generate CA key for %q: %w", commonName, err)
	}
	serial, err := randomSerial()
	if err != nil {
		return nil, err
	}

	template := &x509.Certificate{
		SerialNumber:          serial,
		Subject:               pkix.Name{CommonName: commonName},
		NotBefore:             time.Now().Add(-time.Minute),
		NotAfter:              time.Now().Add(24 * time.Hour),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}

	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		return nil, fmt.Errorf("create CA certificate for %q: %w", commonName, err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		return nil, err
	}

	return &demoCert{
		certPEM: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
		keyPEM:  encodeRSAKey(key),
		cert:    cert,
		key:     key,
	}, nil
}

// newDemoLeaf creates a certificate valid for validFor, signed by parent (or
// self-signed when parent is nil). serverAuth selects a TLS server certificate
// (DNSNames covering commonName and localhost, plus the 127.0.0.1 SAN a Go TLS client
// checks when dialing "127.0.0.1:port"); a false value produces a TLS client
// certificate instead, which is all the Authenticated Origin Pulls demo needs.
func newDemoLeaf(commonName string, parent *demoCert, serverAuth bool, validFor time.Duration) (*demoCert, error) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return nil, fmt.Errorf("generate leaf key for %q: %w", commonName, err)
	}
	serial, err := randomSerial()
	if err != nil {
		return nil, err
	}

	template := &x509.Certificate{
		SerialNumber: serial,
		Subject:      pkix.Name{CommonName: commonName},
		NotBefore:    time.Now().Add(-time.Minute),
		NotAfter:     time.Now().Add(validFor),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
	}
	if serverAuth {
		template.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}
		template.DNSNames = []string{commonName, "localhost"}
		template.IPAddresses = []net.IP{net.ParseIP("127.0.0.1")}
	} else {
		template.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}
	}

	parentCert, parentKey := template, key // self-signed default: sign with its own key
	if parent != nil {
		parentCert, parentKey = parent.cert, parent.key
	}

	der, err := x509.CreateCertificate(rand.Reader, template, parentCert, &key.PublicKey, parentKey)
	if err != nil {
		return nil, fmt.Errorf("create leaf certificate for %q: %w", commonName, err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		return nil, err
	}

	return &demoCert{
		certPEM: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
		keyPEM:  encodeRSAKey(key),
		cert:    cert,
		key:     key,
	}, nil
}

func encodeRSAKey(key *rsa.PrivateKey) []byte {
	return pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)})
}

// randomSerial returns a certificate serial number in the range x509.CreateCertificate
// requires (positive, non-zero).
func randomSerial() (*big.Int, error) {
	limit := new(big.Int).Lsh(big.NewInt(1), 128)
	serial, err := rand.Int(rand.Reader, limit)
	if err != nil {
		return nil, fmt.Errorf("generate certificate serial: %w", err)
	}
	return serial, nil
}
