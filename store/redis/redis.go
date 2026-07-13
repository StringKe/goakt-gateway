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

// Package redis provides a Redis- or Valkey-backed gateway.CertStore, so that every process
// in a deployment shares one persistent certificate store: a node that cold-starts fetches
// already-issued certificates back from the server instead of depending on the issuer being
// reachable/willing to re-issue. It is a separate package specifically so that importing the
// root gateway package never pulls in github.com/redis/go-redis/v9 for applications that use
// the default gateway.MemoryCertStore or gateway.FileCertStore.
//
// go-redis speaks the same RESP protocol to Redis and to Valkey (a BSD-licensed fork of
// Redis 7.2.4), so the constructor takes a goredis.UniversalClient pointed at either one and
// the code below carries no Redis-versus-Valkey branch. Only core string commands are used,
// which both servers implement identically.
package redis

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	goredis "github.com/redis/go-redis/v9"

	gateway "github.com/StringKe/goakt-gateway"
)

// certKeyInfix separates the store's own certificate keys from anything else sharing the
// same prefix, so a single WithKeyPrefix namespace can hold certificate keys alongside
// other gateway state without collisions.
const certKeyInfix = "cert:"

// certRecord is the JSON wire format used to store a gateway.Certificate on the server. It is a
// self-contained copy of the root package's unexported encoding rather than a reference to
// it, so this package depends only on the public gateway.Certificate type. CertPEM/KeyPEM
// are []byte, which encoding/json base64-encodes, keeping arbitrary bytes safe on the wire.
type certRecord struct {
	Domain   string `json:"domain"`
	CertPEM  []byte `json:"cert_pem"`
	KeyPEM   []byte `json:"key_pem"`
	NotAfter int64  `json:"not_after_unix"`
}

// Store is a gateway.CertStore backed by a Redis or Valkey client: each certificate is
// stored under its own key with no TTL, so a certificate survives until it is overwritten
// by Put or removed by Delete. Server-side persistence (Redis RDB/AOF, or the Valkey
// equivalent) is what makes the store survive a full deployment restart, which is the
// reason to plug it in over MemoryCertStore. Only core string commands (GET/SET/DEL) are
// used, all present and identical on Redis 7.2 and Valkey 8, so one client pointed at
// either server works with no code change.
type Store struct {
	client goredis.UniversalClient
	prefix string
}

// Option configures a Store created with New.
type Option func(*Store)

// WithKeyPrefix namespaces every key this Store reads or writes, so multiple gateway
// deployments (or unrelated applications) can share one Redis instance/database without
// colliding. Defaults to no prefix.
func WithKeyPrefix(prefix string) Option {
	return func(s *Store) { s.prefix = prefix }
}

// New creates a Store backed by client. client may be a *redis.Client,
// *redis.ClusterClient, *redis.Ring, or any other goredis.UniversalClient implementation,
// pointed at either a Redis or a Valkey server.
func New(client goredis.UniversalClient, opts ...Option) *Store {
	s := &Store{client: client}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// enforce compilation error
var _ gateway.CertStore = (*Store)(nil)

// key maps a domain to its Redis key.
func (s *Store) key(domain string) string {
	return s.prefix + certKeyInfix + domain
}

// Get implements gateway.CertStore. It returns gateway.ErrCertificateNotFound when no
// certificate is stored for domain.
func (s *Store) Get(ctx context.Context, domain string) (*gateway.Certificate, error) {
	data, err := s.client.Get(ctx, s.key(domain)).Bytes()
	if errors.Is(err, goredis.Nil) {
		return nil, gateway.ErrCertificateNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("gateway: failed to read certificate for %q: %w", domain, err)
	}

	var record certRecord
	if err := json.Unmarshal(data, &record); err != nil {
		return nil, fmt.Errorf("gateway: stored certificate for %q is invalid: %w", domain, err)
	}
	return &gateway.Certificate{
		Domain:   record.Domain,
		CertPEM:  record.CertPEM,
		KeyPEM:   record.KeyPEM,
		NotAfter: time.Unix(record.NotAfter, 0).UTC(),
	}, nil
}

// Put implements gateway.CertStore, overwriting any previous certificate for the same
// domain. The key is written without a TTL: a certificate lives until Put replaces it or
// Delete removes it, since expiry is governed by the certificate's own NotAfter rather
// than by Redis eviction.
func (s *Store) Put(ctx context.Context, cert *gateway.Certificate) error {
	data, err := json.Marshal(certRecord{
		Domain:   cert.Domain,
		CertPEM:  cert.CertPEM,
		KeyPEM:   cert.KeyPEM,
		NotAfter: cert.NotAfter.Unix(),
	})
	if err != nil {
		return fmt.Errorf("gateway: failed to encode certificate for %q: %w", cert.Domain, err)
	}
	if err := s.client.Set(ctx, s.key(cert.Domain), data, 0).Err(); err != nil {
		return fmt.Errorf("gateway: failed to write certificate for %q: %w", cert.Domain, err)
	}
	return nil
}

// Delete implements gateway.CertStore. It is idempotent: deleting a domain with no stored
// certificate is not an error.
func (s *Store) Delete(ctx context.Context, domain string) error {
	if err := s.client.Del(ctx, s.key(domain)).Err(); err != nil {
		return fmt.Errorf("gateway: failed to delete certificate for %q: %w", domain, err)
	}
	return nil
}
