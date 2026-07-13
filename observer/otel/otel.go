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

// Package otel provides an OpenTelemetry-backed implementation of gateway.Observer.
//
// It maps the six Observer hooks onto OpenTelemetry metric instruments: the live
// connection count is an int64 up/down counter, while replacements, dropped and failed
// deliveries and broadcast fan-out are monotonic int64 counters. When the configured
// Observer also needs to report offline fallbacks it additionally satisfies
// gateway.OfflineObserver, so the same instance can be passed to WithObserver and cover
// every event a Registry emits.
package otel

import (
	"context"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	metricnoop "go.opentelemetry.io/otel/metric/noop"

	gateway "github.com/StringKe/goakt-gateway"
)

// defaultPrefix is prepended to every instrument name so a caller can attribute the
// metrics to this library without wiring a prefix themselves.
const defaultPrefix = "gateway."

// attrKeyGroup is the attribute key used for the connection group when group attributes
// are enabled. It doubles as the broadcast topic key because a topic is the fan-out
// analogue of a group and keeping one key keeps dashboards simple.
const attrKeyGroup = "group"

// Compile-time proof the concrete type satisfies both the core Observer contract and the
// optional offline extension, so a single value covers every Registry event.
var (
	_ gateway.Observer        = (*observer)(nil)
	_ gateway.OfflineObserver = (*observer)(nil)
)

// Option customises an Observer built by NewObserver.
type Option func(*config)

// config holds the resolved Observer settings.
type config struct {
	prefix    string
	groupAttr bool
}

// WithInstrumentPrefix overrides the "gateway." prefix prepended to every instrument
// name. An empty prefix registers the instruments under their bare names.
func WithInstrumentPrefix(prefix string) Option {
	return func(c *config) { c.prefix = prefix }
}

// WithGroupAttribute records the connection group (and the broadcast topic) as a metric
// attribute. It is off by default because an unbounded set of groups produces an
// unbounded number of time series; enable it only when the group space is bounded.
func WithGroupAttribute() Option {
	return func(c *config) { c.groupAttr = true }
}

// observer is the OpenTelemetry-backed gateway.Observer. Every hook runs on a delivery
// hot path, so it only performs a single non-blocking counter Add and never allocates
// attributes unless group reporting was explicitly enabled.
type observer struct {
	groupAttr bool

	active    metric.Int64UpDownCounter
	replaced  metric.Int64Counter
	dropped   metric.Int64Counter
	failed    metric.Int64Counter
	fanout    metric.Int64Counter
	fallbacks metric.Int64Counter
}

// NewObserver builds a gateway.Observer that records connection and delivery events on
// meter. Instrument creation errors are not fatal: the affected instrument falls back to
// a no-op so a partially misconfigured meter never breaks delivery. Passing a nil meter
// yields an Observer whose hooks are all no-ops.
func NewObserver(meter metric.Meter, opts ...Option) gateway.Observer {
	cfg := config{prefix: defaultPrefix}
	for _, opt := range opts {
		opt(&cfg)
	}
	if meter == nil {
		meter = metricnoop.NewMeterProvider().Meter("")
	}

	o := &observer{
		groupAttr: cfg.groupAttr,
		active: upDownCounter(meter, cfg.prefix+"connections.active",
			"Number of connections currently registered on this node.", "{connection}"),
		replaced: counter(meter, cfg.prefix+"connections.replaced",
			"Connections evicted by a same-id takeover.", "{connection}"),
		dropped: counter(meter, cfg.prefix+"delivery.dropped",
			"Payloads dropped because a connection's outbound buffer was full.", "{message}"),
		failed: counter(meter, cfg.prefix+"delivery.failed",
			"Local deliveries that failed with a non-backpressure error.", "{message}"),
		fanout: counter(meter, cfg.prefix+"broadcast.fanout",
			"Local members a broadcast payload was written to.", "{message}"),
		fallbacks: counter(meter, cfg.prefix+"offline.fallback",
			"Offline channel fallback attempts, labelled by result.", "{attempt}"),
	}
	return o
}

// groupOption returns the measurement attributes for a group-scoped event, allocating a
// single-key set only when group reporting is enabled to keep the default path free of
// per-call allocation.
func (o *observer) groupOption(group string) []metric.AddOption {
	if !o.groupAttr {
		return nil
	}
	return []metric.AddOption{metric.WithAttributes(attribute.String(attrKeyGroup, group))}
}

// ConnectionRegistered increments the live connection gauge.
func (o *observer) ConnectionRegistered(id, group string) {
	o.active.Add(context.Background(), 1, o.groupOption(group)...)
}

// ConnectionUnregistered decrements the live connection gauge.
func (o *observer) ConnectionUnregistered(id, group string) {
	o.active.Add(context.Background(), -1, o.groupOption(group)...)
}

// ConnectionReplaced counts a takeover eviction.
func (o *observer) ConnectionReplaced(id, group string) {
	o.replaced.Add(context.Background(), 1, o.groupOption(group)...)
}

// DeliveryDropped counts a backpressure drop.
func (o *observer) DeliveryDropped(id, group string) {
	o.dropped.Add(context.Background(), 1, o.groupOption(group)...)
}

// DeliveryFailed counts a non-backpressure delivery failure. The underlying error is not
// recorded as an attribute because its cardinality is unbounded.
func (o *observer) DeliveryFailed(id string, err error) {
	o.failed.Add(context.Background(), 1)
}

// BroadcastFanout adds the number of local members a broadcast reached, so the counter
// tracks total payloads written rather than broadcast invocations.
func (o *observer) BroadcastFanout(topic string, localMembers int) {
	if localMembers <= 0 {
		return
	}
	var opts []metric.AddOption
	if o.groupAttr {
		opts = []metric.AddOption{metric.WithAttributes(attribute.String(attrKeyGroup, topic))}
	}
	o.fanout.Add(context.Background(), int64(localMembers), opts...)
}

// OfflineFallback counts an offline channel fallback, labelled result=success or
// result=failure so the success ratio is queryable without a separate instrument.
func (o *observer) OfflineFallback(group string, err error) {
	result := "success"
	if err != nil {
		result = "failure"
	}
	attrs := []attribute.KeyValue{attribute.String("result", result)}
	if o.groupAttr {
		attrs = append(attrs, attribute.String(attrKeyGroup, group))
	}
	o.fallbacks.Add(context.Background(), 1, metric.WithAttributes(attrs...))
}

// counter creates a monotonic int64 counter, substituting a no-op instrument on error so
// a single failed registration cannot panic a delivery hot path.
func counter(meter metric.Meter, name, desc, unit string) metric.Int64Counter {
	c, err := meter.Int64Counter(name, metric.WithDescription(desc), metric.WithUnit(unit))
	if err != nil {
		return metricnoop.Int64Counter{}
	}
	return c
}

// upDownCounter creates an int64 up/down counter, substituting a no-op instrument on
// error for the same reason as counter.
func upDownCounter(meter metric.Meter, name, desc, unit string) metric.Int64UpDownCounter {
	c, err := meter.Int64UpDownCounter(name, metric.WithDescription(desc), metric.WithUnit(unit))
	if err != nil {
		return metricnoop.Int64UpDownCounter{}
	}
	return c
}
