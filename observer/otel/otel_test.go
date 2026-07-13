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

package otel_test

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	gateway "github.com/StringKe/goakt-gateway"
	otelobs "github.com/StringKe/goakt-gateway/observer/otel"
)

// collect drains the manual reader and returns a name->sum map over the int64 data
// points, summing every attribute set so tests can assert either aggregate totals or,
// via point, a specific labelled series.
func collect(t *testing.T, reader *sdkmetric.ManualReader) map[string]int64 {
	t.Helper()
	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(context.Background(), &rm))

	totals := make(map[string]int64)
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if sum, ok := m.Data.(metricdata.Sum[int64]); ok {
				for _, dp := range sum.DataPoints {
					totals[m.Name] += dp.Value
				}
			}
		}
	}
	return totals
}

// point returns the int64 value of the data point on metric name carrying attribute
// key=val, or fails if no such point exists.
func point(t *testing.T, reader *sdkmetric.ManualReader, name, key, val string) int64 {
	t.Helper()
	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(context.Background(), &rm))
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != name {
				continue
			}
			sum, ok := m.Data.(metricdata.Sum[int64])
			if !ok {
				continue
			}
			for _, dp := range sum.DataPoints {
				if v, ok := dp.Attributes.Value(attribute.Key(key)); ok && v.AsString() == val {
					return dp.Value
				}
			}
		}
	}
	t.Fatalf("no data point for metric %q with %s=%q", name, key, val)
	return 0
}

func newTestObserver(t *testing.T, opts ...otelobs.Option) (gateway.Observer, *sdkmetric.ManualReader) {
	t.Helper()
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	return otelobs.NewObserver(provider.Meter("test"), opts...), reader
}

func TestObserverMapsHooksToInstruments(t *testing.T) {
	obs, reader := newTestObserver(t)

	obs.ConnectionRegistered("c1", "g1")
	obs.ConnectionRegistered("c2", "g1")
	obs.ConnectionRegistered("c3", "g2")
	obs.ConnectionUnregistered("c3", "g2")
	obs.ConnectionReplaced("c1", "g1")
	obs.DeliveryDropped("c2", "g1")
	obs.DeliveryDropped("c2", "g1")
	obs.DeliveryFailed("c2", errors.New("socket gone"))
	obs.BroadcastFanout("topic", 5)

	totals := collect(t, reader)
	require.Equal(t, int64(2), totals["gateway.connections.active"], "3 registered minus 1 unregistered")
	require.Equal(t, int64(1), totals["gateway.connections.replaced"])
	require.Equal(t, int64(2), totals["gateway.delivery.dropped"])
	require.Equal(t, int64(1), totals["gateway.delivery.failed"])
	require.Equal(t, int64(5), totals["gateway.broadcast.fanout"], "fanout adds local member count")
}

func TestBroadcastFanoutIgnoresNonPositive(t *testing.T) {
	obs, reader := newTestObserver(t)

	obs.BroadcastFanout("topic", 0)
	obs.BroadcastFanout("topic", -3)

	totals := collect(t, reader)
	require.Zero(t, totals["gateway.broadcast.fanout"])
}

func TestOfflineFallbackLabelsResult(t *testing.T) {
	obs, reader := newTestObserver(t)
	offline, ok := obs.(gateway.OfflineObserver)
	require.True(t, ok, "otel observer must implement OfflineObserver")

	offline.OfflineFallback("g1", nil)
	offline.OfflineFallback("g1", nil)
	offline.OfflineFallback("g2", errors.New("push failed"))

	require.Equal(t, int64(2), point(t, reader, "gateway.offline.fallback", "result", "success"))
	require.Equal(t, int64(1), point(t, reader, "gateway.offline.fallback", "result", "failure"))
}

func TestGroupAttributeOptIn(t *testing.T) {
	obs, reader := newTestObserver(t, otelobs.WithGroupAttribute())

	obs.ConnectionRegistered("c1", "alpha")
	obs.ConnectionRegistered("c2", "beta")
	obs.BroadcastFanout("alpha", 4)

	require.Equal(t, int64(1), point(t, reader, "gateway.connections.active", "group", "alpha"))
	require.Equal(t, int64(1), point(t, reader, "gateway.connections.active", "group", "beta"))
	require.Equal(t, int64(4), point(t, reader, "gateway.broadcast.fanout", "group", "alpha"))
}

func TestInstrumentPrefixOverride(t *testing.T) {
	obs, reader := newTestObserver(t, otelobs.WithInstrumentPrefix("app."))

	obs.ConnectionRegistered("c1", "g1")

	totals := collect(t, reader)
	require.Equal(t, int64(1), totals["app.connections.active"])
	require.Zero(t, totals["gateway.connections.active"])
}

func TestNilMeterIsNoOp(t *testing.T) {
	obs := otelobs.NewObserver(nil)
	require.NotPanics(t, func() {
		obs.ConnectionRegistered("c1", "g1")
		obs.DeliveryFailed("c1", errors.New("x"))
		obs.BroadcastFanout("t", 3)
		obs.(gateway.OfflineObserver).OfflineFallback("g1", nil)
	})
}
