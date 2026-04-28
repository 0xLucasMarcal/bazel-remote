package grpcproxy

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/buchgr/bazel-remote/v2/cache"
	"github.com/buchgr/bazel-remote/v2/utils/backendproxy"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

// putWithFullQueue installs a channel-backed uploadQueue with no
// reader, so that exactly `capacity` Puts succeed and any further Put
// hits the "queue full" branch deterministically (no goroutine
// scheduling races).
func putWithFullQueue(t *testing.T, capacity int) *remoteGrpcProxyCache {
	t.Helper()
	queue := make(chan backendproxy.UploadReq, capacity)
	return &remoteGrpcProxyCache{
		uploadQueue:  queue,
		accessLogger: logger,
		errorLogger:  logger,
	}
}

// TestPutIncrementsUploadsDroppedWhenQueueFull verifies that when the
// upload queue is full, Put drops the upload AND increments the
// bazel_remote_grpc_proxy_uploads_dropped_total counter (per kind).
// This is the observability hook operators need for the multi-instance
// fleet case where the shared upstream can't keep up with combined
// local-cache write rate.
func TestPutIncrementsUploadsDroppedWhenQueueFull(t *testing.T) {
	const capacity = 3
	p := putWithFullQueue(t, capacity)

	put := func(kind cache.EntryKind) {
		data := []byte("payload")
		p.Put(context.Background(), kind, "deadbeef", int64(len(data)), int64(len(data)),
			nopReadCloser{Reader: bytes.NewReader(data)})
	}

	casCounter := uploadsDropped.WithLabelValues(cache.CAS.String())
	acCounter := uploadsDropped.WithLabelValues(cache.AC.String())
	beforeCas := testutil.ToFloat64(casCounter)
	beforeAc := testutil.ToFloat64(acCounter)

	// Fill the queue exactly. None of these should be dropped.
	for i := 0; i < capacity; i++ {
		put(cache.CAS)
	}
	if got := testutil.ToFloat64(casCounter) - beforeCas; got != 0 {
		t.Fatalf("expected 0 drops while filling queue, got %v", got)
	}

	// Every subsequent Put must be dropped and counted.
	const overflow = 5
	for i := 0; i < overflow; i++ {
		put(cache.CAS)
	}
	if got := testutil.ToFloat64(casCounter) - beforeCas; got != float64(overflow) {
		t.Fatalf("expected uploadsDropped{kind=cas} delta=%d, got %v", overflow, got)
	}

	// Label hygiene: a CAS overflow must not bleed into the AC counter.
	if got := testutil.ToFloat64(acCounter) - beforeAc; got != 0 {
		t.Fatalf("expected AC counter unchanged, got delta=%v", got)
	}
}

// TestPutDoesNotIncrementWhenQueueHasSpace verifies the counter is NOT
// touched on the happy path (so dashboards built off it stay flat under
// healthy conditions and only spike when the upstream actually lags).
func TestPutDoesNotIncrementWhenQueueHasSpace(t *testing.T) {
	p := putWithFullQueue(t, 8)

	rawCounter := uploadsDropped.WithLabelValues(cache.RAW.String())
	before := testutil.ToFloat64(rawCounter)

	p.Put(context.Background(), cache.RAW, "feedface", 4, 4,
		nopReadCloser{Reader: bytes.NewReader([]byte("data"))})

	after := testutil.ToFloat64(rawCounter)
	if after != before {
		t.Fatalf("expected uploadsDropped{kind=raw} unchanged, got %v -> %v", before, after)
	}
}

// TestUploadsDroppedMetricIsRegistered verifies the counter is exposed
// in the default Prometheus registry under the expected name, so it
// shows up in /metrics scrapes without any additional wiring.
func TestUploadsDroppedMetricIsRegistered(t *testing.T) {
	const want = "bazel_remote_grpc_proxy_uploads_dropped_total"

	mfs, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		t.Fatal(err)
	}
	names := make([]string, 0, len(mfs))
	for _, mf := range mfs {
		if mf.GetName() == want {
			return
		}
		names = append(names, mf.GetName())
	}
	t.Fatalf("metric %q not registered with the default Prometheus gatherer; registered: %s",
		want, strings.Join(names, ", "))
}
