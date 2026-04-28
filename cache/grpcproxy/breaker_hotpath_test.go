package grpcproxy

import (
	"context"
	"errors"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/buchgr/bazel-remote/v2/cache"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"

	pb "github.com/buchgr/bazel-remote/v2/genproto/build/bazel/remote/execution/v2"
	bs "google.golang.org/genproto/googleapis/bytestream"
)

// --- Pure breaker unit tests (no networking) -------------------------------

func TestCircuitBreakerOpensAfterThreshold(t *testing.T) {
	cb := newCircuitBreaker(3, 100*time.Millisecond)
	if !cb.Allow() {
		t.Fatal("breaker should start closed and allow")
	}
	for i := 0; i < 2; i++ {
		cb.RecordFailure()
		if cb.State() != cbClosed {
			t.Fatalf("state after %d failures = %s; want closed", i+1, cb.State())
		}
	}
	cb.RecordFailure()
	if cb.State() != cbOpen {
		t.Fatalf("state after threshold failures = %s; want open", cb.State())
	}
	if cb.Allow() {
		t.Fatal("Allow() must be false while open")
	}
}

func TestCircuitBreakerHalfOpenSinglePermit(t *testing.T) {
	cb := newCircuitBreaker(1, 10*time.Millisecond)
	cb.RecordFailure()
	if cb.State() != cbOpen {
		t.Fatalf("state = %s; want open", cb.State())
	}
	time.Sleep(20 * time.Millisecond)

	// First Allow() after cooldown promotes to half_open and grants the
	// single probe permit.
	if !cb.Allow() {
		t.Fatal("Allow() after cooldown must grant probe")
	}
	if cb.State() != cbHalfOpen {
		t.Fatalf("state after probe grant = %s; want half_open", cb.State())
	}
	// Concurrent callers must NOT be granted while a probe is in flight.
	for i := 0; i < 10; i++ {
		if cb.Allow() {
			t.Fatalf("Allow() #%d in half_open must deny extra callers", i)
		}
	}
}

func TestCircuitBreakerProbeSuccessCloses(t *testing.T) {
	cb := newCircuitBreaker(1, 10*time.Millisecond)
	cb.RecordFailure()
	time.Sleep(20 * time.Millisecond)
	cb.Allow() // probe granted
	cb.RecordSuccess()
	if cb.State() != cbClosed {
		t.Fatalf("state after probe success = %s; want closed", cb.State())
	}
	if !cb.Allow() {
		t.Fatal("Allow() after close must be true")
	}
}

func TestCircuitBreakerProbeFailureReopens(t *testing.T) {
	cb := newCircuitBreaker(1, 10*time.Millisecond)
	cb.RecordFailure()
	time.Sleep(20 * time.Millisecond)
	cb.Allow() // probe granted (now half_open)
	cb.RecordFailure()
	if cb.State() != cbOpen {
		t.Fatalf("state after probe failure = %s; want open", cb.State())
	}
	if cb.Allow() {
		t.Fatal("Allow() immediately after re-open must be false (cooldown restarted)")
	}
}

// --- Networked tests covering wire-up into Contains/FindMissing -----------

// failingCASUpstream is a CAS server whose FindMissingBlobs always
// returns the configured error after the configured delay (default 0).
type failingCASUpstream struct {
	pb.UnimplementedCapabilitiesServer
	pb.UnimplementedContentAddressableStorageServer
	pb.UnimplementedActionCacheServer
	bs.UnimplementedByteStreamServer

	delay time.Duration
	err   error
	calls atomic.Int64
}

func (s *failingCASUpstream) GetCapabilities(_ context.Context, _ *pb.GetCapabilitiesRequest) (*pb.ServerCapabilities, error) {
	return &pb.ServerCapabilities{
		CacheCapabilities: &pb.CacheCapabilities{
			DigestFunctions:               []pb.DigestFunction_Value{pb.DigestFunction_SHA256},
			ActionCacheUpdateCapabilities: &pb.ActionCacheUpdateCapabilities{UpdateEnabled: true},
		},
	}, nil
}

func (s *failingCASUpstream) FindMissingBlobs(ctx context.Context, _ *pb.FindMissingBlobsRequest) (*pb.FindMissingBlobsResponse, error) {
	s.calls.Add(1)
	if s.delay > 0 {
		select {
		case <-time.After(s.delay):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	if s.err != nil {
		return nil, s.err
	}
	return &pb.FindMissingBlobsResponse{}, nil
}

func newProxyForCAS(t *testing.T, up *failingCASUpstream) (*remoteGrpcProxyCache, func()) {
	t.Helper()
	listener := bufconn.Listen(1024 * 1024)
	srv := grpc.NewServer()
	pb.RegisterCapabilitiesServer(srv, up)
	pb.RegisterContentAddressableStorageServer(srv, up)
	pb.RegisterActionCacheServer(srv, up)
	bs.RegisterByteStreamServer(srv, up)
	go func() { _ = srv.Serve(listener) }()

	dialer := func(context.Context, string) (net.Conn, error) {
		return listener.Dial()
	}
	cc, err := grpc.NewClient("passthrough://bufnet",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(dialer),
	)
	if err != nil {
		t.Fatal(err)
	}
	clients := NewGrpcClients(cc)
	if err := clients.CheckCapabilities(false); err != nil {
		t.Fatal(err)
	}
	p := New(clients, "uncompressed", logger, logger, 1, 1).(*remoteGrpcProxyCache)
	return p, func() {
		srv.Stop()
		_ = cc.Close()
	}
}

// TestHotPathTimeoutFiresFasterThanBackgroundTimeout asserts that when a
// caller marks the context with cache.WithHotPath, an upstream stall is
// killed inside the (short) hot-path timeout window — not the (long)
// background one. This is the regression test for the inbound-handler
// pile-up where every bazel CAS upload was eating up to 10s of upstream
// timeout before bazel-remote could answer.
func TestHotPathTimeoutFiresFasterThanBackgroundTimeout(t *testing.T) {
	prevHot := upstreamFindMissingHotTimeout
	prevBg := upstreamFindMissingTimeout
	upstreamFindMissingHotTimeout = 100 * time.Millisecond
	upstreamFindMissingTimeout = 5 * time.Second
	defer func() {
		upstreamFindMissingHotTimeout = prevHot
		upstreamFindMissingTimeout = prevBg
	}()

	// Upstream blocks for longer than EITHER timeout.
	up := &failingCASUpstream{delay: 2 * time.Second}
	p, cleanup := newProxyForCAS(t, up)
	defer cleanup()

	digests := []cache.Digest{{Hash: "aa", SizeBytes: 1}}

	start := time.Now()
	missing, err := p.FindMissingCasBlobs(cache.WithHotPath(context.Background()), digests)
	elapsed := time.Since(start)

	// The hot-path call must fail with DeadlineExceeded inside the
	// hot-path window (with generous slack for scheduling/grpc).
	if err == nil {
		t.Fatalf("expected DeadlineExceeded, got nil; missing=%v", missing)
	}
	if status.Code(err) != codes.DeadlineExceeded && !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected DeadlineExceeded, got %v", err)
	}
	if elapsed > upstreamFindMissingHotTimeout+1*time.Second {
		t.Fatalf("hot-path call took %v; expected to fail close to %v",
			elapsed, upstreamFindMissingHotTimeout)
	}
	if elapsed >= upstreamFindMissingTimeout {
		t.Fatalf("hot-path call took %v; that's the BACKGROUND timeout — hot-path didn't fire", elapsed)
	}
}

// TestBreakerShortCircuitsFindMissingAfterRepeatedFailures verifies the
// integration: enough failing FindMissing calls open the breaker, after
// which subsequent calls return immediately (treating ALL digests as
// missing) without contacting upstream.
func TestBreakerShortCircuitsFindMissingAfterRepeatedFailures(t *testing.T) {
	prevThreshold := upstreamBreakerThreshold
	prevCooldown := upstreamBreakerCooldown
	prevHot := upstreamFindMissingHotTimeout
	upstreamBreakerThreshold = 3
	upstreamBreakerCooldown = 200 * time.Millisecond
	upstreamFindMissingHotTimeout = 50 * time.Millisecond
	defer func() {
		upstreamBreakerThreshold = prevThreshold
		upstreamBreakerCooldown = prevCooldown
		upstreamFindMissingHotTimeout = prevHot
	}()

	// Upstream that always stalls past the hot-path timeout so each
	// call records a failure on the breaker.
	up := &failingCASUpstream{delay: 1 * time.Second}
	p, cleanup := newProxyForCAS(t, up)
	defer cleanup()

	hotCtx := cache.WithHotPath(context.Background())
	digests := []cache.Digest{
		{Hash: "aa", SizeBytes: 1},
		{Hash: "bb", SizeBytes: 2},
	}

	// Drive the breaker open with `threshold` consecutive failures.
	// Each FindMissingCasBlobs key is unique so we don't get collapsed
	// by singleflight.
	for i := 0; i < upstreamBreakerThreshold; i++ {
		uniq := []cache.Digest{{Hash: hashPad("openf", i), SizeBytes: int64(i + 1)}}
		_, err := p.FindMissingCasBlobs(hotCtx, uniq)
		if err == nil {
			t.Fatalf("call #%d: expected error from stalled upstream, got nil", i)
		}
	}
	if got := p.breaker.State(); got != cbOpen {
		t.Fatalf("breaker state = %s after %d failures; want open",
			got, upstreamBreakerThreshold)
	}

	callsBefore := up.calls.Load()
	missing, err := p.FindMissingCasBlobs(hotCtx, digests)
	if err != nil {
		t.Fatalf("short-circuited call must not error, got %v", err)
	}
	if len(missing) != len(digests) {
		t.Fatalf("short-circuit must report ALL digests missing; got %d/%d", len(missing), len(digests))
	}
	if got := up.calls.Load() - callsBefore; got != 0 {
		t.Fatalf("short-circuited call must NOT contact upstream; got %d new calls", got)
	}
}

// TestBreakerRecoversAfterCooldown verifies: open -> wait cooldown ->
// half_open probe succeeds (we use a healthy upstream for the probe) ->
// breaker closes -> subsequent calls go through normally.
func TestBreakerRecoversAfterCooldown(t *testing.T) {
	prevThreshold := upstreamBreakerThreshold
	prevCooldown := upstreamBreakerCooldown
	prevHot := upstreamFindMissingHotTimeout
	upstreamBreakerThreshold = 1
	upstreamBreakerCooldown = 50 * time.Millisecond
	upstreamFindMissingHotTimeout = 30 * time.Millisecond
	defer func() {
		upstreamBreakerThreshold = prevThreshold
		upstreamBreakerCooldown = prevCooldown
		upstreamFindMissingHotTimeout = prevHot
	}()

	// Upstream that's initially slow (forces breaker open) then
	// becomes fast for the probe.
	up := &failingCASUpstream{delay: 200 * time.Millisecond}
	p, cleanup := newProxyForCAS(t, up)
	defer cleanup()

	hotCtx := cache.WithHotPath(context.Background())

	_, _ = p.FindMissingCasBlobs(hotCtx, []cache.Digest{{Hash: "aa", SizeBytes: 1}})
	if p.breaker.State() != cbOpen {
		t.Fatalf("breaker should be open; got %s", p.breaker.State())
	}

	// Make the upstream healthy so the probe will succeed.
	up.delay = 0

	// Wait past cooldown.
	time.Sleep(upstreamBreakerCooldown + 20*time.Millisecond)

	// First call after cooldown is the probe; should succeed and close
	// the breaker.
	_, err := p.FindMissingCasBlobs(hotCtx, []cache.Digest{{Hash: "probe", SizeBytes: 1}})
	if err != nil {
		t.Fatalf("probe call should succeed, got %v", err)
	}
	if p.breaker.State() != cbClosed {
		t.Fatalf("breaker should close after probe success; state=%s", p.breaker.State())
	}
}

// TestUpstreamCallMetricsAreEmitted verifies the latency histogram and
// outcome counter both fire on a normal upstream call. The exact values
// are not asserted (timing varies); we just confirm the series exists
// with a non-zero sample count.
func TestUpstreamCallMetricsAreEmitted(t *testing.T) {
	up := &failingCASUpstream{}
	p, cleanup := newProxyForCAS(t, up)
	defer cleanup()

	beforeOK := testutil.ToFloat64(upstreamCallOutcome.WithLabelValues("FindMissingCasBlobs", "ok"))

	_, err := p.FindMissingCasBlobs(context.Background(), []cache.Digest{{Hash: "metric", SizeBytes: 1}})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if got := testutil.ToFloat64(upstreamCallOutcome.WithLabelValues("FindMissingCasBlobs", "ok")) - beforeOK; got != 1 {
		t.Fatalf("expected outcome{method=FindMissingCasBlobs,outcome=ok} delta=1, got %v", got)
	}

	// Latency histogram: confirm at least one observation landed.
	mfs, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		t.Fatal(err)
	}
	var found bool
	for _, mf := range mfs {
		if mf.GetName() != "bazel_remote_grpc_proxy_upstream_call_seconds" {
			continue
		}
		for _, m := range mf.GetMetric() {
			for _, l := range m.GetLabel() {
				if l.GetName() == "method" && l.GetValue() == "FindMissingCasBlobs" && m.GetHistogram().GetSampleCount() > 0 {
					found = true
				}
			}
		}
	}
	if !found {
		t.Fatal("expected a non-empty histogram sample for method=FindMissingCasBlobs")
	}
}

// hashPad pads a short string with zeros to look like a hex hash so the
// validateHash-friendly tests don't choke. Tests in this file don't go
// through validateHash, but it's harmless and keeps fixtures readable.
func hashPad(prefix string, i int) string {
	out := []byte(prefix)
	for len(out) < 64 {
		out = append(out, '0')
	}
	out[len(out)-1] = byte('0' + i%10)
	return string(out)
}
