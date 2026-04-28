package grpcproxy

import (
	"context"
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/buchgr/bazel-remote/v2/cache"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	pb "github.com/buchgr/bazel-remote/v2/genproto/build/bazel/remote/execution/v2"
	bs "google.golang.org/genproto/googleapis/bytestream"
)

// slowUpstream is a minimal CAS+Capabilities server whose FindMissingBlobs
// blocks until released (or its context is cancelled). It also counts
// invocations so tests can assert singleflight collapsing.
type slowUpstream struct {
	pb.UnimplementedContentAddressableStorageServer
	pb.UnimplementedCapabilitiesServer
	pb.UnimplementedActionCacheServer
	bs.UnimplementedByteStreamServer

	release   chan struct{}
	callCount atomic.Int64
}

func (s *slowUpstream) GetCapabilities(ctx context.Context, req *pb.GetCapabilitiesRequest) (*pb.ServerCapabilities, error) {
	return &pb.ServerCapabilities{
		CacheCapabilities: &pb.CacheCapabilities{
			DigestFunctions:               []pb.DigestFunction_Value{pb.DigestFunction_SHA256},
			ActionCacheUpdateCapabilities: &pb.ActionCacheUpdateCapabilities{UpdateEnabled: true},
			SupportedCompressors:          []pb.Compressor_Value{pb.Compressor_IDENTITY, pb.Compressor_ZSTD},
		},
	}, nil
}

func (s *slowUpstream) FindMissingBlobs(ctx context.Context, req *pb.FindMissingBlobsRequest) (*pb.FindMissingBlobsResponse, error) {
	s.callCount.Add(1)
	select {
	case <-s.release:
		return &pb.FindMissingBlobsResponse{}, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func newSlowUpstreamProxy(t *testing.T) (*slowUpstream, *remoteGrpcProxyCache, func()) {
	t.Helper()

	listener := bufconn.Listen(1024 * 1024)
	srv := grpc.NewServer()
	up := &slowUpstream{release: make(chan struct{})}
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

	cleanup := func() {
		srv.Stop()
		_ = cc.Close()
	}
	return up, p, cleanup
}

// TestFindMissingCasBlobsRespectsUpstreamTimeout verifies that a wedged
// upstream cannot indefinitely block FindMissingCasBlobs callers — they
// must observe DeadlineExceeded within roughly upstreamFindMissingTimeout
// regardless of caller-provided context.
func TestFindMissingCasBlobsRespectsUpstreamTimeout(t *testing.T) {
	if testing.Short() {
		t.Skip("respects upstream timeout: long test")
	}

	up, p, cleanup := newSlowUpstreamProxy(t)
	defer cleanup()
	defer close(up.release)

	digests := []cache.Digest{
		{Hash: "aa", SizeBytes: 1},
	}

	start := time.Now()
	_, err := p.FindMissingCasBlobs(context.Background(), digests)
	elapsed := time.Since(start)

	if err == nil {
		t.Fatalf("expected an error from a wedged upstream, got nil after %v", elapsed)
	}
	if got := status.Code(err); got != codes.DeadlineExceeded && !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected DeadlineExceeded, got code=%v err=%v", got, err)
	}
	if elapsed > upstreamFindMissingTimeout+5*time.Second {
		t.Fatalf("call took %v, expected to fail close to %v", elapsed, upstreamFindMissingTimeout)
	}
}

// TestFindMissingCasBlobsSingleflight verifies that concurrent calls for
// the same digest set produce exactly one upstream RPC, and that the
// digest order does not affect deduplication.
func TestFindMissingCasBlobsSingleflight(t *testing.T) {
	up, p, cleanup := newSlowUpstreamProxy(t)
	defer cleanup()

	digestsA := []cache.Digest{
		{Hash: "aa", SizeBytes: 1},
		{Hash: "bb", SizeBytes: 2},
	}
	// Same set, different order — must collapse via the order-independent key.
	digestsB := []cache.Digest{
		{Hash: "bb", SizeBytes: 2},
		{Hash: "aa", SizeBytes: 1},
	}

	const numCallers = 8

	var wg sync.WaitGroup
	errs := make(chan error, numCallers)
	for i := 0; i < numCallers; i++ {
		wg.Add(1)
		ds := digestsA
		if i%2 == 1 {
			ds = digestsB
		}
		go func() {
			defer wg.Done()
			_, err := p.FindMissingCasBlobs(context.Background(), ds)
			errs <- err
		}()
	}

	// Give all callers a chance to enter singleflight before releasing.
	time.Sleep(100 * time.Millisecond)

	// Release the upstream so the single in-flight RPC returns.
	close(up.release)
	wg.Wait()
	close(errs)

	for err := range errs {
		if err != nil {
			t.Fatalf("unexpected error from caller: %v", err)
		}
	}

	if got := up.callCount.Load(); got != 1 {
		t.Fatalf("expected exactly 1 upstream call, got %d", got)
	}
}

// TestFindMissingSingleflightKeyStable verifies the key is deterministic
// and order-independent (the property singleflight relies on).
func TestFindMissingSingleflightKeyStable(t *testing.T) {
	d1 := []cache.Digest{
		{Hash: "aa", SizeBytes: 1},
		{Hash: "bb", SizeBytes: 2},
	}
	d2 := []cache.Digest{
		{Hash: "bb", SizeBytes: 2},
		{Hash: "aa", SizeBytes: 1},
	}
	if k1, k2 := findMissingSingleflightKey(pb.DigestFunction_SHA256, d1), findMissingSingleflightKey(pb.DigestFunction_SHA256, d2); k1 != k2 {
		t.Fatalf("expected order-independent key, got distinct results")
	}

	d3 := []cache.Digest{
		{Hash: "aa", SizeBytes: 1},
		{Hash: "bb", SizeBytes: 3}, // different size
	}
	if k1, k2 := findMissingSingleflightKey(pb.DigestFunction_SHA256, d1), findMissingSingleflightKey(pb.DigestFunction_SHA256, d3); k1 == k2 {
		t.Fatalf("expected different key when SizeBytes differs")
	}

	if k1, k2 := findMissingSingleflightKey(pb.DigestFunction_SHA256, d1), findMissingSingleflightKey(pb.DigestFunction_BLAKE3, d1); k1 == k2 {
		t.Fatalf("expected different key for different DigestFunction")
	}
}
