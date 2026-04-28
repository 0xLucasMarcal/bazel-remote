package grpcproxy

import (
	"bytes"
	"context"
	"io"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/buchgr/bazel-remote/v2/cache"
	"github.com/buchgr/bazel-remote/v2/utils/backendproxy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	pb "github.com/buchgr/bazel-remote/v2/genproto/build/bazel/remote/execution/v2"
	bs "google.golang.org/genproto/googleapis/bytestream"
)

// wedgeAfterFirstRecvUpstream simulates an upstream that accepts the
// first Write chunk and then stops responding entirely. The stream looks
// "alive" long enough for the client to make at least one successful
// Send (and thus one idle-timer reset), then stalls — exactly the shape
// observed in the production goroutine dump where uploaders pile up in
// bytestream.Send / CloseAndRecv.
type wedgeAfterFirstRecvUpstream struct {
	pb.UnimplementedCapabilitiesServer
	pb.UnimplementedContentAddressableStorageServer
	pb.UnimplementedActionCacheServer
	bs.UnimplementedByteStreamServer

	received atomic.Int64
}

func (u *wedgeAfterFirstRecvUpstream) GetCapabilities(_ context.Context, _ *pb.GetCapabilitiesRequest) (*pb.ServerCapabilities, error) {
	return &pb.ServerCapabilities{
		CacheCapabilities: &pb.CacheCapabilities{
			DigestFunctions:               []pb.DigestFunction_Value{pb.DigestFunction_SHA256},
			ActionCacheUpdateCapabilities: &pb.ActionCacheUpdateCapabilities{UpdateEnabled: true},
		},
	}, nil
}

func (u *wedgeAfterFirstRecvUpstream) Write(srv bs.ByteStream_WriteServer) error {
	if _, err := srv.Recv(); err != nil {
		return err
	}
	u.received.Add(1)
	<-srv.Context().Done()
	return srv.Context().Err()
}

// slowRecvUpstream simulates an upstream that is slow per-chunk but
// always making forward progress. It models the bandwidth-throttled
// (~1 Mbps) shared upstream described in the deployment notes: each
// Recv lands eventually, well within the idle window.
type slowRecvUpstream struct {
	pb.UnimplementedCapabilitiesServer
	pb.UnimplementedContentAddressableStorageServer
	pb.UnimplementedActionCacheServer
	bs.UnimplementedByteStreamServer

	perChunkDelay time.Duration
	received      atomic.Int64
}

func (u *slowRecvUpstream) GetCapabilities(_ context.Context, _ *pb.GetCapabilitiesRequest) (*pb.ServerCapabilities, error) {
	return &pb.ServerCapabilities{
		CacheCapabilities: &pb.CacheCapabilities{
			DigestFunctions:               []pb.DigestFunction_Value{pb.DigestFunction_SHA256},
			ActionCacheUpdateCapabilities: &pb.ActionCacheUpdateCapabilities{UpdateEnabled: true},
		},
	}, nil
}

func (u *slowRecvUpstream) Write(srv bs.ByteStream_WriteServer) error {
	var committed int64
	for {
		time.Sleep(u.perChunkDelay)
		req, err := srv.Recv()
		if err == io.EOF {
			return srv.SendAndClose(&bs.WriteResponse{CommittedSize: committed})
		}
		if err != nil {
			return err
		}
		committed += int64(len(req.Data))
		u.received.Add(1)
	}
}

// newUploadProxyForUpstream wires `up` (which must implement at least
// the bytestream + capabilities servers) behind a bufconn-backed grpc
// proxy and returns the proxy plus a cleanup func.
func newUploadProxyForUpstream(t *testing.T, up any) (*remoteGrpcProxyCache, func()) {
	t.Helper()

	listener := bufconn.Listen(1024 * 1024)
	srv := grpc.NewServer()
	if c, ok := up.(pb.CapabilitiesServer); ok {
		pb.RegisterCapabilitiesServer(srv, c)
	}
	if c, ok := up.(pb.ContentAddressableStorageServer); ok {
		pb.RegisterContentAddressableStorageServer(srv, c)
	}
	if c, ok := up.(pb.ActionCacheServer); ok {
		pb.RegisterActionCacheServer(srv, c)
	}
	if c, ok := up.(bs.ByteStreamServer); ok {
		bs.RegisterByteStreamServer(srv, c)
	}
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

type nopReadCloser struct{ io.Reader }

func (nopReadCloser) Close() error { return nil }

// TestUploadFileAbortsOnWedgedUpstream verifies that when an upstream
// accepts the start of a Write stream and then never responds again,
// UploadFile returns within roughly upstreamWriteIdleTimeout instead
// of pinning the worker indefinitely. This is the regression test for
// the goroutine pile-up where every UploadFile was parked in
// bytestream.Send / CloseAndRecv.
func TestUploadFileAbortsOnWedgedUpstream(t *testing.T) {
	prevIdle := upstreamWriteIdleTimeout
	upstreamWriteIdleTimeout = 200 * time.Millisecond
	defer func() { upstreamWriteIdleTimeout = prevIdle }()

	up := &wedgeAfterFirstRecvUpstream{}
	p, cleanup := newUploadProxyForUpstream(t, up)
	defer cleanup()

	payload := make([]byte, 256*1024)
	item := backendproxy.UploadReq{
		Hash:           "deadbeef",
		LogicalSize:    int64(len(payload)),
		SizeOnDisk:     int64(len(payload)),
		Kind:           cache.CAS,
		Rc:             nopReadCloser{Reader: bytes.NewReader(payload)},
		DigestFunction: cache.DigestFunctionSHA256,
	}

	done := make(chan struct{})
	start := time.Now()
	go func() {
		p.UploadFile(item)
		close(done)
	}()

	// Generous outer bound so a flaky CI machine doesn't cause a
	// hang; the meaningful assertion is the elapsed-time check below.
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatalf("UploadFile did not return; expected abort within ~%v", upstreamWriteIdleTimeout)
	}

	elapsed := time.Since(start)
	// Allow up to 5x the idle timeout to account for scheduler jitter
	// and gRPC teardown, but still well under the previous 5-minute
	// fixed deadline that motivated this change.
	if elapsed > 5*upstreamWriteIdleTimeout+2*time.Second {
		t.Fatalf("UploadFile took %v; expected to abort close to %v", elapsed, upstreamWriteIdleTimeout)
	}
	if up.received.Load() < 1 {
		t.Fatalf("expected upstream to observe at least 1 chunk, got %d", up.received.Load())
	}
}

// TestUploadFileSurvivesSlowProgressingUpstream verifies that an
// upstream which is slow per-chunk but always making forward progress
// is NOT killed by the idle timeout — every successful Send resets it.
// This guards against a regression where a fixed end-to-end deadline
// would abort a legitimate slow-but-progressing upload (the failure
// mode that retries would then amplify into more upstream pressure).
func TestUploadFileSurvivesSlowProgressingUpstream(t *testing.T) {
	prevIdle := upstreamWriteIdleTimeout
	upstreamWriteIdleTimeout = 300 * time.Millisecond
	defer func() { upstreamWriteIdleTimeout = prevIdle }()

	// Each Recv arrives ~50ms apart — well under the 300ms idle
	// window, but the cumulative upload spans many idle windows.
	up := &slowRecvUpstream{perChunkDelay: 50 * time.Millisecond}
	p, cleanup := newUploadProxyForUpstream(t, up)
	defer cleanup()

	// 5 MB at maxChunkSize=2MB → 3 Send calls + a final FinishWrite
	// + CloseAndRecv. Total artificial delay ≈ 200–250ms, comfortably
	// longer than a single idle window but short enough for CI.
	payload := make([]byte, 5*1024*1024)
	item := backendproxy.UploadReq{
		Hash:           "cafebabe",
		LogicalSize:    int64(len(payload)),
		SizeOnDisk:     int64(len(payload)),
		Kind:           cache.CAS,
		Rc:             nopReadCloser{Reader: bytes.NewReader(payload)},
		DigestFunction: cache.DigestFunctionSHA256,
	}

	done := make(chan struct{})
	go func() {
		p.UploadFile(item)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatalf("UploadFile did not complete on a slow-but-progressing upstream within 10s")
	}

	if got := up.received.Load(); got < 3 {
		t.Fatalf("expected upstream to Recv at least 3 chunks, got %d", got)
	}
}
