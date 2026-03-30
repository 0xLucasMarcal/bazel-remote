package config

import (
	"context"
	"net"
	"testing"

	pb "github.com/buchgr/bazel-remote/v2/genproto/build/bazel/remote/execution/v2"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/test/bufconn"
)

type metadataCapturingServer struct {
	pb.UnimplementedCapabilitiesServer
	captured metadata.MD
}

func (s *metadataCapturingServer) GetCapabilities(ctx context.Context, _ *pb.GetCapabilitiesRequest) (*pb.ServerCapabilities, error) {
	s.captured, _ = metadata.FromIncomingContext(ctx)
	return &pb.ServerCapabilities{
		CacheCapabilities: &pb.CacheCapabilities{
			DigestFunctions:               []pb.DigestFunction_Value{pb.DigestFunction_SHA256},
			ActionCacheUpdateCapabilities: &pb.ActionCacheUpdateCapabilities{UpdateEnabled: true},
			SupportedCompressors: []pb.Compressor_Value{
				pb.Compressor_IDENTITY,
				pb.Compressor_ZSTD,
			},
		},
	}, nil
}

func startCapturingServer(t *testing.T) (*metadataCapturingServer, func(context.Context, string) (net.Conn, error), func()) {
	t.Helper()
	capServer := &metadataCapturingServer{}
	listener := bufconn.Listen(1024 * 1024)
	srv := grpc.NewServer()
	pb.RegisterCapabilitiesServer(srv, capServer)
	go func() { _ = srv.Serve(listener) }()

	dialer := func(context.Context, string) (net.Conn, error) {
		return listener.Dial()
	}
	return capServer, dialer, srv.Stop
}

func TestGRPCProxyCustomHeadersForwarded(t *testing.T) {
	capServer, dialer, stop := startCapturingServer(t)
	defer stop()

	headers := map[string]string{
		"x-buildbuddy-api-key": "test-api-key-123",
		"x-custom-header":     "custom-value",
	}

	var metadataKVs []string
	for k, v := range headers {
		metadataKVs = append(metadataKVs, k, v)
	}

	unaryMD := func(ctx context.Context, method string, req, res interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		return invoker(metadata.AppendToOutgoingContext(ctx, metadataKVs...), method, req, res, cc, opts...)
	}

	conn, err := grpc.NewClient("passthrough://bufnet",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(dialer),
		grpc.WithChainUnaryInterceptor(unaryMD),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	client := pb.NewCapabilitiesClient(conn)
	_, err = client.GetCapabilities(context.Background(), &pb.GetCapabilitiesRequest{})
	if err != nil {
		t.Fatal(err)
	}

	for k, v := range headers {
		got := capServer.captured.Get(k)
		if len(got) == 0 || got[0] != v {
			t.Errorf("expected header %s=%s, got %v", k, v, got)
		}
	}
}

func TestGRPCProxyBasicAuthAndCustomHeadersCombined(t *testing.T) {
	capServer, dialer, stop := startCapturingServer(t)
	defer stop()

	metadataKVs := []string{
		"authorization", "Basic dXNlcjpwYXNz",
		"x-buildbuddy-api-key", "my-api-key",
	}

	unaryMD := func(ctx context.Context, method string, req, res interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		return invoker(metadata.AppendToOutgoingContext(ctx, metadataKVs...), method, req, res, cc, opts...)
	}

	conn, err := grpc.NewClient("passthrough://bufnet",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(dialer),
		grpc.WithChainUnaryInterceptor(unaryMD),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	client := pb.NewCapabilitiesClient(conn)
	_, err = client.GetCapabilities(context.Background(), &pb.GetCapabilitiesRequest{})
	if err != nil {
		t.Fatal(err)
	}

	authVals := capServer.captured.Get("authorization")
	if len(authVals) == 0 || authVals[0] != "Basic dXNlcjpwYXNz" {
		t.Errorf("expected Basic auth header, got %v", authVals)
	}

	apiKeyVals := capServer.captured.Get("x-buildbuddy-api-key")
	if len(apiKeyVals) == 0 || apiKeyVals[0] != "my-api-key" {
		t.Errorf("expected x-buildbuddy-api-key=my-api-key, got %v", apiKeyVals)
	}
}

func TestGRPCProxyNoHeadersNoInterceptor(t *testing.T) {
	capServer, dialer, stop := startCapturingServer(t)
	defer stop()

	conn, err := grpc.NewClient("passthrough://bufnet",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(dialer),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	client := pb.NewCapabilitiesClient(conn)
	_, err = client.GetCapabilities(context.Background(), &pb.GetCapabilitiesRequest{})
	if err != nil {
		t.Fatal(err)
	}

	if vals := capServer.captured.Get("x-buildbuddy-api-key"); len(vals) > 0 {
		t.Errorf("expected no x-buildbuddy-api-key header, got %v", vals)
	}
}
