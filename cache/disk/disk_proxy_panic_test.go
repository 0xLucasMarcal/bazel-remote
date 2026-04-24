package disk

import (
	"bytes"
	"context"
	"io"
	"os"
	"testing"

	"github.com/buchgr/bazel-remote/v2/cache"
	testutils "github.com/buchgr/bazel-remote/v2/utils"
)

// panicProxy is a cache.Proxy whose Get returns a reader that panics
// or whose body is garbage zstd. It is used to verify that
// (*diskCache).get does not allow a panic in the zstd decoder to escape
// up to the gRPC handler (which would otherwise crash the process).
type panicProxy struct {
	hash string
	body func() io.ReadCloser
}

func (p *panicProxy) Put(ctx context.Context, kind cache.EntryKind, hash string, logicalSize int64, sizeOnDisk int64, rc io.ReadCloser) {
}

func (p *panicProxy) Get(ctx context.Context, kind cache.EntryKind, hash string, _ int64) (io.ReadCloser, int64, error) {
	if hash != p.hash || kind != cache.CAS {
		return nil, -1, nil
	}
	return p.body(), contentsLength, nil
}

func (p *panicProxy) Contains(ctx context.Context, kind cache.EntryKind, hash string, _ int64) (bool, int64) {
	if hash != p.hash || kind != cache.CAS {
		return false, -1
	}
	return true, contentsLength
}

func (p *panicProxy) FindMissingCasBlobs(ctx context.Context, digests []cache.Digest) ([]cache.Digest, error) {
	return nil, cache.ErrProxyBatchNotImplemented
}

// panickingReader is an io.ReadCloser whose Read deliberately panics.
// It simulates the klauspost/compress zstd decoder panicking on
// adversarial input from the upstream proxy.
type panickingReader struct{}

func (panickingReader) Read(p []byte) (int, error) {
	panic("simulated zstd decoder panic from proxy stream")
}

func (panickingReader) Close() error { return nil }

// TestProxyZstdDecodePanicIsRecovered asserts that when the proxy returns
// a stream whose zstd decode panics, (*diskCache).Get returns an error
// rather than letting the panic propagate (and crash the gRPC server),
// and that nothing is committed to the LRU.
func TestProxyZstdDecodePanicIsRecovered(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cacheDir := tempDir(t)
	defer func() { _ = os.RemoveAll(cacheDir) }()

	pp := &panicProxy{
		hash: contentsHash,
		body: func() io.ReadCloser { return panickingReader{} },
	}

	// Default storage mode is "zstd", which is what triggers the
	// proxy-fetch-then-decode branch in (*diskCache).get.
	testCacheI, err := New(cacheDir, BlockSize,
		WithProxyBackend(pp),
		WithAccessLogger(testutils.NewSilentLogger()),
	)
	if err != nil {
		t.Fatal(err)
	}
	testCache := testCacheI.(*diskCache)

	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("panic escaped from diskCache.Get: %v", r)
		}
	}()

	rdr, _, err := testCache.Get(ctx, cache.CAS, contentsHash, contentsLength, 0)
	if rdr != nil {
		_ = rdr.Close()
		t.Fatal("expected no reader when proxy stream panics during decode")
	}
	if err == nil {
		t.Fatal("expected an error when proxy stream panics during decode")
	}

	if testCache.lru.Len() != 0 {
		t.Fatalf("expected the LRU to stay empty after a recovered panic, found %d items", testCache.lru.Len())
	}
}

// TestProxyMalformedZstdReturnsError asserts that a malformed zstd payload
// from the proxy yields a clean error and an empty LRU, regardless of
// whether the underlying decoder panics or just errors out internally.
func TestProxyMalformedZstdReturnsError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cacheDir := tempDir(t)
	defer func() { _ = os.RemoveAll(cacheDir) }()

	garbage := []byte{
		0x28, 0xb5, 0x2f, 0xfd,
		0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
		0x00, 0x00, 0x00, 0x00,
	}

	pp := &panicProxy{
		hash: contentsHash,
		body: func() io.ReadCloser { return io.NopCloser(bytes.NewReader(garbage)) },
	}

	testCacheI, err := New(cacheDir, BlockSize,
		WithProxyBackend(pp),
		WithAccessLogger(testutils.NewSilentLogger()),
	)
	if err != nil {
		t.Fatal(err)
	}
	testCache := testCacheI.(*diskCache)

	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("panic escaped from diskCache.Get on malformed zstd: %v", r)
		}
	}()

	rdr, _, err := testCache.Get(ctx, cache.CAS, contentsHash, contentsLength, 0)
	if rdr != nil {
		_ = rdr.Close()
		t.Fatal("expected no reader for malformed zstd from proxy")
	}
	if err == nil {
		t.Fatal("expected an error for malformed zstd from proxy")
	}

	if testCache.lru.Len() != 0 {
		t.Fatalf("expected the LRU to stay empty after a malformed proxy stream, found %d items", testCache.lru.Len())
	}
}
