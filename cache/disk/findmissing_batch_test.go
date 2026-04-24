package disk

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"sync/atomic"
	"testing"

	"github.com/buchgr/bazel-remote/v2/cache"
	testutils "github.com/buchgr/bazel-remote/v2/utils"
	"google.golang.org/protobuf/proto"

	pb "github.com/buchgr/bazel-remote/v2/genproto/build/bazel/remote/execution/v2"
)

// batchProxy is a counting cache.Proxy used to verify that
// findMissingCasBlobsInternal collapses N per-blob Contains lookups into
// one FindMissingCasBlobs RPC when the proxy supports it.
type batchProxy struct {
	containsCount       atomic.Int64
	findMissingCount    atomic.Int64
	findMissingBatchLen atomic.Int64

	missing map[string]struct{}

	// returnErr, if set, is returned from FindMissingCasBlobs in place
	// of the normal response.
	returnErr error
}

func (p *batchProxy) Put(ctx context.Context, kind cache.EntryKind, hash string, logicalSize int64, sizeOnDisk int64, rc io.ReadCloser) {
}

func (p *batchProxy) Get(ctx context.Context, kind cache.EntryKind, hash string, _ int64) (io.ReadCloser, int64, error) {
	return nil, -1, nil
}

func (p *batchProxy) Contains(ctx context.Context, kind cache.EntryKind, hash string, size int64) (bool, int64) {
	p.containsCount.Add(1)
	if _, isMissing := p.missing[hash]; isMissing {
		return false, -1
	}
	return true, size
}

func (p *batchProxy) FindMissingCasBlobs(ctx context.Context, digests []cache.Digest) ([]cache.Digest, error) {
	p.findMissingCount.Add(1)
	p.findMissingBatchLen.Add(int64(len(digests)))
	if p.returnErr != nil {
		return nil, p.returnErr
	}
	var out []cache.Digest
	for _, d := range digests {
		if _, isMissing := p.missing[d.Hash]; isMissing {
			out = append(out, d)
		}
	}
	return out, nil
}

// TestFindMissingCasBlobsUsesBatchProxy verifies that when the proxy
// implements FindMissingCasBlobs, the disk cache issues exactly one
// batch RPC (per chunk) instead of N per-blob Contains RPCs.
func TestFindMissingCasBlobsUsesBatchProxy(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cacheDir := tempDir(t)
	defer func() { _ = os.RemoveAll(cacheDir) }()

	const numBlobs = 5
	digests := make([]*pb.Digest, numBlobs)
	missing := make(map[string]struct{}, numBlobs/2)
	for i := 0; i < numBlobs; i++ {
		_, d := testutils.RandomDataAndDigest(100)
		digests[i] = &d
		if i%2 == 0 {
			missing[d.Hash] = struct{}{}
		}
	}

	bp := &batchProxy{missing: missing}

	testCacheI, err := New(cacheDir, 10*1024,
		WithProxyBackend(bp),
		WithAccessLogger(testutils.NewSilentLogger()),
	)
	if err != nil {
		t.Fatal(err)
	}

	gotMissing, err := testCacheI.FindMissingCasBlobs(ctx, digests)
	if err != nil {
		t.Fatal(err)
	}

	if got := bp.containsCount.Load(); got != 0 {
		t.Errorf("expected zero per-blob Contains calls when batch is supported, got %d", got)
	}

	if got := bp.findMissingCount.Load(); got != 1 {
		t.Errorf("expected exactly one batch FindMissingCasBlobs call (numBlobs=%d <= chunk=20), got %d", numBlobs, got)
	}

	if got := bp.findMissingBatchLen.Load(); got != int64(numBlobs) {
		t.Errorf("expected the batch call to receive all %d digests, got %d", numBlobs, got)
	}

	wantMissing := make([]string, 0, len(missing))
	for h := range missing {
		wantMissing = append(wantMissing, h)
	}
	sort.Strings(wantMissing)

	gotHashes := make([]string, 0, len(gotMissing))
	for _, d := range gotMissing {
		gotHashes = append(gotHashes, d.Hash)
	}
	sort.Strings(gotHashes)

	if fmt.Sprintf("%v", wantMissing) != fmt.Sprintf("%v", gotHashes) {
		t.Errorf("missing hashes mismatch: want %v, got %v", wantMissing, gotHashes)
	}
}

// TestFindMissingCasBlobsBatchChunking verifies that when the request
// exceeds the internal batchSize (20), the batch RPC is invoked once
// per chunk rather than degenerating to per-blob calls.
func TestFindMissingCasBlobsBatchChunking(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cacheDir := tempDir(t)
	defer func() { _ = os.RemoveAll(cacheDir) }()

	const numBlobs = 50 // > 2 * batchSize(20), so 3 chunks expected
	digests := make([]*pb.Digest, numBlobs)
	for i := 0; i < numBlobs; i++ {
		_, d := testutils.RandomDataAndDigest(100)
		digests[i] = &d
	}

	bp := &batchProxy{missing: map[string]struct{}{}} // proxy claims to have everything

	testCacheI, err := New(cacheDir, 10*1024,
		WithProxyBackend(bp),
		WithAccessLogger(testutils.NewSilentLogger()),
	)
	if err != nil {
		t.Fatal(err)
	}

	gotMissing, err := testCacheI.FindMissingCasBlobs(ctx, digests)
	if err != nil {
		t.Fatal(err)
	}
	if len(gotMissing) != 0 {
		t.Fatalf("expected nothing missing (proxy claims to have all blobs), got %d missing", len(gotMissing))
	}

	if got := bp.containsCount.Load(); got != 0 {
		t.Errorf("expected zero per-blob Contains calls, got %d", got)
	}

	wantBatchCalls := int64((numBlobs + 19) / 20)
	if got := bp.findMissingCount.Load(); got != wantBatchCalls {
		t.Errorf("expected %d batch calls (one per chunk of 20), got %d", wantBatchCalls, got)
	}

	if got := bp.findMissingBatchLen.Load(); got != int64(numBlobs) {
		t.Errorf("expected the batch calls to receive all %d digests in aggregate, got %d", numBlobs, got)
	}
}

// TestFindMissingCasBlobsBatchErrorIsTreatedAsMissing verifies that a
// non-NotImplemented error from the batch RPC degrades to "everything
// is missing", which mirrors the per-blob path's behaviour where a
// proxy.Contains error returns false.
func TestFindMissingCasBlobsBatchErrorIsTreatedAsMissing(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cacheDir := tempDir(t)
	defer func() { _ = os.RemoveAll(cacheDir) }()

	bp := &batchProxy{
		missing:   map[string]struct{}{},
		returnErr: errors.New("simulated transient backend error"),
	}

	testCacheI, err := New(cacheDir, 10*1024,
		WithProxyBackend(bp),
		WithAccessLogger(testutils.NewSilentLogger()),
	)
	if err != nil {
		t.Fatal(err)
	}

	_, d1 := testutils.RandomDataAndDigest(100)
	_, d2 := testutils.RandomDataAndDigest(200)

	gotMissing, err := testCacheI.FindMissingCasBlobs(ctx, []*pb.Digest{&d1, &d2})
	if err != nil {
		t.Fatalf("expected nil error in non-failFast mode, got %v", err)
	}

	if len(gotMissing) != 2 {
		t.Fatalf("expected both blobs to be reported missing on batch error, got %d", len(gotMissing))
	}

	if !proto.Equal(gotMissing[0], &d1) || !proto.Equal(gotMissing[1], &d2) {
		t.Fatalf("expected missing == [d1, d2], got %v", gotMissing)
	}

	if got := bp.containsCount.Load(); got != 0 {
		t.Errorf("expected no fallback to Contains on a real (non-NotImplemented) batch error, got %d Contains calls", got)
	}
}

// TestFindMissingCasBlobsBatchNotImplementedFallsBackToContains verifies
// that when the proxy returns ErrProxyBatchNotImplemented, the disk
// cache transparently falls back to the per-blob containsQueue path
// (the existing pre-PR1 behaviour).
func TestFindMissingCasBlobsBatchNotImplementedFallsBackToContains(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cacheDir := tempDir(t)
	defer func() { _ = os.RemoveAll(cacheDir) }()

	bp := &batchProxy{
		missing:   map[string]struct{}{},
		returnErr: cache.ErrProxyBatchNotImplemented,
	}

	testCacheI, err := New(cacheDir, 10*1024,
		WithProxyBackend(bp),
		WithAccessLogger(testutils.NewSilentLogger()),
	)
	if err != nil {
		t.Fatal(err)
	}

	const numBlobs = 4
	digests := make([]*pb.Digest, numBlobs)
	for i := 0; i < numBlobs; i++ {
		_, d := testutils.RandomDataAndDigest(100)
		digests[i] = &d
	}

	gotMissing, err := testCacheI.FindMissingCasBlobs(ctx, digests)
	if err != nil {
		t.Fatal(err)
	}
	if len(gotMissing) != 0 {
		t.Fatalf("expected nothing missing (proxy claims to have all blobs via Contains), got %d", len(gotMissing))
	}

	if got := bp.findMissingCount.Load(); got != 1 {
		t.Errorf("expected one batch attempt before fallback, got %d", got)
	}

	if got := bp.containsCount.Load(); got != int64(numBlobs) {
		t.Errorf("expected %d per-blob Contains calls after batch returned ErrProxyBatchNotImplemented, got %d", numBlobs, got)
	}
}
