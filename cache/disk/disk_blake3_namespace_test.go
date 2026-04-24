package disk

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/buchgr/bazel-remote/v2/cache"
	pb "github.com/buchgr/bazel-remote/v2/genproto/build/bazel/remote/execution/v2"
	testutils "github.com/buchgr/bazel-remote/v2/utils"

	"github.com/zeebo/blake3"
)

func sha256Hex(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

func blake3Hex(b []byte) string {
	h := blake3.New()
	_, _ = h.Write(b)
	return hex.EncodeToString(h.Sum(nil))
}

// TestSameHexHashDoesNotCollideAcrossDigestFunctions writes two AC
// entries that share the same hex hash but are issued under different
// digest functions, and verifies that PUT/GET/Contains all keep them
// independent. AC entries are used because they are stored verbatim
// (no content hash verification), which lets us simulate a "real"
// cross-function hex collision without finding one.
func TestSameHexHashDoesNotCollideAcrossDigestFunctions(t *testing.T) {
	cacheDir := tempDir(t)
	defer func() { _ = os.RemoveAll(cacheDir) }()

	dcI, err := New(cacheDir, 1<<20, WithAccessLogger(testutils.NewSilentLogger()))
	if err != nil {
		t.Fatal(err)
	}
	dc := dcI.(*diskCache)

	const hash = "1111111111111111111111111111111111111111111111111111111111111111"

	shaData := []byte("sha256-payload")
	blakeData := []byte("blake3-payload-different")

	shaCtx := cache.WithDigestFunction(context.Background(), cache.DigestFunctionSHA256)
	blakeCtx := cache.WithDigestFunction(context.Background(), cache.DigestFunctionBLAKE3)

	if err := dc.Put(shaCtx, cache.AC, hash, int64(len(shaData)), bytes.NewReader(shaData)); err != nil {
		t.Fatalf("sha256 Put failed: %v", err)
	}
	if err := dc.Put(blakeCtx, cache.AC, hash, int64(len(blakeData)), bytes.NewReader(blakeData)); err != nil {
		t.Fatalf("blake3 Put failed: %v", err)
	}

	if got := readACAll(t, dc, shaCtx, hash, int64(len(shaData))); !bytes.Equal(got, shaData) {
		t.Fatalf("sha256 Get returned wrong bytes: got %q, want %q", got, shaData)
	}
	if got := readACAll(t, dc, blakeCtx, hash, int64(len(blakeData))); !bytes.Equal(got, blakeData) {
		t.Fatalf("blake3 Get returned wrong bytes: got %q, want %q", got, blakeData)
	}

	if ok, sz := dc.Contains(shaCtx, cache.AC, hash, int64(len(shaData))); !ok || sz != int64(len(shaData)) {
		t.Fatalf("sha256 Contains returned (%v, %d), want (true, %d)", ok, sz, len(shaData))
	}
	if ok, sz := dc.Contains(blakeCtx, cache.AC, hash, int64(len(blakeData))); !ok || sz != int64(len(blakeData)) {
		t.Fatalf("blake3 Contains returned (%v, %d), want (true, %d)", ok, sz, len(blakeData))
	}

	// Asking for the SHA256 entry's size in the BLAKE3 namespace must
	// not be satisfied by the SHA256 entry, and vice versa. Either the
	// other namespace's entry is missing, or - if it exists - its size
	// disagrees, so isSizeMismatch must reject it.
	if ok, _ := dc.Contains(blakeCtx, cache.AC, hash, int64(len(shaData))); ok && len(shaData) != len(blakeData) {
		t.Fatalf("blake3 Contains was satisfied by the SHA256 entry: collision!")
	}
	if ok, _ := dc.Contains(shaCtx, cache.AC, hash, int64(len(blakeData))); ok && len(shaData) != len(blakeData) {
		t.Fatalf("sha256 Contains was satisfied by the BLAKE3 entry: collision!")
	}
}

// TestFindMissingBlobsUsesDigestFunctionNamespace stores a CAS blob
// under SHA256 only, then queries FindMissingCasBlobs under BLAKE3
// for the same hex hash. The blob must be reported as missing in
// the BLAKE3 namespace.
func TestFindMissingBlobsUsesDigestFunctionNamespace(t *testing.T) {
	cacheDir := tempDir(t)
	defer func() { _ = os.RemoveAll(cacheDir) }()

	dcI, err := New(cacheDir, 1<<20, WithAccessLogger(testutils.NewSilentLogger()))
	if err != nil {
		t.Fatal(err)
	}
	dc := dcI.(*diskCache)

	payload := []byte("a CAS blob written under SHA256")
	shaHash := sha256Hex(payload)
	bHash := blake3Hex(payload)

	shaCtx := cache.WithDigestFunction(context.Background(), cache.DigestFunctionSHA256)
	if err := dc.Put(shaCtx, cache.CAS, shaHash, int64(len(payload)), bytes.NewReader(payload)); err != nil {
		t.Fatalf("sha256 Put failed: %v", err)
	}

	// Same payload under BLAKE3 - hex hash differs so no collision yet.
	blakeCtx := cache.WithDigestFunction(context.Background(), cache.DigestFunctionBLAKE3)

	// Probing the SHA256 hex under BLAKE3: must be missing because
	// the BLAKE3 namespace is empty.
	missing, err := dc.FindMissingCasBlobs(blakeCtx, []*pb.Digest{
		{Hash: shaHash, SizeBytes: int64(len(payload))},
	})
	if err != nil {
		t.Fatalf("blake3 FindMissingCasBlobs failed: %v", err)
	}
	if len(missing) != 1 || missing[0].Hash != shaHash {
		t.Fatalf("blake3 FindMissingCasBlobs reported %+v, want one missing %s", missing, shaHash)
	}

	// Probing the same SHA256 hex under SHA256: must be present.
	missing, err = dc.FindMissingCasBlobs(shaCtx, []*pb.Digest{
		{Hash: shaHash, SizeBytes: int64(len(payload))},
	})
	if err != nil {
		t.Fatalf("sha256 FindMissingCasBlobs failed: %v", err)
	}
	if len(missing) != 0 {
		t.Fatalf("sha256 FindMissingCasBlobs reported missing: %+v", missing)
	}

	// Now write the same payload under BLAKE3 (using the real BLAKE3
	// hex of the payload, since CAS validates content). Both namespaces
	// must coexist on disk.
	if err := dc.Put(blakeCtx, cache.CAS, bHash, int64(len(payload)), bytes.NewReader(payload)); err != nil {
		t.Fatalf("blake3 Put failed: %v", err)
	}

	missing, err = dc.FindMissingCasBlobs(blakeCtx, []*pb.Digest{
		{Hash: bHash, SizeBytes: int64(len(payload))},
	})
	if err != nil {
		t.Fatalf("blake3 FindMissingCasBlobs (after put) failed: %v", err)
	}
	if len(missing) != 0 {
		t.Fatalf("blake3 FindMissingCasBlobs reported missing after put: %+v", missing)
	}
}

// TestBlake3StoredInSeparateSubtree verifies the on-disk layout: a
// BLAKE3 AC blob lives under ac.v2/blake3/<hexpair>/, while a SHA256
// blob with the same hex hash lives at the legacy ac.v2/<hexpair>/
// path. This is what gives us collision safety without having to
// migrate any existing on-disk data.
func TestBlake3StoredInSeparateSubtree(t *testing.T) {
	cacheDir := tempDir(t)
	defer func() { _ = os.RemoveAll(cacheDir) }()

	dcI, err := New(cacheDir, 1<<20, WithAccessLogger(testutils.NewSilentLogger()))
	if err != nil {
		t.Fatal(err)
	}
	dc := dcI.(*diskCache)

	const hash = "abcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcd"
	data := []byte("payload")

	blakeCtx := cache.WithDigestFunction(context.Background(), cache.DigestFunctionBLAKE3)
	if err := dc.Put(blakeCtx, cache.AC, hash, int64(len(data)), bytes.NewReader(data)); err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	blakeDir := filepath.Join(cacheDir, "ac.v2", "blake3", hash[:2])
	entries, err := os.ReadDir(blakeDir)
	if err != nil {
		t.Fatalf("expected blake3 subtree to exist at %s: %v", blakeDir, err)
	}
	found := false
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), hash+"-") {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("did not find a blob for %s under %s; entries=%v", hash, blakeDir, entries)
	}

	legacyDir := filepath.Join(cacheDir, "ac.v2", hash[:2])
	legacyEntries, err := os.ReadDir(legacyDir)
	if err != nil {
		t.Fatalf("expected legacy SHA256 subtree to still exist (empty) at %s: %v", legacyDir, err)
	}
	for _, e := range legacyEntries {
		if strings.HasPrefix(e.Name(), hash+"-") {
			t.Fatalf("blake3 Put leaked a file into the SHA256 subtree: %s/%s", legacyDir, e.Name())
		}
	}
}

// TestLoaderPicksUpBothNamespaces stores an AC entry under each
// digest function at the same hex hash, drops the cache instance,
// then re-opens the same directory. After reload both entries must
// still be retrievable under their respective digest functions,
// proving the loader walks the new namespaced subtree as well as the
// legacy SHA256 layout.
func TestLoaderPicksUpBothNamespaces(t *testing.T) {
	cacheDir := tempDir(t)
	defer func() { _ = os.RemoveAll(cacheDir) }()

	const hash = "2222222222222222222222222222222222222222222222222222222222222222"
	shaData := []byte("sha-bytes-from-disk-after-reload")
	blakeData := []byte("blake3-bytes-from-disk-after-reload-different")

	{
		first, err := New(cacheDir, 1<<20, WithAccessLogger(testutils.NewSilentLogger()))
		if err != nil {
			t.Fatal(err)
		}
		dc := first.(*diskCache)

		shaCtx := cache.WithDigestFunction(context.Background(), cache.DigestFunctionSHA256)
		blakeCtx := cache.WithDigestFunction(context.Background(), cache.DigestFunctionBLAKE3)

		if err := dc.Put(shaCtx, cache.AC, hash, int64(len(shaData)), bytes.NewReader(shaData)); err != nil {
			t.Fatalf("sha256 Put failed: %v", err)
		}
		if err := dc.Put(blakeCtx, cache.AC, hash, int64(len(blakeData)), bytes.NewReader(blakeData)); err != nil {
			t.Fatalf("blake3 Put failed: %v", err)
		}
	}

	reopened, err := New(cacheDir, 1<<20, WithAccessLogger(testutils.NewSilentLogger()))
	if err != nil {
		t.Fatalf("reopening cache failed: %v", err)
	}
	dc := reopened.(*diskCache)

	shaCtx := cache.WithDigestFunction(context.Background(), cache.DigestFunctionSHA256)
	blakeCtx := cache.WithDigestFunction(context.Background(), cache.DigestFunctionBLAKE3)

	if got := readACAll(t, dc, shaCtx, hash, int64(len(shaData))); !bytes.Equal(got, shaData) {
		t.Fatalf("after reload sha256 Get returned %q, want %q", got, shaData)
	}
	if got := readACAll(t, dc, blakeCtx, hash, int64(len(blakeData))); !bytes.Equal(got, blakeData) {
		t.Fatalf("after reload blake3 Get returned %q, want %q", got, blakeData)
	}
}

func readACAll(t *testing.T, dc *diskCache, ctx context.Context, hash string, size int64) []byte {
	t.Helper()
	rc, _, err := dc.Get(ctx, cache.AC, hash, size, 0)
	if err != nil {
		t.Fatalf("Get(%s) failed: %v", hash, err)
	}
	if rc == nil {
		t.Fatalf("Get(%s) returned nil reader", hash)
	}
	defer rc.Close()
	b, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("read Get(%s): %v", hash, err)
	}
	return b
}
