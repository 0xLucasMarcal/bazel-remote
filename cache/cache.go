package cache

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"hash"
	"io"

	"github.com/zeebo/blake3"
)

// ErrProxyBatchNotImplemented is returned by Proxy implementations whose
// backend cannot answer FindMissingCasBlobs in a single round-trip.
// Callers receiving this error must fall back to per-blob Contains lookups.
var ErrProxyBatchNotImplemented = errors.New("proxy backend does not implement FindMissingCasBlobs")

// Digest is a small mirror of build.bazel.remote.execution.v2.Digest used at
// the cache.Proxy boundary so the Proxy interface does not have to import
// the REAPI protobufs (which would couple all proxy implementations to it).
type Digest struct {
	Hash      string
	SizeBytes int64
}

type contextKey string

const actionDigestSizeKey contextKey = "actionDigestSize"
const digestFunctionKey contextKey = "digestFunction"
const hotPathKey contextKey = "hotPath"

// WithHotPath marks ctx as belonging to a "hot" client-blocking call
// path — i.e. a request where a bazel client is synchronously waiting
// on bazel-remote's response. Proxy implementations may use this hint
// to apply tighter per-RPC timeouts and/or short-circuit when a circuit
// breaker is open, so a wedged upstream cannot translate one-for-one
// into client-visible latency.
//
// Background paths (e.g. the upload uploaders, admin endpoints, or any
// call where bazel itself is not waiting in real time) should NOT mark
// the context — they will then continue to use the more generous
// background timeouts that prefer correctness over latency.
func WithHotPath(ctx context.Context) context.Context {
	return context.WithValue(ctx, hotPathKey, true)
}

// IsHotPath reports whether ctx was marked via WithHotPath.
func IsHotPath(ctx context.Context) bool {
	v, _ := ctx.Value(hotPathKey).(bool)
	return v
}

type DigestFunction int

const (
	DigestFunctionSHA256 DigestFunction = iota
	DigestFunctionBLAKE3
)

func WithDigestFunction(ctx context.Context, df DigestFunction) context.Context {
	return context.WithValue(ctx, digestFunctionKey, df)
}

func DigestFunctionFromContext(ctx context.Context) DigestFunction {
	df, ok := ctx.Value(digestFunctionKey).(DigestFunction)
	if !ok {
		return DigestFunctionSHA256
	}
	return df
}

func NewHashForFunction(df DigestFunction) hash.Hash {
	if df == DigestFunctionBLAKE3 {
		return blake3.New()
	}
	return sha256.New()
}

// WithActionDigestSize attaches the original Action digest's SizeBytes to the
// context so that downstream proxy implementations can reconstruct a valid
// Digest for GetActionResult requests.
func WithActionDigestSize(ctx context.Context, size int64) context.Context {
	return context.WithValue(ctx, actionDigestSizeKey, size)
}

// ActionDigestSize retrieves the Action digest SizeBytes from the context.
func ActionDigestSize(ctx context.Context) (int64, bool) {
	size, ok := ctx.Value(actionDigestSizeKey).(int64)
	return size, ok
}

// EntryKind describes the kind of cache entry
type EntryKind int

const (
	// AC stands for Action Cache.
	AC EntryKind = iota

	// CAS stands for Content Addressable Storage.
	CAS

	// RAW cache items are not validated. Not exposed externally, only
	// used for HTTP when running with the --disable_http_ac_validation
	// commandline flag.
	RAW
)

func (e EntryKind) String() string {
	if e == AC {
		return "ac"
	}
	if e == CAS {
		return "cas"
	}
	return "raw"
}

func (e EntryKind) DirName() string {
	if e == AC {
		return "ac.v2"
	}
	if e == CAS {
		return "cas.v2"
	}
	return "raw.v2"
}

// Logger is designed to be satisfied by log.Logger.
type Logger interface {
	Printf(format string, v ...interface{})
}

// Error is used by Cache implementations to return a structured error.
type Error struct {
	// Corresponds to a http.Status* code
	Code int
	// A human-readable string describing the error
	Text string
}

func (e *Error) Error() string {
	return e.Text
}

// Proxy is the interface that (optional) proxy backends must implement.
// Implementations are expected to be safe for concurrent use.
type Proxy interface {

	// Put makes a reasonable effort to asynchronously upload the cache
	// item identified by `hash` with logical size `logicalSize` and
	// `sizeOnDisk` bytes on disk, whose data is readable from `rc` to
	// the proxy backend. The data available in `rc` is in the same
	// format as used by the disk.Cache instance.
	//
	// This is allowed to fail silently (for example when under heavy load).
	Put(ctx context.Context, kind EntryKind, hash string, logicalSize int64, sizeOnDisk int64, rc io.ReadCloser)

	// Get returns an io.ReadCloser from which the cache item identified by
	// `hash` can be read, its logical size, and an error if something went
	// wrong. The data available from `rc` is in the same format as used by
	// the disk.Cache instance.
	Get(ctx context.Context, kind EntryKind, hash string, size int64) (io.ReadCloser, int64, error)

	// Contains returns whether or not the cache item exists on the
	// remote end, and the size if it exists (and -1 if the size is
	// unknown).
	Contains(ctx context.Context, kind EntryKind, hash string, size int64) (bool, int64)

	// FindMissingCasBlobs returns the subset of `digests` that the proxy
	// backend reports as missing. Implementations whose backend cannot
	// answer this in a single round-trip must return
	// ErrProxyBatchNotImplemented so that the caller falls back to
	// per-blob Contains lookups.
	FindMissingCasBlobs(ctx context.Context, digests []Digest) ([]Digest, error)
}

// TransformActionCacheKey takes an ActionCache key and an instance name
// and returns a new ActionCache key to use instead. If the instance name
// is empty, then the original key is returned unchanged.
func TransformActionCacheKey(key, instance string, logger Logger) string {
	if instance == "" {
		return key
	}

	h := sha256.New()
	h.Write([]byte(key))
	h.Write([]byte(instance))
	b := h.Sum(nil)
	newKey := hex.EncodeToString(b[:])

	logger.Printf("REMAP AC HASH %s : %s => %s", key, instance, newKey)

	return newKey
}

func LookupKey(kind EntryKind, hash string) string {
	return kind.String() + "/" + hash
}
