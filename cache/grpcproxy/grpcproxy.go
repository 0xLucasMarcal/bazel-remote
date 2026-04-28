package grpcproxy

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"
	"time"

	"github.com/buchgr/bazel-remote/v2/cache"
	"github.com/buchgr/bazel-remote/v2/utils/backendproxy"
	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"golang.org/x/sync/singleflight"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	asset "github.com/buchgr/bazel-remote/v2/genproto/build/bazel/remote/asset/v1"
	pb "github.com/buchgr/bazel-remote/v2/genproto/build/bazel/remote/execution/v2"
	bs "google.golang.org/genproto/googleapis/bytestream"
)

const (
	// The maximum chunk size to write back to the client in Send calls.
	// Inspired by Goma's FileBlob.FILE_CHUNK maxium size.
	maxChunkSize = 2 * 1024 * 1024 // 2M

	// Upper bounds for upstream RPCs. These prevent a slow or wedged
	// upstream proxy from indefinitely tying up upload workers and
	// FindMissingBlobs callers (which is the typical root cause of
	// client-side "uploading missing input" stalls observed via
	// goroutine dumps where every in-flight request is parked on
	// an upstream call).
	//
	// The unary timeouts are sized so a momentary upstream hiccup
	// fails fast rather than blocking a worker for minutes.
	upstreamFindMissingTimeout = 10 * time.Second
	upstreamContainsTimeout    = 10 * time.Second
	upstreamUpdateACTimeout    = 30 * time.Second
)

// upstreamWriteIdleTimeout bounds how long a CAS upload to the upstream
// may make no forward progress before we give up. We deliberately do not
// impose an end-to-end deadline on the Write stream: the upstream is a
// shared, throttled resource (observed as low as ~1 Mbps under fleet
// pressure), and a fixed total deadline would kill legitimate large-blob
// uploads and amplify load via retries. An idle timeout instead
// distinguishes "wedged" from "slow but progressing" — every successful
// Send resets the timer, so a 1 Mbps trickle survives, but a stream that
// stops accepting bytes is aborted within this window.
//
// Declared as a var (not a const) so tests can shrink it without
// extending their wall-clock runtime to 60s+.
var upstreamWriteIdleTimeout = 60 * time.Second

// uploadsDropped counts asynchronous upstream uploads that were dropped
// because the upload queue was full at Put-time. A non-zero rate is the
// canonical signal that the upstream proxy cannot keep up with the
// local cache's write rate (e.g. a multi-instance fleet sharing one
// throttled upstream): the blob is still served from the local cache,
// but it will not be re-pushed to the upstream by this Put. Use the
// per-kind label to distinguish CAS uploads (the typical bottleneck)
// from AC/RAW.
var uploadsDropped = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "bazel_remote_grpc_proxy_uploads_dropped_total",
	Help: "Number of upstream uploads dropped because the grpc proxy upload queue was full at the time of Put. A non-zero rate indicates the upstream cannot keep up with the local cache's write rate.",
}, []string{"kind"})

type GrpcClients struct {
	asset asset.FetchClient
	bs    bs.ByteStreamClient
	ac    pb.ActionCacheClient
	cas   pb.ContentAddressableStorageClient
	cap   pb.CapabilitiesClient
}

func contains[A comparable](arr []A, value A) bool {
	for _, v := range arr {
		if v == value {
			return true
		}
	}
	return false
}

func NewGrpcClients(cc *grpc.ClientConn) *GrpcClients {
	return &GrpcClients{
		asset: asset.NewFetchClient(cc),
		bs:    bs.NewByteStreamClient(cc),
		ac:    pb.NewActionCacheClient(cc),
		cas:   pb.NewContentAddressableStorageClient(cc),
		cap:   pb.NewCapabilitiesClient(cc),
	}
}

func (c *GrpcClients) CheckCapabilities(zstd bool) error {
	resp, err := c.cap.GetCapabilities(context.Background(), &pb.GetCapabilitiesRequest{})
	if err != nil {
		return err
	}
	if !resp.CacheCapabilities.ActionCacheUpdateCapabilities.UpdateEnabled {
		return errors.New("proxy backend does not allow action cache updates")
	}
	if !contains(resp.CacheCapabilities.DigestFunctions, pb.DigestFunction_SHA256) {
		return errors.New("proxy backend does not support sha256")
	}
	if zstd && !contains(resp.CacheCapabilities.SupportedCompressors, pb.Compressor_ZSTD) {
		return errors.New("compression required but the grpc proxy does not support it")
	}
	return nil
}

type remoteGrpcProxyCache struct {
	clients      *GrpcClients
	uploadQueue  chan<- backendproxy.UploadReq
	accessLogger cache.Logger
	errorLogger  cache.Logger
	v2mode       bool

	// findMissingSF collapses concurrent FindMissingCasBlobs requests
	// for an identical digest set into a single in-flight upstream RPC.
	// This is most useful when retries from the same client (or
	// overlapping clients) cause duplicate fan-out under a slow
	// upstream.
	findMissingSF singleflight.Group
}

func digestFunctionProto(df cache.DigestFunction) pb.DigestFunction_Value {
	if df == cache.DigestFunctionBLAKE3 {
		return pb.DigestFunction_BLAKE3
	}
	return pb.DigestFunction_SHA256
}

func digestFunctionSegment(df cache.DigestFunction) string {
	if df == cache.DigestFunctionBLAKE3 {
		return "blake3/"
	}
	return ""
}

func New(clients *GrpcClients, storageMode string,
	accessLogger cache.Logger, errorLogger cache.Logger,
	numUploaders, maxQueuedUploads int) cache.Proxy {

	proxy := &remoteGrpcProxyCache{
		clients:      clients,
		accessLogger: accessLogger,
		errorLogger:  errorLogger,
		v2mode:       storageMode == "zstd",
	}

	proxy.uploadQueue = backendproxy.StartUploaders(proxy, numUploaders, maxQueuedUploads)

	return proxy
}

// Helper function for logging responses
func logResponse(logger cache.Logger, method string, msg string, kind cache.EntryKind, hash string) {
	logger.Printf("GRPC PROXY %s %s %s: %s", strings.ToUpper(method), strings.ToUpper(kind.String()), hash, msg)
}

func (r *remoteGrpcProxyCache) UploadFile(item backendproxy.UploadReq) {
	defer func() { _ = item.Rc.Close() }()

	switch item.Kind {
	case cache.RAW:
		// RAW cache entries are a special case of AC, used when --disable_http_ac_validation
		// is enabled. We can treat them as AC in this scope
		fallthrough
	case cache.AC:
		data := make([]byte, item.SizeOnDisk)
		read := int64(0)
		for {
			n, err := item.Rc.Read(data[read:])
			if n > 0 {
				read += int64(n)
			}
			if err == io.EOF || read == item.SizeOnDisk {
				break
			}
		}
		if read != item.SizeOnDisk {
			logResponse(r.errorLogger, "Update", "Unexpected short read", item.Kind, item.Hash)
			return
		}
		ar := &pb.ActionResult{}
		err := proto.Unmarshal(data, ar)
		if err != nil {
			logResponse(r.errorLogger, "Update", err.Error(), item.Kind, item.Hash)
			return
		}
		digest := &pb.Digest{
			Hash:      item.Hash,
			SizeBytes: item.LogicalSize,
		}

		req := &pb.UpdateActionResultRequest{
			ActionDigest:   digest,
			ActionResult:   ar,
			DigestFunction: digestFunctionProto(item.DigestFunction),
		}
		updateCtx, updateCancel := context.WithTimeout(context.Background(), upstreamUpdateACTimeout)
		defer updateCancel()
		_, err = r.clients.ac.UpdateActionResult(updateCtx, req)
		if err != nil {
			logResponse(r.errorLogger, "Update", err.Error(), item.Kind, item.Hash)
		}
		return
	case cache.CAS:
		writeCtx, writeCancel := context.WithCancel(context.Background())
		defer writeCancel()
		// Idle watchdog: cancel the stream if no successful Send
		// (or CloseAndRecv) happens within upstreamWriteIdleTimeout.
		// Reset on every observed forward progress below.
		idleTimer := time.AfterFunc(upstreamWriteIdleTimeout, writeCancel)
		defer idleTimer.Stop()

		stream, err := r.clients.bs.Write(writeCtx)
		if err != nil {
			logResponse(r.errorLogger, "Write", err.Error(), item.Kind, item.Hash)
			return
		}

		bufSize := item.SizeOnDisk
		if bufSize > maxChunkSize {
			bufSize = maxChunkSize
		}
		buf := make([]byte, bufSize)

		dfSeg := digestFunctionSegment(item.DigestFunction)
		template := "uploads/%s/blobs/%s%s/%d"
		if r.v2mode {
			template = "uploads/%s/compressed-blobs/zstd/%s%s/%d"
		}
		resourceName := fmt.Sprintf(template, uuid.New().String(), dfSeg, item.Hash, item.LogicalSize)

		firstIteration := true
		var writeOffset int64
		for {
			n, readErr := item.Rc.Read(buf)
			if readErr != nil && readErr != io.EOF {
				logResponse(r.errorLogger, "Write", readErr.Error(), item.Kind, item.Hash)
				err := stream.CloseSend()
				if err != nil {
					logResponse(r.errorLogger, "Write", err.Error(), item.Kind, item.Hash)
				}
				return
			}
			finished := readErr == io.EOF

			if n > 0 || finished {
				rn := ""
				if firstIteration {
					firstIteration = false
					rn = resourceName
				}
				req := &bs.WriteRequest{
					ResourceName: rn,
					WriteOffset:  writeOffset,
					FinishWrite:  finished,
				}
				if n > 0 {
					req.Data = buf[:n]
					writeOffset += int64(n)
				}
				err := stream.Send(req)
				if err == io.EOF {
					// Server closed the stream early. The real error (or a
					// short-circuit success for a blob that already exists)
					// is in CloseAndRecv.
					idleTimer.Reset(upstreamWriteIdleTimeout)
					_, recvErr := stream.CloseAndRecv()
					if recvErr != nil {
						logResponse(r.errorLogger, "Write", recvErr.Error(), item.Kind, item.Hash)
					}
					return
				}
				if err != nil {
					logResponse(r.errorLogger, "Write", err.Error(), item.Kind, item.Hash)
					return
				}
				// Successful Send is the proof of forward progress
				// against the upstream; refresh the idle deadline.
				idleTimer.Reset(upstreamWriteIdleTimeout)
			}

			if finished {
				_, err = stream.CloseAndRecv()
				if err != nil {
					logResponse(r.errorLogger, "Write", err.Error(), item.Kind, item.Hash)
					return
				}
				return
			}
		}
	default:
		logResponse(r.errorLogger, "Write", "Unexpected kind", item.Kind, item.Hash)
		return
	}
}

func (r *remoteGrpcProxyCache) Put(ctx context.Context, kind cache.EntryKind, hash string, logicalSize int64, sizeOnDisk int64, rc io.ReadCloser) {
	if r.uploadQueue == nil {
		_ = rc.Close()
		return
	}

	item := backendproxy.UploadReq{
		Hash:           hash,
		LogicalSize:    logicalSize,
		SizeOnDisk:     sizeOnDisk,
		Kind:           kind,
		Rc:             rc,
		DigestFunction: cache.DigestFunctionFromContext(ctx),
	}

	select {
	case r.uploadQueue <- item:
	default:
		uploadsDropped.WithLabelValues(kind.String()).Inc()
		r.errorLogger.Printf("GRPC PROXY: dropped upload %s %s (size=%d): upload queue full (capacity=%d) - upstream is not keeping up",
			strings.ToUpper(kind.String()), hash, logicalSize, cap(r.uploadQueue))
		_ = rc.Close()
	}
}

func (r *remoteGrpcProxyCache) fetchBlobDigest(ctx context.Context, hash string) (*pb.Digest, error) {
	decoded, err := hex.DecodeString(hash)
	if err != nil {
		return nil, err
	}
	q := asset.Qualifier{
		Name:  "checksum.sri",
		Value: fmt.Sprintf("sha256-%s", base64.StdEncoding.EncodeToString(decoded)),
	}
	freq := asset.FetchBlobRequest{
		Uris:       []string{},
		Qualifiers: []*asset.Qualifier{&q},
	}

	res, err := r.clients.asset.FetchBlob(ctx, &freq)
	if err != nil {
		return nil, err
	}

	if res.Status.GetCode() == int32(codes.NotFound) {
		return nil, errors.New("not found")
	}
	if res.Status.GetCode() != int32(codes.OK) {
		return nil, errors.New(res.Status.Message)
	}
	return res.BlobDigest, nil
}

func (r *remoteGrpcProxyCache) Get(ctx context.Context, kind cache.EntryKind, hash string, size int64) (io.ReadCloser, int64, error) {
	switch kind {
	case cache.RAW:
		// RAW cache entries are a special case of AC, used when --disable_http_ac_validation
		// is enabled. We can treat them as AC in this scope
		fallthrough
	case cache.AC:
		digestSize := size
		if actionDigestSize, ok := cache.ActionDigestSize(ctx); ok && actionDigestSize > 0 {
			digestSize = actionDigestSize
		} else if digestSize < 0 {
			digestSize = 0
		}
		digest := pb.Digest{
			Hash:      hash,
			SizeBytes: digestSize,
		}

		req := &pb.GetActionResultRequest{
			ActionDigest:   &digest,
			DigestFunction: digestFunctionProto(cache.DigestFunctionFromContext(ctx)),
		}

		res, err := r.clients.ac.GetActionResult(ctx, req)
		status, ok := status.FromError(err)
		if ok && status.Code() == codes.NotFound {
			return nil, -1, nil
		}

		if err != nil {
			logResponse(r.errorLogger, "Get", err.Error(), kind, hash)
			return nil, -1, err
		}
		data, err := proto.Marshal(res)
		if err != nil {
			logResponse(r.errorLogger, "Get", err.Error(), kind, hash)
			return nil, -1, err
		}

		return io.NopCloser(bytes.NewReader(data)), int64(len(data)), nil

	case cache.CAS:
		if size < 0 {
			// We don't know the size, so send a FetchBlob request first to get the digest
			digest, err := r.fetchBlobDigest(ctx, hash)
			if err != nil {
				logResponse(r.errorLogger, "Fetch", err.Error(), kind, hash)
				return nil, -1, err
			}
			size = digest.SizeBytes
		}

		dfSeg := digestFunctionSegment(cache.DigestFunctionFromContext(ctx))
		template := "blobs/%s%s/%d"
		if r.v2mode {
			template = "compressed-blobs/zstd/%s%s/%d"
		}
		req := bs.ReadRequest{
			ResourceName: fmt.Sprintf(template, dfSeg, hash, size),
		}
		stream, err := r.clients.bs.Read(ctx, &req)
		if err != nil {
			logResponse(r.errorLogger, "Read", err.Error(), kind, hash)
			return nil, -1, err
		}
		rc := StreamReadCloser[*bs.ReadResponse]{Stream: stream}
		return &rc, size, nil
	default:
		return nil, -1, fmt.Errorf("unexpected kind %s", kind)
	}
}

func (r *remoteGrpcProxyCache) Contains(ctx context.Context, kind cache.EntryKind, hash string, size int64) (bool, int64) {
	switch kind {
	case cache.RAW:
		// RAW cache entries are a special case of AC, used when --disable_http_ac_validation
		// is enabled. We can treat them as AC in this scope
		fallthrough
	case cache.AC:
		// There's not "contains" method for the action cache so the best we can do
		// is to get the object and discard the result
		// We don't expect this to ever be called anyways since it is not part of the grpc protocol
		rc, size, err := r.Get(ctx, kind, hash, size)
		_ = rc.Close()
		if err != nil || size < 0 {
			return false, -1
		}
		return true, size
	case cache.CAS:
		if size < 0 {
			// If don't know the size, use the remote asset api to find the missing blob
			digest, err := r.fetchBlobDigest(ctx, hash)
			if err != nil {
				logResponse(r.errorLogger, "Contains", err.Error(), kind, hash)
				return false, -1
			}
			return true, digest.SizeBytes
		}

		req := &pb.FindMissingBlobsRequest{
			DigestFunction: digestFunctionProto(cache.DigestFunctionFromContext(ctx)),
			BlobDigests: []*pb.Digest{{
				Hash:      hash,
				SizeBytes: size,
			}},
		}
		containsCtx, cancel := context.WithTimeout(ctx, upstreamContainsTimeout)
		defer cancel()
		res, err := r.clients.cas.FindMissingBlobs(containsCtx, req)
		if err != nil {
			logResponse(r.errorLogger, "Contains", err.Error(), kind, hash)
			return false, -1
		}
		for range res.MissingBlobDigests {
			return false, -1
		}
		return true, size
	default:
		logResponse(r.errorLogger, "Contains", "Unexpected kind", kind, hash)
		return false, -1
	}
}

// FindMissingCasBlobs asks the upstream CAS in a single FindMissingBlobs RPC
// which of the supplied digests it does not have. This collapses what would
// otherwise be one RPC per digest into one RPC per call, which is the main
// fix for findMissingCasBlobsInternal's per-blob fan-out.
//
// The upstream call is bounded by upstreamFindMissingTimeout so that a slow
// or wedged upstream cannot indefinitely tie up callers. Concurrent calls
// for an identical digest set are collapsed via singleflight to avoid
// amplifying load on an already-struggling upstream.
func (r *remoteGrpcProxyCache) FindMissingCasBlobs(ctx context.Context, digests []cache.Digest) ([]cache.Digest, error) {
	if len(digests) == 0 {
		return nil, nil
	}

	df := digestFunctionProto(cache.DigestFunctionFromContext(ctx))
	key := findMissingSingleflightKey(df, digests)

	v, err, _ := r.findMissingSF.Do(key, func() (any, error) {
		return r.findMissingCasBlobsUpstream(ctx, df, digests)
	})
	if err != nil {
		return nil, err
	}
	return v.([]cache.Digest), nil
}

func (r *remoteGrpcProxyCache) findMissingCasBlobsUpstream(ctx context.Context, df pb.DigestFunction_Value, digests []cache.Digest) ([]cache.Digest, error) {
	pbDigests := make([]*pb.Digest, 0, len(digests))
	for _, d := range digests {
		pbDigests = append(pbDigests, &pb.Digest{
			Hash:      d.Hash,
			SizeBytes: d.SizeBytes,
		})
	}

	req := &pb.FindMissingBlobsRequest{
		DigestFunction: df,
		BlobDigests:    pbDigests,
	}

	fmbCtx, cancel := context.WithTimeout(ctx, upstreamFindMissingTimeout)
	defer cancel()
	res, err := r.clients.cas.FindMissingBlobs(fmbCtx, req)
	if err != nil {
		logResponse(r.errorLogger, "FindMissingCasBlobs", err.Error(), cache.CAS, "")
		return nil, err
	}

	if len(res.MissingBlobDigests) == 0 {
		return nil, nil
	}

	missing := make([]cache.Digest, 0, len(res.MissingBlobDigests))
	for _, d := range res.MissingBlobDigests {
		missing = append(missing, cache.Digest{
			Hash:      d.Hash,
			SizeBytes: d.SizeBytes,
		})
	}
	return missing, nil
}

// findMissingSingleflightKey returns a stable, deterministic key for a
// (digestFunction, digestSet) pair. The digest order is normalised so
// that callers asking the same question in different orders share one
// upstream RPC.
func findMissingSingleflightKey(df pb.DigestFunction_Value, digests []cache.Digest) string {
	sorted := make([]cache.Digest, len(digests))
	copy(sorted, digests)
	sort.Slice(sorted, func(i, j int) bool {
		if sorted[i].Hash != sorted[j].Hash {
			return sorted[i].Hash < sorted[j].Hash
		}
		return sorted[i].SizeBytes < sorted[j].SizeBytes
	})

	h := sha256.New()
	var sizeBuf [8]byte
	binary.LittleEndian.PutUint32(sizeBuf[:4], uint32(df))
	_, _ = h.Write(sizeBuf[:4])
	for _, d := range sorted {
		_, _ = h.Write([]byte(d.Hash))
		binary.LittleEndian.PutUint64(sizeBuf[:], uint64(d.SizeBytes))
		_, _ = h.Write(sizeBuf[:])
	}
	return string(h.Sum(nil))
}
