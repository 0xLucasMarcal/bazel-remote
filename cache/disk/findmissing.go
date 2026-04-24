package disk

import (
	"context"
	"errors"
	"sync"

	"github.com/buchgr/bazel-remote/v2/cache"

	pb "github.com/buchgr/bazel-remote/v2/genproto/build/bazel/remote/execution/v2"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type proxyCheck struct {
	wg          *sync.WaitGroup
	digest      **pb.Digest
	ctx         context.Context
	onProxyMiss func()
}

var errMissingBlob = errors.New("a blob could not be found")

var errRequestCancelled = status.Error(codes.Canceled, "Request was cancelled")

// Optimised implementation of FindMissingBlobs, which batches local index
// lookups and performs concurrent proxy lookups for local cache misses.
// Returns a slice with the blobs that are missing from the cache.
//
// Note that this modifies the input slice and returns a subset of it.
func (c *diskCache) FindMissingCasBlobs(ctx context.Context, blobs []*pb.Digest) ([]*pb.Digest, error) {
	err := c.findMissingCasBlobsInternal(ctx, blobs, false)
	if err != nil {
		return nil, err
	}
	return filterNonNil(blobs), nil
}

// Identifies local and proxy cache misses for blobs. Modifies the input `blobs` slice such that found
// blobs are replaced with nil, while the missing digests remain unchanged.
//
// When failFast is true and a blob could not be found in the local cache nor in the
// proxy back end, the search will immediately terminate and errMissingBlob will be returned. Given that the
// search is terminated early, the contents of blobs will only have partially been updated.
func (c *diskCache) findMissingCasBlobsInternal(ctx context.Context, blobs []*pb.Digest, failFast bool) error {
	// batchSize moderates how long the cache lock is held by findMissingLocalCAS.
	const batchSize = 20

	var cancelContextForFailFast context.CancelFunc = nil
	cancelledDueToFailFast := false

	if failFast && c.proxy != nil {
		var cancel context.CancelFunc
		ctx, cancel = context.WithCancel(ctx)
		defer cancel()

		cancelContextForFailFast = func() {
			// Indicate that we were canceled so that we can fail fast.
			cancelledDueToFailFast = true
			cancel()
		}
	}

	var wg sync.WaitGroup

	var chunk []*pb.Digest
	remaining := blobs

	for len(remaining) > 0 {
		select {
		case <-ctx.Done():
			if cancelledDueToFailFast {
				return errMissingBlob
			}
			return errRequestCancelled
		default:
		}

		if len(remaining) <= batchSize {
			chunk = remaining
			remaining = nil
		} else {
			chunk = remaining[:batchSize]
			remaining = remaining[batchSize:]
		}

		numMissing := c.findMissingLocalCAS(ctx, chunk)
		if numMissing == 0 {
			continue
		}

		if c.proxy == nil && failFast {
			// There's no proxy, there are missing blobs from the local cache, and we are failing fast.
			return errMissingBlob
		}

		if c.proxy != nil {
			// Build the list of digests in this chunk that the proxy
			// should be asked about (i.e. local-misses small enough to
			// fit under maxProxyBlobSize).
			var (
				batch    []cache.Digest
				batchIdx []int // index into chunk for each batch entry
			)
			for i := range chunk {
				if chunk[i] == nil {
					continue
				}

				if chunk[i].SizeBytes > c.maxProxyBlobSize {
					// The blob would exceed the limit, so skip it.
					if failFast {
						return errMissingBlob
					}
					continue
				}

				batch = append(batch, cache.Digest{
					Hash:      chunk[i].Hash,
					SizeBytes: chunk[i].SizeBytes,
				})
				batchIdx = append(batchIdx, i)
			}

			if len(batch) > 0 {
				// Try the single-RPC batch path first. For the gRPC
				// proxy this collapses N per-blob FindMissingBlobs RPCs
				// into one. For proxies without native batch support
				// (HTTP, S3, GCS, AZBlob) FindMissingCasBlobs returns
				// ErrProxyBatchNotImplemented and we fall back to the
				// per-blob containsQueue worker pool below.
				missingFromProxy, err := c.proxy.FindMissingCasBlobs(ctx, batch)
				switch {
				case err == nil:
					stillMissing := make(map[string]struct{}, len(missingFromProxy))
					for _, d := range missingFromProxy {
						stillMissing[d.Hash] = struct{}{}
					}
					for _, i := range batchIdx {
						if _, missing := stillMissing[chunk[i].Hash]; missing {
							c.accessLogger.Printf("GRPC CAS HEAD %s NOT FOUND", chunk[i].Hash)
							if failFast {
								return errMissingBlob
							}
						} else {
							c.accessLogger.Printf("GRPC CAS HEAD %s OK", chunk[i].Hash)
							chunk[i] = nil
						}
					}
				case errors.Is(err, cache.ErrProxyBatchNotImplemented):
					// Per-blob containsQueue fallback for proxies that
					// don't support batch FindMissingBlobs.
					for _, i := range batchIdx {
						// Adding to the containsQueue channel may have blocked
						// on a previous iteration, so check to see if the
						// context has cancelled.
						select {
						case <-ctx.Done():
							if cancelledDueToFailFast {
								return errMissingBlob
							}
							return errRequestCancelled
						default:
						}

						wg.Add(1)
						c.containsQueue <- proxyCheck{
							wg:     &wg,
							digest: &chunk[i],
							ctx:    ctx,
							// When failFast is true, onProxyMiss will have been set to a function that
							// will cancel the context, causing the remaining proxyChecks to short-circuit.
							onProxyMiss: cancelContextForFailFast,
						}
					}
				default:
					// The proxy returned a real error (network, timeout,
					// etc). Match the per-blob path's behaviour where
					// r.proxy.Contains errors degrade to "treated as
					// missing" (the underlying boolean is false). Log
					// each digest as NOT FOUND and let failFast trigger
					// if the caller wants strict semantics.
					for _, i := range batchIdx {
						c.accessLogger.Printf("GRPC CAS HEAD %s NOT FOUND (proxy batch error: %v)", chunk[i].Hash, err)
					}
					if failFast {
						return errMissingBlob
					}
				}
			}
		}
	}

	if c.proxy != nil {
		// Adapt the waitgroup for select
		waitCh := make(chan struct{})
		go func() {
			wg.Wait()
			close(waitCh)
		}()

		// Wait for all proxyChecks to finish or a context cancellation.
		select {
		case <-ctx.Done():
			if cancelledDueToFailFast {
				return errMissingBlob
			}
			return errRequestCancelled
		case <-waitCh: // Everything in the waitgroup has finished.
		}
	}

	return nil
}

// Move all the non-nil items in the input slice to the
// start, and return the non-nil sub-slice.
func filterNonNil(blobs []*pb.Digest) []*pb.Digest {
	count := 0
	for i := 0; i < len(blobs); i++ {
		if blobs[i] != nil {
			blobs[count] = blobs[i]
			count++
		}
	}

	return blobs[:count]
}

// Set blobs that exist in the disk cache to nil, and return the number
// of missing blobs.
//
// The access log is intentionally written AFTER c.mu is released so that
// access-log I/O latency cannot stall every other RPC waiting on the
// global cache mutex. The lock-held section now does only in-memory work.
func (c *diskCache) findMissingLocalCAS(ctx context.Context, blobs []*pb.Digest) int {
	var key string
	missing := 0

	// Pre-allocate to avoid growing the slice while the lock is held.
	hits := make([]string, 0, len(blobs))

	df := cache.DigestFunctionFromContext(ctx)
	emptyHash := cache.EmptyDigestHash(df)

	c.mu.Lock()

	for i := range blobs {
		if blobs[i].SizeBytes == 0 && blobs[i].Hash == emptyHash {
			hits = append(hits, blobs[i].Hash)
			blobs[i] = nil
			continue
		}

		foundSize := int64(-1)
		key = cache.LookupKey(cache.CAS, df, blobs[i].Hash)
		item, listElem := c.lru.Get(key)
		if listElem != nil {
			foundSize = item.size
		}

		if listElem != nil && !isSizeMismatch(blobs[i].SizeBytes, foundSize) {
			hits = append(hits, blobs[i].Hash)
			blobs[i] = nil
		} else {
			missing++
		}
	}

	c.mu.Unlock()

	for _, h := range hits {
		c.accessLogger.Printf("GRPC CAS HEAD %s OK", h)
	}

	return missing
}

func (c *diskCache) containsWorker() {
	var ok bool
	for req := range c.containsQueue {
		if req.ctx != nil {
			select {
			case <-req.ctx.Done():
				// Fast-fail if the context has already been cancelled.
				c.accessLogger.Printf("GRPC CAS HEAD %s CANCELLED", (*req.digest).Hash)
				req.wg.Done()
				continue
			default:
			}
		}

		ok, _ = c.proxy.Contains(req.ctx, cache.CAS, (*req.digest).Hash, (*req.digest).SizeBytes)
		if ok {
			c.accessLogger.Printf("GRPC CAS HEAD %s OK", (*req.digest).Hash)
			// The blob exists on the proxy, remove it from the
			// list of missing blobs.
			*(req.digest) = nil
		} else {
			c.accessLogger.Printf("GRPC CAS HEAD %s NOT FOUND", (*req.digest).Hash)
			if req.onProxyMiss != nil {
				req.onProxyMiss()
			}
		}
		req.wg.Done()
	}
}

// Default sizes for the per-blob proxy.Contains worker pool. These are
// only relevant for proxies that do NOT implement FindMissingCasBlobs;
// the gRPC proxy collapses the entire fan-out into one batch RPC and
// therefore never enqueues to containsQueue.
const (
	defaultContainsQueueSize = 2048
	defaultContainsWorkers   = 512
)

func (c *diskCache) spawnContainsQueueWorkers() {
	queueSize := c.containsQueueSize
	if queueSize <= 0 {
		queueSize = defaultContainsQueueSize
	}
	numWorkers := c.containsWorkers
	if numWorkers <= 0 {
		numWorkers = defaultContainsWorkers
	}

	c.containsQueue = make(chan proxyCheck, queueSize)
	for i := 0; i < numWorkers; i++ {
		go c.containsWorker()
	}
}
