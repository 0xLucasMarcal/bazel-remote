package disk

import (
	"fmt"
	"log"

	"github.com/buchgr/bazel-remote/v2/cache"
	"github.com/buchgr/bazel-remote/v2/cache/disk/casblob"
	"github.com/buchgr/bazel-remote/v2/cache/disk/zstdimpl"

	"github.com/prometheus/client_golang/prometheus"
)

type Option func(*CacheConfig) error

type CacheConfig struct {
	diskCache        *diskCache        // Assumed to be non-nil.
	metrics          *metricsDecorator // May be nil.
	maxSizeHardLimit int64
}

func WithStorageMode(mode string) Option {
	return func(c *CacheConfig) error {
		switch mode {
		case "zstd":
			c.diskCache.storageMode = casblob.Zstandard
			return nil
		case "uncompressed":
			c.diskCache.storageMode = casblob.Identity
			return nil
		default:
			return fmt.Errorf("unsupported storage mode: %s", mode)
		}
	}
}

func WithZstdImplementation(impl string) Option {
	return func(c *CacheConfig) error {
		var err error
		c.diskCache.zstd, err = zstdimpl.Get(impl)
		return err
	}
}

func WithMaxBlobSize(size int64) Option {
	return func(c *CacheConfig) error {
		if size <= 0 {
			return fmt.Errorf("invalid MaxBlobSize: %d", size)
		}

		c.diskCache.maxBlobSize = size
		return nil
	}
}

func WithProxyBackend(proxy cache.Proxy) Option {
	return func(c *CacheConfig) error {
		if c.diskCache.proxy != nil && proxy != nil {
			return fmt.Errorf("proxy backends may be set only once")
		}

		if proxy != nil {
			c.diskCache.proxy = proxy
			c.diskCache.spawnContainsQueueWorkers()
		}

		return nil
	}
}

// WithProxyContainsQueue overrides the size of the per-blob proxy.Contains
// queue and the number of worker goroutines that drain it. Both values
// must be > 0; zero/negative values keep the defaults
// (defaultContainsQueueSize / defaultContainsWorkers).
//
// This only affects proxies whose FindMissingCasBlobs returns
// ErrProxyBatchNotImplemented (HTTP/S3/GCS/AZBlob); the gRPC proxy
// answers FindMissingBlobs in a single batch RPC and never queues here.
//
// Must be set before WithProxyBackend, since WithProxyBackend spawns the
// workers immediately.
func WithProxyContainsQueue(queueSize, workers int) Option {
	return func(c *CacheConfig) error {
		if queueSize < 0 || workers < 0 {
			return fmt.Errorf("invalid proxy contains queue config: queueSize=%d workers=%d", queueSize, workers)
		}
		c.diskCache.containsQueueSize = queueSize
		c.diskCache.containsWorkers = workers
		return nil
	}
}

func WithProxyMaxBlobSize(maxProxyBlobSize int64) Option {
	return func(c *CacheConfig) error {
		if maxProxyBlobSize <= 0 {
			return fmt.Errorf("invalid MaxProxyBlobSize: %d", maxProxyBlobSize)
		}

		c.diskCache.maxProxyBlobSize = maxProxyBlobSize
		return nil
	}
}

func WithAccessLogger(logger *log.Logger) Option {
	return func(c *CacheConfig) error {
		c.diskCache.accessLogger = logger
		return nil
	}
}

func WithEndpointMetrics() Option {
	return func(c *CacheConfig) error {
		if c.metrics != nil {
			return fmt.Errorf("WithEndpointMetrics specified multiple times")
		}

		c.metrics = &metricsDecorator{
			counter: prometheus.NewCounterVec(prometheus.CounterOpts{
				Name: "bazel_remote_incoming_requests_total",
				Help: "The number of incoming cache requests",
			},
				[]string{"method", "kind", "status"}),
		}

		c.metrics.counter.WithLabelValues("get", "cas", "hit").Add(0)
		c.metrics.counter.WithLabelValues("get", "cas", "miss").Add(0)
		c.metrics.counter.WithLabelValues("contains", "cas", "hit").Add(0)
		c.metrics.counter.WithLabelValues("contains", "cas", "miss").Add(0)
		c.metrics.counter.WithLabelValues("get", "ac", "hit").Add(0)
		c.metrics.counter.WithLabelValues("get", "ac", "miss").Add(0)

		return nil
	}
}

func WithMaxSizeHardLimit(maxSizeHardLimit int64) Option {
	return func(cc *CacheConfig) error {
		cc.maxSizeHardLimit = maxSizeHardLimit
		return nil
	}
}

// WithDropPageCacheAfterRead controls whether bazel-remote asks the
// kernel to drop the page-cache pages backing each cache blob right
// after the blob has been streamed to the client (via
// posix_fadvise(POSIX_FADV_DONTNEED)). Useful when the process runs
// under a cgroup memory limit (e.g. Kubernetes), where page cache is
// counted toward container_memory_working_set_bytes and can drive OOM
// kills even though the Go heap is small.
//
// Trade-off: dropping pages defeats the page cache for repeated reads
// of the same blob, so latency on hot blobs may increase if the
// underlying storage is slow. Enable when bound by container memory,
// leave off when bound by read latency.
//
// No-op on non-Linux platforms.
func WithDropPageCacheAfterRead(enabled bool) Option {
	return func(c *CacheConfig) error {
		casblob.SetDropPageCacheOnClose(enabled)
		return nil
	}
}
