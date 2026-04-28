//go:build linux

package casblob

import (
	"os"
	"sync/atomic"

	"golang.org/x/sys/unix"
)

// dropPageCacheOnClose is set once at startup by SetDropPageCacheOnClose.
// Reads happen in many goroutines on every cache hit, so it is stored
// atomically to avoid the data-race detector flagging unsynchronised access
// even though in practice the value never changes after init.
var dropPageCacheOnClose atomic.Bool

// SetDropPageCacheOnClose toggles whether dropPageCache issues a
// posix_fadvise(POSIX_FADV_DONTNEED) syscall before closing files that
// were just read for a cache hit. Intended to be called once at startup.
func SetDropPageCacheOnClose(enabled bool) {
	dropPageCacheOnClose.Store(enabled)
}

// dropPageCache hints to the kernel that the page-cache pages backing f
// can be dropped. Safe to call on a still-open file; the file descriptor
// must not have been closed yet, otherwise EBADF is returned (and
// silently ignored). Does nothing if SetDropPageCacheOnClose(true) was
// not called.
func dropPageCache(f *os.File) {
	if f == nil || !dropPageCacheOnClose.Load() {
		return
	}
	// offset=0, length=0 means "from offset 0 to end of file".
	// Errors are intentionally ignored: this is a best-effort hint and
	// the worst case is that the kernel keeps the pages a bit longer.
	_ = unix.Fadvise(int(f.Fd()), 0, 0, unix.FADV_DONTNEED)
}
