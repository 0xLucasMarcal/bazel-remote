//go:build !linux

package casblob

import "os"

// SetDropPageCacheOnClose is a no-op on non-Linux platforms because
// posix_fadvise is Linux-specific. Accepting the call keeps the option
// usable from cross-platform code paths without #ifdefs at the call
// site.
func SetDropPageCacheOnClose(_ bool) {}

func dropPageCache(_ *os.File) {}
