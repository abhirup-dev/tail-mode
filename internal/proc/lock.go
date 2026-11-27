package proc

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/gofrs/flock"
)

// Lock takes an exclusive lock on path (creating it and its dir), retrying for up to 2 s.
// It fails if another process keeps holding it. The fd is close-on-exec, so children never inherit
// the lock; it is released by release() or when this process dies.
func Lock(path string) (release func(), err error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return nil, err
	}
	f := flock.New(path)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	ok, err := f.TryLockContext(ctx, 50*time.Millisecond)
	if !ok {
		if err == nil || ctx.Err() != nil {
			err = fmt.Errorf("lock %s is held by another process", path)
		}
		return nil, err
	}
	return func() { _ = f.Close() }, nil
}

// Locked reports whether another process holds the exclusive lock on path. It probes with a shared
// lock, so concurrent probes do not see each other, and never creates the file.
func Locked(path string) bool {
	f := flock.New(path, flock.SetFlag(os.O_RDONLY))
	ok, err := f.TryRLock()
	if err != nil { // missing file or dir: nobody ever locked it
		return false
	}
	if ok {
		_ = f.Close()
	}
	return !ok
}
