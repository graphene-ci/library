package host

import (
	"fmt"
	"io/fs"

	"golang.org/x/sys/unix"
)

// setAccess sets mode and owner of the directory at name beneath root. The
// kernel resolves the path (openat2): no component may be a symlink and the
// path may not leave root, and O_DIRECTORY refuses a planted FIFO or device
// before any open side effect. Mode and owner change through that
// descriptor, so what was resolved is what changes. MkdirAll's mode passes
// through the umask; the spec's is exact.
func setAccess(root, name string, mode fs.FileMode, uid, gid int) error {
	rootFd, err := unix.Open(root, unix.O_PATH|unix.O_DIRECTORY|unix.O_CLOEXEC, 0)
	if err != nil {
		return fmt.Errorf("open machine root: %w", err)
	}
	defer func() { _ = unix.Close(rootFd) }()
	fd, err := unix.Openat2(rootFd, name, &unix.OpenHow{
		Flags:   unix.O_RDONLY | unix.O_DIRECTORY | unix.O_NOFOLLOW | unix.O_NONBLOCK | unix.O_CLOEXEC,
		Resolve: unix.RESOLVE_BENEATH | unix.RESOLVE_NO_SYMLINKS,
	})
	if err != nil {
		return fmt.Errorf("dir %s is not the directory the record created: %w", name, err)
	}
	defer func() { _ = unix.Close(fd) }()
	if err := unix.Fchmod(fd, uint32(mode.Perm())); err != nil {
		return fmt.Errorf("chmod %s: %w", name, err)
	}
	if err := unix.Fchown(fd, uid, gid); err != nil {
		return fmt.Errorf("chown %s: %w", name, err)
	}
	return nil
}
