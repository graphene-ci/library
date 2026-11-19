package host

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path"
	"time"

	"github.com/graphene-ci/library/host/internal/contract"
)

// DirInfo is the output of a Dir resource.
type DirInfo struct {
	Path string `json:"path"`
}

type dirSpec = contract.DirSpec

// trashDir collects a removed directory next to where it lived: a rename
// inside one filesystem is atomic and instant however large the tree is,
// so the record lets go at once and the bytes are reclaimed after.
const trashDir = ".graphene-trash"

const defaultDirMode = 0o750

func validateDir(spec dirSpec) error {
	if !path.IsAbs(spec.Path) || path.Clean(spec.Path) != spec.Path || spec.Path == "/" {
		return fmt.Errorf("dir path %q: want a clean absolute path below /", spec.Path)
	}
	if path.Base(spec.Path) == trashDir {
		return fmt.Errorf("dir path %q: %s is reserved", spec.Path, trashDir)
	}
	if spec.Mode > 0o7777 {
		return fmt.Errorf("dir %s: mode %o out of range", spec.Path, spec.Mode)
	}
	if spec.UID < 0 || spec.GID < 0 {
		return fmt.Errorf("dir %s: negative uid or gid", spec.Path)
	}
	return nil
}

// applyDir creates the directory the record owns. A directory that exists
// and is not the record's is refused: finalize deletes what the record
// owns, and it must never own data it did not create.
func (m Machine) applyDir(_ context.Context, req applyRequest[dirSpec]) (DirInfo, error) {
	if err := validateDir(req.Spec); err != nil {
		return DirInfo{}, err
	}
	f, err := m.open()
	if err != nil {
		return DirInfo{}, err
	}
	defer f.Close()
	name := rel(req.Spec.Path)
	if _, ours, err := f.held(contract.DirKind, req.Spec.Path, req.Record); err != nil {
		return DirInfo{}, err
	} else if !ours {
		if _, err := f.r.Lstat(name); err == nil {
			return DirInfo{}, conflict(contract.DirKind, req.Spec.Path, "an existing directory")
		} else if !errors.Is(err, fs.ErrNotExist) {
			return DirInfo{}, err
		}
	}
	if _, err := f.take(contract.DirKind, req.Spec.Path, req.Record, func() (string, error) { return "", nil }); err != nil {
		return DirInfo{}, err
	}
	mode := fs.FileMode(req.Spec.Mode)
	if mode == 0 {
		mode = defaultDirMode
	}
	if err := f.r.MkdirAll(name, mode); err != nil {
		return DirInfo{}, err
	}
	if err := setAccess(f, name, mode, req.Spec.UID, req.Spec.GID); err != nil {
		return DirInfo{}, err
	}
	return DirInfo{Path: req.Spec.Path}, nil
}

// setAccess sets mode and owner through a descriptor of the directory
// itself, verified to BE the path: a workload that swaps the path for a
// symlink gets an error, never a chmod or chown of the symlink's target.
// The descriptor is opened first and compared with a no-follow Lstat
// after: a swap before the open shows as a symlink or another inode, a
// swap after it no longer matters — the descriptor holds the real
// directory. MkdirAll's mode passes through the umask; the spec's is
// exact.
func setAccess(f fsys, name string, mode fs.FileMode, uid, gid int) error {
	dir, err := f.r.Open(name)
	if err != nil {
		return err
	}
	defer func() { _ = dir.Close() }()
	opened, err := dir.Stat()
	if err != nil {
		return err
	}
	at, err := f.r.Lstat(name)
	if err != nil {
		return err
	}
	if !at.IsDir() || !os.SameFile(opened, at) {
		return fmt.Errorf("dir %s changed under the record: not the directory it created", name)
	}
	if err := dir.Chmod(mode); err != nil {
		return err
	}
	return dir.Chown(uid, gid)
}

// removeDir moves the record's directory into the trash beside it, gives
// up the claim, and deletes the moved tree. A purge cut short leaves the
// bytes in the trash, never under the record's path.
func (m Machine) removeDir(ctx context.Context, req applyRequest[dirSpec]) error {
	f, err := m.open()
	if err != nil {
		return err
	}
	defer f.Close()
	_, ours, err := f.held(contract.DirKind, req.Spec.Path, req.Record)
	if err != nil || !ours {
		return err
	}
	trash := path.Join(path.Dir(req.Spec.Path), trashDir)
	moved := path.Join(trash, path.Base(req.Spec.Path)+"."+fileSafe(req.Record))
	if err := f.r.MkdirAll(rel(trash), 0o700); err != nil {
		return err
	}
	if err := f.r.Rename(rel(req.Spec.Path), rel(moved)); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return err
	}
	if err := f.drop(contract.DirKind, req.Spec.Path, req.Record); err != nil {
		return err
	}
	if err := purge(ctx, f, rel(moved)); err != nil {
		return err
	}
	// The trash goes once empty; another record's leftovers keep it.
	_ = f.r.Remove(rel(trash))
	return nil
}

// purgeBeat is how often a running purge reports it is alive.
const purgeBeat = 20 * time.Second

// purge removes a tree, heartbeating while the filesystem works through
// it: hundreds of gigabytes in a few files are quick, millions of small
// files are not.
func purge(ctx context.Context, f fsys, dir string) error {
	done := make(chan error, 1)
	go func() { done <- f.r.RemoveAll(dir) }()
	tick := time.NewTicker(purgeBeat)
	defer tick.Stop()
	for {
		select {
		case err := <-done:
			return err
		case <-tick.C:
			heartbeat(ctx, "purging "+dir)
		case <-ctx.Done():
			// RemoveAll has no cancel: the purge is awaited, never left
			// running behind a returned activity.
			<-done
			return ctx.Err()
		}
	}
}
