//go:build !windows

package natsfs

import (
	"context"
	"io"
	"os"
	"strings"
	"syscall"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
)

type NatsFsRoot struct {
	fs.Inode
	nfs *NatsFs
}

type NatsFileNode struct {
	fs.Inode
	fsys *NatsFs
	name string
	file File
}

func MountNatsFs(mountPoint string, nfs *NatsFs) error {
	if isMountPoint(mountPoint) {
		return syscall.EBUSY
	}

	root := &NatsFsRoot{nfs: nfs}
	server, err := fs.Mount(mountPoint, root, &fs.Options{
		MountOptions: fuse.MountOptions{
			AllowOther: true,
			Name:       "natsfs",
		},
	})
	if err != nil {
		return err
	}
	server.Wait()
	return nil
}

func (r *NatsFsRoot) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	info, err := r.nfs.Stat(name)
	if err != nil {
		return nil, syscall.ENOENT
	}

	child := &NatsFileNode{
		fsys: r.nfs,
		name: name,
	}

	mode := uint32(info.Mode())
	return r.NewPersistentInode(ctx, child, fs.StableAttr{Mode: mode}), fs.OK
}

func (r *NatsFsRoot) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	files, err := r.nfs.List("")
	if err != nil {
		return nil, syscall.EIO
	}

	var entries []fuse.DirEntry
	for _, file := range files {
		entries = append(entries, fuse.DirEntry{
			Name: file.Name(),
			Mode: uint32(file.Mode()),
		})
	}
	return fs.NewListDirStream(entries), fs.OK
}

func (n *NatsFileNode) Getattr(ctx context.Context, f fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	info, err := n.fsys.Stat(n.name)
	if err != nil {
		return syscall.ENOENT
	}
	out.Mode = uint32(info.Mode())
	out.Size = uint64(info.Size())
	out.Mtime = uint64(info.ModTime().Unix())
	out.Ctime = out.Mtime
	out.Atime = out.Mtime
	out.Nlink = 1
	return fs.OK
}

func (n *NatsFileNode) Open(ctx context.Context, flags uint32) (fs.FileHandle, uint32, syscall.Errno) {
	f, err := n.fsys.Open(n.name)
	if err != nil {
		return nil, 0, syscall.ENOENT
	}
	n.file = f
	return n, fuse.FOPEN_KEEP_CACHE, fs.OK
}

func (n *NatsFileNode) Read(ctx context.Context, fh fs.FileHandle, dest []byte, off int64) (fuse.ReadResult, syscall.Errno) {
	buf := make([]byte, len(dest))
	nRead, err := n.file.ReadAt(buf, off)
	if err != nil && err != io.EOF {
		return nil, syscall.EIO
	}
	return fuse.ReadResultData(buf[:nRead]), fs.OK
}

func (n *NatsFileNode) Write(ctx context.Context, fh fs.FileHandle, data []byte, off int64) (uint32, syscall.Errno) {
	nWritten, err := n.file.WriteAt(data, off)
	if err != nil {
		return 0, syscall.EIO
	}
	return uint32(nWritten), fs.OK
}

func (r *NatsFsRoot) Create(ctx context.Context, name string, flags uint32, mode uint32, out *fuse.EntryOut) (*fs.Inode, fs.FileHandle, uint32, syscall.Errno) {
	f, err := r.nfs.Create(name)
	if err != nil {
		return nil, nil, 0, syscall.EIO
	}
	fileNode := &NatsFileNode{
		fsys: r.nfs,
		name: name,
		file: f,
	}
	inode := r.NewPersistentInode(ctx, fileNode, fs.StableAttr{Mode: mode})
	return inode, fileNode, fuse.FOPEN_KEEP_CACHE, fs.OK
}

func isMountPoint(path string) bool {
	mounts, err := os.ReadFile("/proc/mounts")
	if err != nil {
		return false
	}
	return strings.Contains(string(mounts), path)
}
