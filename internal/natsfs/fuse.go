package natsfs

import (
	"context"
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
