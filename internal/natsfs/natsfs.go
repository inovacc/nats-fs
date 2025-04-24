package natsfs

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
)

var (
	_ os.FileInfo    = (*NatsFileData)(nil)
	_ fs.ReadDirFile = (*NatsFileImpl)(nil)
)

// File represents a file in the filesystem.
type File interface {
	io.Closer
	io.Reader
	io.ReaderAt
	io.Seeker
	io.Writer
	io.WriterAt

	Name() string
	Readdir(count int) ([]os.FileInfo, error)
	Readdirnames(n int) ([]string, error)
	Stat() (os.FileInfo, error)
	Sync() error
	Truncate(size int64) error
	WriteString(s string) (ret int, err error)
}

// Fs is the filesystem interface.
//
// Any simulated or real filesystem should implement this interface.
type Fs interface {
	// Create creates a file in the filesystem, returning the file and an
	// error, if any happens.
	Create(name string) (File, error)

	// Mkdir creates a directory in the filesystem, return an error if any
	// happens.
	Mkdir(name string, perm os.FileMode) error

	// MkdirAll creates a directory path and all parents that does not exist
	// yet.
	MkdirAll(path string, perm os.FileMode) error

	// Open opens a file, returning it or an error, if any happens.
	Open(name string) (File, error)

	// OpenFile opens a file using the given flags and the given mode.
	OpenFile(name string, flag int, perm os.FileMode) (File, error)

	// Remove removes a file identified by name, returning an error, if any
	// happens.
	Remove(name string) error

	// RemoveAll removes a directory path and any children it contains. It
	// does not fail if the path does not exist (return nil).
	RemoveAll(path string) error

	// Rename renames a file.
	Rename(oldname, newname string) error

	// Stat returns a FileInfo describing the named file, or an error, if any
	// happens.
	Stat(name string) (os.FileInfo, error)

	// Name The name of this FileSystem
	Name() string

	// Chmod changes the mode of the named file to mode.
	Chmod(name string, mode os.FileMode) error

	// Chown changes the uid and gid of the named file.
	Chown(name string, uid, gid int) error

	// Chtimes changes the access and modification times of the named file
	Chtimes(name string, atime time.Time, mtime time.Time) error
}

type NatsFileImpl struct {
	data     *NatsFileData
	fs       *NatsFs
	position int64
	mu       sync.RWMutex
}

func (f *NatsFileImpl) Close() error {
	return f.fs.Save(f.data)
}

func (f *NatsFileImpl) Read(p []byte) (n int, err error) {
	f.mu.RLock()
	defer f.mu.RUnlock()

	if f.position >= int64(len(f.data.Data)) {
		return 0, io.EOF
	}

	n = copy(p, f.data.Data[f.position:])
	f.position += int64(n)
	return n, nil
}

func (f *NatsFileImpl) ReadAt(p []byte, off int64) (n int, err error) {
	f.mu.RLock()
	defer f.mu.RUnlock()

	if off < 0 {
		return 0, errors.New("negative offset")
	}
	if off >= int64(len(f.data.Data)) {
		return 0, io.EOF
	}

	n = copy(p, f.data.Data[off:])
	if n < len(p) {
		err = io.EOF
	}
	return n, err
}

func (f *NatsFileImpl) Seek(offset int64, whence int) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	var abs int64
	switch whence {
	case io.SeekStart:
		abs = offset
	case io.SeekCurrent:
		abs = f.position + offset
	case io.SeekEnd:
		abs = int64(len(f.data.Data)) + offset
	default:
		return 0, errors.New("invalid whence")
	}

	if abs < 0 {
		return 0, errors.New("negative position")
	}

	f.position = abs
	return abs, nil
}

func (f *NatsFileImpl) Write(p []byte) (n int, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	// Ensure capacity
	if required := int(f.position) + len(p); required > len(f.data.Data) {
		newData := make([]byte, required)
		copy(newData, f.data.Data)
		f.data.Data = newData
	}

	n = copy(f.data.Data[f.position:], p)
	f.position += int64(n)
	f.data.ObjectModTime = time.Now()
	return n, nil
}

func (f *NatsFileImpl) WriteAt(p []byte, off int64) (n int, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if off < 0 {
		return 0, errors.New("negative offset")
	}

	// Ensure capacity
	if required := int(off) + len(p); required > len(f.data.Data) {
		newData := make([]byte, required)
		copy(newData, f.data.Data)
		f.data.Data = newData
	}

	n = copy(f.data.Data[off:], p)
	f.data.ObjectModTime = time.Now()
	return n, nil
}

func (f *NatsFileImpl) Name() string {
	return f.data.ObjectName
}

func (f *NatsFileImpl) Readdir(count int) ([]os.FileInfo, error) {
	if !f.data.ObjectIsDir {
		return nil, errors.New("not a directory")
	}

	files, err := f.fs.List(f.data.ObjectName)
	if err != nil {
		return nil, err
	}

	var fileInfos []os.FileInfo
	for _, file := range files {
		fileInfos = append(fileInfos, file)
	}

	if count > 0 {
		if count > len(fileInfos) {
			count = len(fileInfos)
		}
		fileInfos = fileInfos[:count]
	}

	return fileInfos, nil
}

func (f *NatsFileImpl) ReadDir(n int) ([]fs.DirEntry, error) {
	files, err := f.Readdir(n)
	if err != nil {
		return nil, err
	}
	entries := make([]fs.DirEntry, len(files))
	for i, fi := range files {
		entries[i] = fs.FileInfoToDirEntry(fi)
	}
	return entries, nil
}

func (f *NatsFileImpl) Readdirnames(n int) ([]string, error) {
	files, err := f.Readdir(n)
	if err != nil {
		return nil, err
	}

	names := make([]string, len(files))
	for i, file := range files {
		names[i] = file.Name()
	}
	return names, nil
}

func (f *NatsFileImpl) Stat() (os.FileInfo, error) {
	return f.data, nil
}

func (f *NatsFileImpl) Sync() error {
	return f.fs.Save(f.data)
}

func (f *NatsFileImpl) Truncate(size int64) error {
	if size < 0 {
		return errors.New("negative size")
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	if size > int64(len(f.data.Data)) {
		newData := make([]byte, size)
		copy(newData, f.data.Data)
		f.data.Data = newData
	} else {
		f.data.Data = f.data.Data[:size]
	}

	f.data.ObjectModTime = time.Now()
	return nil
}

func (f *NatsFileImpl) WriteString(s string) (n int, err error) {
	return f.Write([]byte(s))
}

type NatsFileData struct {
	ObjectName    string
	ObjectIsDir   bool
	Data          []byte
	ObjectMode    os.FileMode
	ObjectModTime time.Time
}

func NewNatsFile(data *NatsFileData, fs *NatsFs) File {
	return &NatsFileImpl{
		data: data,
		fs:   fs,
	}
}

func (f *NatsFileData) Name() string {
	return f.ObjectName
}

func (f *NatsFileData) Size() int64 {
	return int64(len(f.Data))
}

func (f *NatsFileData) Mode() os.FileMode {
	return f.ObjectMode
}

func (f *NatsFileData) ModTime() time.Time {
	return f.ObjectModTime
}

func (f *NatsFileData) IsDir() bool {
	return f.ObjectIsDir
}

func (f *NatsFileData) Sys() any {
	return nil
}

type StorageType int

const (
	// FileStorage specifies on disk storage. It's the default.
	FileStorage StorageType = iota
	// MemoryStorage specifies in memory only.
	MemoryStorage
)

type Config struct {
	ConnectionID string
	MountPath    string
	Description  string
	Bucket       string
	TTL          time.Duration
	Compression  bool
	Storage      StorageType
}

// NatsFs represents a filesystem implementation backed by NATS Object Store.
// It provides file operations with caching capabilities and path resolution.
type NatsFs struct {
	store   nats.ObjectStore
	entries map[string]*NatsFileData
	mu      sync.RWMutex
}

func NewNatsFs(js nats.JetStreamContext, config Config) (Fs, error) {
	ttl := config.TTL
	if ttl == 0 {
		ttl = 24 * time.Hour
	}

	natsConfig := &nats.ObjectStoreConfig{
		TTL:         ttl,
		Bucket:      config.Bucket,
		Compression: config.Compression,
		Description: config.Description,
	}

	switch config.Storage {
	case FileStorage:
		natsConfig.Storage = nats.FileStorage
	case MemoryStorage:
		natsConfig.Storage = nats.MemoryStorage
	}

	store, err := js.CreateObjectStore(natsConfig)
	if err != nil && !errors.Is(err, nats.ErrBucketNotFound) {
		return nil, err
	}

	return &NatsFs{
		store:   store,
		entries: make(map[string]*NatsFileData),
	}, nil
}

func (n *NatsFs) Load(name string) (*NatsFileData, error) {
	n.mu.RLock()
	entry, ok := n.entries[name]
	n.mu.RUnlock()
	if ok {
		return entry, nil
	}

	obj, err := n.store.Get(name)
	if err != nil {
		return nil, err
	}
	defer obj.Close()

	data, err := io.ReadAll(obj)
	if err != nil {
		return nil, err
	}

	info, err := n.store.GetInfo(name)
	if err != nil {
		return nil, err
	}

	file := &NatsFileData{
		ObjectName:    name,
		ObjectIsDir:   false,
		Data:          data,
		ObjectMode:    0644,
		ObjectModTime: info.ModTime,
	}

	n.mu.Lock()
	n.entries[name] = file
	n.mu.Unlock()

	return file, nil
}

func (n *NatsFs) Save(file *NatsFileData) error {
	if file.ObjectIsDir {
		return nil // No actual write needed for dirs
	}
	_, err := n.store.Put(&nats.ObjectMeta{Name: file.ObjectName}, bytes.NewReader(file.Data))
	if err == nil {
		n.mu.Lock()
		n.entries[file.ObjectName] = file
		n.mu.Unlock()
	}
	return err
}

func (n *NatsFs) List(dir string) ([]*NatsFileData, error) {
	n.mu.RLock()
	defer n.mu.RUnlock()
	var files []*NatsFileData
	prefix := fmt.Sprintf("/%s", strings.TrimSuffix(dir, "/"))
	for name, file := range n.entries {
		if strings.HasPrefix(name, prefix) && name != dir {
			rel := strings.TrimPrefix(name, prefix)
			if !strings.Contains(rel, "/") {
				files = append(files, file)
			}
		}
	}
	return files, nil
}

func (n *NatsFs) CreateFile(name string, data []byte) (*NatsFileData, error) {
	file := &NatsFileData{
		ObjectName:    name,
		ObjectIsDir:   false,
		Data:          data,
		ObjectMode:    0644,
		ObjectModTime: time.Now(),
	}
	return file, n.Save(file)
}

func (n *NatsFs) CreateDir(name string) *NatsFileData {
	dir := &NatsFileData{
		ObjectName:    name,
		ObjectIsDir:   true,
		ObjectModTime: time.Now(),
		ObjectMode:    os.ModeDir | 0755,
	}
	n.mu.Lock()
	n.entries[name] = dir
	n.mu.Unlock()
	return dir
}

func (n *NatsFs) Remove(name string) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	delete(n.entries, name)
	return n.store.Delete(name)
}

func (n *NatsFs) Stat(name string) (os.FileInfo, error) {
	n.mu.RLock()
	entry, ok := n.entries[name]
	n.mu.RUnlock()
	if !ok {
		return nil, os.ErrNotExist
	}
	return entry, nil
}

func (n *NatsFs) Create(name string) (File, error) {
	file := &NatsFileData{
		ObjectName:    name,
		ObjectIsDir:   false,
		Data:          make([]byte, 0),
		ObjectMode:    0644,
		ObjectModTime: time.Now(),
	}

	err := n.Save(file)
	if err != nil {
		return nil, err
	}

	return NewNatsFile(file, n), nil
}

func (n *NatsFs) Mkdir(name string, perm os.FileMode) error {
	// TODO implement me
	panic("implement me")
}

func (n *NatsFs) MkdirAll(path string, perm os.FileMode) error {
	// TODO implement me
	panic("implement me")
}

func (n *NatsFs) Open(name string) (File, error) {
	// TODO implement me
	panic("implement me")
}

func (n *NatsFs) OpenFile(name string, flag int, perm os.FileMode) (File, error) {
	// TODO implement me
	panic("implement me")
}

func (n *NatsFs) RemoveAll(path string) error {
	// TODO implement me
	panic("implement me")
}

func (n *NatsFs) Rename(oldname, newname string) error {
	file, err := n.Load(oldname)
	if err != nil {
		return err
	}
	file.ObjectName = newname
	if err := n.Save(file); err != nil {
		return err
	}
	return n.Remove(oldname)
}

func (n *NatsFs) Name() string {
	// TODO implement me
	panic("implement me")
}

func (n *NatsFs) Chmod(name string, mode os.FileMode) error {
	// TODO implement me
	panic("implement me")
}

func (n *NatsFs) Chown(name string, uid, gid int) error {
	// TODO implement me
	panic("implement me")
}

func (n *NatsFs) Chtimes(name string, atime time.Time, mtime time.Time) error {
	// TODO implement me
	panic("implement me")
}
