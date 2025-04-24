// Package natsfs provides a filesystem implementation backed by NATS Object Store.
// It implements a virtual filesystem that stores and retrieves files using NATS
// Object Store as the underlying storage mechanism, with support for caching
// and atomic operations.
package v1

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
)

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
	sync.RWMutex
	store        nats.ObjectStore
	connectionID string
	mountPath    string
	cache        map[string]*cacheEntry
	ttl          time.Duration
}

// NewNatsFs creates a new NATS filesystem instance.
// It requires a connection ID, mount path, and an initialized NATS Object Store.
// The mount path will be normalized to ensure consistent path handling.
func NewNatsFs(js nats.JetStreamContext, config Config) (*NatsFs, error) {
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
		store:        store,
		ttl:          ttl,
		connectionID: config.ConnectionID,
		cache:        make(map[string]*cacheEntry),
		mountPath:    getMountPath(config.MountPath),
	}, nil
}

func (fs *NatsFs) getCached(name string) (*cacheEntry, bool) {
	fs.RLock()
	defer fs.RUnlock()

	entry, exists := fs.cache[name]
	if !exists || entry.isExpired() {
		return nil, false
	}
	return entry, true
}

func (fs *NatsFs) setCached(name string, info *nats.ObjectInfo, data []byte) {
	fs.Lock()
	defer fs.Unlock()
	fs.cache[name] = newCacheEntry(info, data, fs.ttl)
}

func (fs *NatsFs) invalidateCache(name string) {
	fs.Lock()
	defer fs.Unlock()

	delete(fs.cache, name)
}

func (fs *NatsFs) mapNATSError(err error) error {
	if err == nil {
		return nil
	}
	switch {
	case errors.Is(err, nats.ErrObjectNotFound):
		return os.ErrNotExist
	case errors.Is(err, nats.ErrInvalidKey):
		return os.ErrInvalid
	default:
		return err
	}
}

// GetMimeType attempts to detect the MIME type of file by its extension or content.
// If the extension-based detection fails, it reads the first 512 bytes of the file
// for content-based detection.
func (fs *NatsFs) GetMimeType(name string) (string, error) {
	// Try to detect the MIME type based on an extension file
	if ext := filepath.Ext(name); ext != "" {
		if mimeType := mime.TypeByExtension(ext); mimeType != "" {
			return mimeType, nil
		}
	}

	// If the extension-based detection fails, it reads the first 512 bytes of the file
	obj, err := fs.store.Get(name)
	if err != nil {
		return "application/octet-stream", err
	}
	defer func(obj nats.ObjectResult) {
		_ = obj.Close()
	}(obj)

	// Read the first 512 bytes to detect the MIME type
	buffer := make([]byte, 512)
	n, err := obj.Read(buffer)
	if err != nil && err != io.EOF {
		return "application/octet-stream", err
	}

	return http.DetectContentType(buffer[:n]), nil
}

// Walk recursively traverses the filesystem starting from root, calling walkFn
// for each file or directory encountered.
func (fs *NatsFs) Walk(root string, walkFn filepath.WalkFunc) error {
	objects, err := fs.store.List()
	if err != nil {
		return err
	}

	// First call walkFn in the root directory
	info := NewFileInfo(root, true, 0, time.Now(), true)
	if err := walkFn(root, info, nil); err != nil {
		return err
	}

	// Process all the objects
	for _, obj := range objects {
		path := obj.Name
		if !strings.HasPrefix(path, root) {
			continue
		}

		info := NewFileInfo(path, false, int64(obj.Size), obj.ModTime, false)
		if err := walkFn(path, info, nil); err != nil {
			if errors.Is(err, filepath.SkipDir) {
				continue
			}
			return err
		}
	}

	return nil
}

// ResolvePath converts a virtual path to its actual filesystem path.
// It ensures the path is within the mount point and returns the relative path.
func (fs *NatsFs) ResolvePath(virtualPath string) (string, error) {
	// Ensure the path is clean and normalized
	cleanPath := filepath.Clean(virtualPath)
	if !strings.HasPrefix(cleanPath, fs.mountPath) {
		return "", fmt.Errorf("path %s outside of mount point %s", virtualPath, fs.mountPath)
	}

	// Remove dot prefix if present
	relPath := strings.TrimPrefix(cleanPath, fs.mountPath)
	if relPath == "" {
		relPath = "/"
	}
	return relPath, nil
}

// ReadDir returns a directory listing for the specified path.
// It returns a NatsDirLister that provides access to the directory entries.
func (fs *NatsFs) ReadDir(dirname string) (*NatsDirLister, error) {
	// Implement mount point support for directories
	objects, err := fs.store.List()
	if err != nil {
		return nil, err
	}

	var entries []os.FileInfo
	prefix := dirname
	if prefix != "/" {
		prefix = prefix + "/"
	}

	for _, obj := range objects {
		// Only include objects in the current directory
		if strings.HasPrefix(obj.Name, prefix) {
			relPath := strings.TrimPrefix(obj.Name, prefix)
			if !strings.Contains(relPath, "/") {
				entries = append(entries, NewFileInfo(
					obj.Name,
					false,
					int64(obj.Size),
					obj.ModTime,
					false,
				))
			}
		}
	}

	return &NatsDirLister{entries: entries, pos: 0}, nil
}

// Name returns the name of the filesystem implementation ("natsfs").
func (fs *NatsFs) Name() string {
	return "natsfs"
}

// ConnectionID returns the connection identifier associated with this filesystem.
func (fs *NatsFs) ConnectionID() string {
	return fs.connectionID
}

// Stat returns file information for the specified path.
func (fs *NatsFs) Stat(name string) (os.FileInfo, error) {
	info, err := fs.store.GetInfo(name)
	if err != nil {
		if errors.Is(err, nats.ErrObjectNotFound) {
			return nil, os.ErrNotExist
		}
		return nil, err
	}
	return NewFileInfo(name, false, int64(info.Size), info.ModTime, false), nil
}

// Lstat returns file information for the specified path.
// This implementation is identical to Stat as symbolic links are not supported.
func (fs *NatsFs) Lstat(name string) (os.FileInfo, error) {
	return fs.Stat(name)
}

// Open opens a file for reading at the specified offset.
// It attempts to retrieve the file from the cache first, falling back to NATS storage if necessary.
func (fs *NatsFs) Open(name string, offset int64) (os.File, PipeReader, error) {
	// First try to get from the cache
	if entry, exists := fs.getCached(name); exists {
		data, err := entry.getData()
		if err == nil {
			reader := bytes.NewReader(data[offset:])
			return os.File{}, &staticReader{r: reader}, nil
		}
		// If we got an error (expired cache), invalidate the entry
		fs.invalidateCache(name)
	}

	// If not in cache or expired, get from NATS
	obj, err := fs.store.Get(name)
	if err != nil {
		return os.File{}, nil, err
	}
	defer func(obj nats.ObjectResult) {
		_ = obj.Close()
	}(obj)

	data, err := io.ReadAll(obj)
	if err != nil {
		return os.File{}, nil, err
	}

	// Cache the new data
	info, err := fs.store.GetInfo(name)
	if err == nil {
		fs.setCached(name, info, data)
	}

	reader := bytes.NewReader(data[offset:])
	return os.File{}, &staticReader{r: reader}, nil
}

// Create creates a new file or truncates an existing file.
// It returns a writer that buffers data until it's committed to NATS storage.
func (fs *NatsFs) Create(name string) (os.File, PipeWriter, error) {
	buf := new(bytes.Buffer)
	return os.File{}, &bufferWriter{
		name: name,
		fs:   fs,
		buf:  buf,
	}, nil
}

// Remove deletes the specified file from the NATS storage.
func (fs *NatsFs) Remove(name string, _ bool) error {
	return fs.store.Delete(name)
}

func (fs *NatsFs) Mkdir(name string) error {
	return nil // NATS Object Store is flat
}

func (fs *NatsFs) Chown(name string, uid int, gid int) error { return nil }

func (fs *NatsFs) Chmod(name string, mode os.FileMode) error { return nil }

func (fs *NatsFs) Chtimes(name string, atime, mtime time.Time, isUploading bool) error {
	return nil
}

func (fs *NatsFs) IsUploadResumeSupported() bool { return false }

func (fs *NatsFs) IsConditionalUploadResumeSupported(size int64) bool { return false }

// IsAtomicUploadSupported returns true as this filesystem supports atomic uploads.
func (fs *NatsFs) IsAtomicUploadSupported() bool { return true }

func (fs *NatsFs) CheckRootPath(username string, uid int, gid int) bool { return true }

func (fs *NatsFs) IsNotExist(err error) bool { return errors.Is(err, os.ErrNotExist) }

func (fs *NatsFs) IsPermission(err error) bool { return false }

func (fs *NatsFs) ScanRootDirContents() (int, int64, error) { return 0, 0, nil }

func (fs *NatsFs) GetDirSize(dirname string) (int, int64, error) { return 0, 0, nil }

func (fs *NatsFs) GetAtomicUploadPath(name string) string { return name }

func (fs *NatsFs) GetRelativePath(name string) string { return name }

func (fs *NatsFs) Join(elem ...string) string { return strings.Join(elem, "/") }

// HasVirtualFolders returns false as this filesystem doesn't support virtual folders.
func (fs *NatsFs) HasVirtualFolders() bool { return false }

// Close performs cleanup when the filesystem is no longer needed.
func (fs *NatsFs) Close() error { return nil }

// getMountPath normalizes the provided mount path to ensure it's in a consistent format.
// It ensures the path:
// - Always starts with a forward slash
// - Does not end with a trailing slash (except for a root path "/")
// - Is cleaned of any redundant separators
func getMountPath(mountPath string) string {
	if mountPath == "" {
		return "/"
	}
	// Clean the path to remove any redundant separators
	mountPath = filepath.Clean(mountPath)
	// Ensure a path starts with /
	if !strings.HasPrefix(mountPath, "/") {
		mountPath = fmt.Sprintf("/%s", mountPath)
	}
	return mountPath
}
