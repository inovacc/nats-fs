package natsfs

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

type NatsFs struct {
	sync.RWMutex
	store        nats.ObjectStore
	connectionID string
	mountPath    string
	cache        map[string]*cacheEntry
	ttl          time.Duration
}

func NewNatsFs(connectionID, mountPath string, store nats.ObjectStore) (*NatsFs, error) {
	return &NatsFs{
		store:        store,
		connectionID: connectionID,
		mountPath:    getMountPath(mountPath),
	}, nil
}

// Funciones auxiliares para gestionar el caché en NatsFs
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

func (fs *NatsFs) GetMimeType(name string) (string, error) {
	// Intentar detectar el tipo MIME basado en la extensión del archivo
	ext := filepath.Ext(name)
	if ext != "" {
		if mimeType := mime.TypeByExtension(ext); mimeType != "" {
			return mimeType, nil
		}
	}

	// Si no se puede determinar, leer los primeros bytes del archivo
	obj, err := fs.store.Get(name)
	if err != nil {
		return "application/octet-stream", err
	}
	defer obj.Close()

	// Leer los primeros 512 bytes para detectar el tipo MIME
	buffer := make([]byte, 512)
	n, err := obj.Read(buffer)
	if err != nil && err != io.EOF {
		return "application/octet-stream", err
	}

	return http.DetectContentType(buffer[:n]), nil
}

func (fs *NatsFs) Walk(root string, walkFn filepath.WalkFunc) error {
	objects, err := fs.store.List()
	if err != nil {
		return err
	}

	// Primero, llamar walkFn en el directorio raíz
	info := NewFileInfo(root, true, 0, time.Now(), true)
	if err := walkFn(root, info, nil); err != nil {
		return err
	}

	// Procesar todos los objetos
	for _, obj := range objects {
		path := obj.Name
		if !strings.HasPrefix(path, root) {
			continue
		}

		info := NewFileInfo(path, false, int64(obj.Size), obj.ModTime, false)
		if err := walkFn(path, info, nil); err != nil {
			if err == filepath.SkipDir {
				continue
			}
			return err
		}
	}

	return nil
}

func (fs *NatsFs) ResolvePath(virtualPath string) (string, error) {
	// Asegurarse de que la ruta esté limpia y normalizada
	cleanPath := filepath.Clean(virtualPath)
	if !strings.HasPrefix(cleanPath, fs.mountPath) {
		return "", fmt.Errorf("path %s outside of mount point %s", virtualPath, fs.mountPath)
	}

	// Quitar el prefijo del punto de montaje
	relPath := strings.TrimPrefix(cleanPath, fs.mountPath)
	if relPath == "" {
		relPath = "/"
	}
	return relPath, nil
}

func (fs *NatsFs) ReadDir(dirname string) (*NatsDirLister, error) {
	// Implementación mejorada para listar objetos
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
		// Solo incluir objetos que estén en el directorio actual
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

func (fs *NatsFs) Name() string {
	return "natsfs"
}

func (fs *NatsFs) ConnectionID() string {
	return fs.connectionID
}

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

func (fs *NatsFs) Lstat(name string) (os.FileInfo, error) {
	return fs.Stat(name)
}

func (fs *NatsFs) Open(name string, offset int64) (os.File, PipeReader, func(), error) {
	// First try to get from the cache
	if entry, exists := fs.getCached(name); exists {
		data, err := entry.getData()
		if err == nil {
			reader := bytes.NewReader(data[offset:])
			return nil, &staticReader{r: reader}, func() {}, nil
		}
		// If we got an error (expired cache), invalidate the entry
		fs.invalidateCache(name)
	}

	// If not in cache or expired, get from NATS
	obj, err := fs.store.Get(name)
	if err != nil {
		return nil, nil, nil, err
	}
	defer obj.Close()

	data, err := io.ReadAll(obj)
	if err != nil {
		return nil, nil, nil, err
	}

	// Cache the new data
	info, err := fs.store.GetInfo(name)
	if err == nil {
		fs.setCached(name, info, data)
	}

	reader := bytes.NewReader(data[offset:])
	return nil, &staticReader{r: reader}, func() {}, nil
}

func (fs *NatsFs) Create(name string, flag, checks int) (os.File, PipeWriter, func(), error) {
	buf := new(bytes.Buffer)
	return nil, &bufferWriter{
		name: name,
		fs:   fs,
		buf:  buf,
	}, func() {}, nil
}

func (fs *NatsFs) Remove(name string, _ bool) error {
	return fs.store.Delete(name)
}

func (fs *NatsFs) Mkdir(name string) error {
	return nil // NATS Object Store is flat
}

func (fs *NatsFs) Symlink(source, target string) error       { return ErrVfsUnsupported }
func (fs *NatsFs) Chown(name string, uid int, gid int) error { return nil }
func (fs *NatsFs) Chmod(name string, mode os.FileMode) error { return nil }
func (fs *NatsFs) Chtimes(name string, atime, mtime time.Time, isUploading bool) error {
	return nil
}
func (fs *NatsFs) Truncate(name string, size int64) error { return ErrVfsUnsupported }

// func (fs *NatsFs) ReadDir(dirname string) (DirLister, error)            { return nil, ErrVfsUnsupported }
func (fs *NatsFs) Readlink(name string) (string, error)                 { return "", ErrVfsUnsupported }
func (fs *NatsFs) IsUploadResumeSupported() bool                        { return false }
func (fs *NatsFs) IsConditionalUploadResumeSupported(size int64) bool   { return false }
func (fs *NatsFs) IsAtomicUploadSupported() bool                        { return true }
func (fs *NatsFs) CheckRootPath(username string, uid int, gid int) bool { return true }

// func (fs *NatsFs) ResolvePath(virtualPath string) (string, error)       { return virtualPath, nil }
func (fs *NatsFs) IsNotExist(err error) bool                     { return errors.Is(err, os.ErrNotExist) }
func (fs *NatsFs) IsPermission(err error) bool                   { return false }
func (fs *NatsFs) IsNotSupported(err error) bool                 { return err == ErrVfsUnsupported }
func (fs *NatsFs) ScanRootDirContents() (int, int64, error)      { return 0, 0, nil }
func (fs *NatsFs) GetDirSize(dirname string) (int, int64, error) { return 0, 0, nil }
func (fs *NatsFs) GetAtomicUploadPath(name string) string        { return name }
func (fs *NatsFs) GetRelativePath(name string) string            { return name }

// func (fs *NatsFs) Walk(root string, walkFn filepath.WalkFunc) error     { return ErrVfsUnsupported }
func (fs *NatsFs) Join(elem ...string) string { return strings.Join(elem, "/") }
func (fs *NatsFs) HasVirtualFolders() bool    { return false }

// func (fs *NatsFs) GetMimeType(name string) (string, error)              { return "application/octet-stream", nil }
func (fs *NatsFs) GetAvailableDiskSize(dirName string) (*sftp.StatVFS, error) {
	return nil, ErrStorageSizeUnavailable
}
func (fs *NatsFs) Close() error { return nil }
