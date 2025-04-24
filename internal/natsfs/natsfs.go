package natsfs

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
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

func (c *Config) validate() error {
	if c.Bucket == "" {
		return errors.New("bucket name is required")
	}
	return nil
}

func (c *Config) objectStoreConfig() *nats.ObjectStoreConfig {
	natsConfig := &nats.ObjectStoreConfig{
		TTL:         c.TTL,
		Bucket:      c.Bucket,
		Compression: c.Compression,
		Description: c.Description,
	}

	switch c.Storage {
	case FileStorage:
		natsConfig.Storage = nats.FileStorage
	case MemoryStorage:
		natsConfig.Storage = nats.MemoryStorage
	}
	return natsConfig
}

// NatsFs represents a filesystem implementation backed by NATS Object Store.
// It provides file operations with caching capabilities and path resolution.
type NatsFs struct {
	store   nats.ObjectStore
	entries map[string]*NatsFileData
	mu      sync.RWMutex
	locks   map[string]*sync.Mutex
}

func NewNatsFs(js nats.JetStreamContext, config Config) (Fs, error) {
	if err := config.validate(); err != nil {
		return nil, err
	}

	store, err := js.ObjectStore(config.Bucket)
	if err != nil {
		if errors.Is(err, nats.ErrStreamNotFound) {
			// Store doesn't exist, try to create it
			store, err = js.CreateObjectStore(config.objectStoreConfig())
			if err != nil {
				return nil, fmt.Errorf("failed to create object store: %w", err)
			}
		} else {
			return nil, fmt.Errorf("failed to get object store: %w", err)
		}
	}

	return &NatsFs{
		store:   store,
		entries: make(map[string]*NatsFileData),
		locks:   make(map[string]*sync.Mutex),
	}, nil
}

func (n *NatsFs) lockFile(name string) {
	n.mu.Lock()
	if _, exists := n.locks[name]; !exists {
		n.locks[name] = &sync.Mutex{}
	}
	l := n.locks[name]
	n.mu.Unlock()
	l.Lock()
}

func (n *NatsFs) unlockFile(name string) {
	n.mu.RLock()
	if l, ok := n.locks[name]; ok {
		l.Unlock()
	}
	n.mu.RUnlock()
}

func (n *NatsFs) Load(name string) (*NatsFileData, error) {
	if name == "" {
		return nil, errors.New("empty name not allowed")
	}

	n.mu.RLock()
	entry, ok := n.entries[name]
	n.mu.RUnlock()
	if ok {
		return entry, nil
	}

	obj, err := n.store.Get(name)
	if err != nil {
		return nil, fmt.Errorf("failed to get object: %w", err)
	}
	defer func(obj nats.ObjectResult) {
		if err := obj.Close(); err != nil {
			// Log or handle close error
			log.Printf("error closing object: %v", err)
		}
	}(obj)

	data, err := io.ReadAll(obj)
	if err != nil {
		return nil, fmt.Errorf("failed to read object data: %w", err)
	}

	info, err := n.store.GetInfo(name)
	if err != nil {
		return nil, fmt.Errorf("failed to get object info: %w", err)
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
	if file == nil {
		return errors.New("error file is nil")
	}
	n.lockFile(file.ObjectName)
	defer n.unlockFile(file.ObjectName)

	if file.ObjectIsDir {
		return nil
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

	entries, err := n.store.List()
	if err == nil {
		for _, e := range entries {
			if strings.HasPrefix(e.Name, ".meta/dirs/") {
				name := strings.TrimPrefix(e.Name, ".meta/dirs/")
				files = append(files, &NatsFileData{
					ObjectName:    name,
					ObjectIsDir:   true,
					ObjectMode:    os.ModeDir | 0755,
					ObjectModTime: e.ModTime,
				})
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

	if err := n.Save(file); err != nil {
		return nil, err
	}

	return NewNatsFile(file, n), nil
}

func (n *NatsFs) Mkdir(name string, perm os.FileMode) error {
	dir := &NatsFileData{
		ObjectName:    name,
		ObjectIsDir:   true,
		ObjectMode:    perm | os.ModeDir,
		ObjectModTime: time.Now(),
	}
	n.mu.Lock()
	n.entries[name] = dir
	n.mu.Unlock()

	// Backfill directory in the NATS store for remote listing
	metaKey := fmt.Sprintf(".meta/dirs/%s", name)
	_, _ = n.store.Put(&nats.ObjectMeta{Name: metaKey}, bytes.NewReader(nil))

	return nil
}

func (n *NatsFs) MkdirAll(path string, perm os.FileMode) error {
	parts := strings.Split(strings.Trim(path, "/"), "/")
	current := ""
	for _, part := range parts {
		if current == "" {
			current = part
		} else {
			current = fmt.Sprintf("%s/%s", current, part)
		}
		if _, exists := n.entries[current]; !exists {
			if err := n.Mkdir(current, perm); err != nil {
				return err
			}
		}
	}
	return nil
}

func (n *NatsFs) Open(name string) (File, error) {
	entry, err := n.Load(name)
	if err != nil {
		return nil, err
	}
	return NewNatsFile(entry, n), nil
}

func (n *NatsFs) OpenFile(name string, flag int, perm os.FileMode) (File, error) {
	switch {
	case flag&os.O_CREATE != 0:
		entry, exists := n.entries[name]
		if !exists {
			entry = &NatsFileData{
				ObjectName:    name,
				ObjectIsDir:   false,
				ObjectMode:    perm,
				ObjectModTime: time.Now(),
				Data:          []byte{},
			}
			n.entries[name] = entry
		} else if flag&os.O_TRUNC != 0 {
			entry.Data = []byte{}
		}
		return NewNatsFile(entry, n), nil
	default:
		return n.Open(name)
	}
}

func (n *NatsFs) RemoveAll(path string) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	for name := range n.entries {
		if name == path || strings.HasPrefix(name, path+"/") {
			delete(n.entries, name)
			_ = n.store.Delete(name)
		}
	}
	metaKey := fmt.Sprintf(".meta/dirs/%s", path)
	_ = n.store.Delete(metaKey)
	return nil
}

func (n *NatsFs) Rename(oldname, newname string) error {
	n.lockFile(oldname)
	defer n.unlockFile(oldname)
	file, err := n.Load(oldname)
	if err != nil {
		return err
	}
	file.ObjectName = newname
	if err := n.Save(file); err != nil {
		return err
	}
	_ = n.Remove(oldname)

	if file.ObjectIsDir {
		oldMeta := fmt.Sprintf(".meta/dirs/%s", oldname)
		newMeta := fmt.Sprintf(".meta/dirs/%s", newname)
		_ = n.store.Delete(oldMeta)
		_, _ = n.store.Put(&nats.ObjectMeta{Name: newMeta}, bytes.NewReader(nil))
	}
	return nil
}

func (n *NatsFs) Name() string {
	return "natsfs"
}

func (n *NatsFs) Chmod(name string, mode os.FileMode) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	entry, exists := n.entries[name]
	if !exists {
		return os.ErrNotExist
	}
	entry.ObjectMode = mode
	return nil
}

func (n *NatsFs) Chown(name string, uid, gid int) error {
	// Not applicable for NATS Object Store, so we no-op.
	return nil
}

func (n *NatsFs) Chtimes(name string, atime time.Time, mtime time.Time) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	entry, exists := n.entries[name]
	if !exists {
		return os.ErrNotExist
	}
	entry.ObjectModTime = mtime
	return nil
}
