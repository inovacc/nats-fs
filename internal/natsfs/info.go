// Package natsfs provides a filesystem implementation backed by NATS Object Store.
// It implements a virtual filesystem that stores and retrieves files using NATS
// Object Store as the underlying storage mechanism, with support for caching
// and atomic operations.
package natsfs

import (
	"os"
	"time"
)

// fileInfo implements os.FileInfo interface for NATS objects
type fileInfo struct {
	name    string      // File name
	size    int64       // File size in bytes
	mode    os.FileMode // File mode and permission bits
	modTime time.Time   // Modification time
	isDir   bool        // Is this a directory?
}

// NewFileInfo creates a new FileInfo instance.
// Parameters:
// - name: name of the file or directory
// - isDir: true if this represents a directory
// - size: size in bytes (typically 0 for directories)
// - modTime: last modification time
// - isRoot: true if this represents the root directory
func NewFileInfo(name string, isDir bool, size int64, modTime time.Time, isRoot bool) os.FileInfo {
	mode := os.FileMode(0644) // default file permission: rw-r--r--
	if isDir {
		mode = os.FileMode(0755) | os.ModeDir // directory permission: rwxr-xr-x
	}

	// Ensure the root directory is represented as "/"
	if isRoot {
		name = "/"
	}

	return &fileInfo{
		name:    name,
		size:    size,
		mode:    mode,
		modTime: modTime,
		isDir:   isDir,
	}
}

// Implementation of os.FileInfo interface

// Name returns the base name of the file
func (fi *fileInfo) Name() string {
	return fi.name
}

// Size returns the size of the file in bytes
func (fi *fileInfo) Size() int64 {
	return fi.size
}

// Mode returns the file mode bits
func (fi *fileInfo) Mode() os.FileMode {
	return fi.mode
}

// ModTime returns the modification time
func (fi *fileInfo) ModTime() time.Time {
	return fi.modTime
}

// IsDir returns true if the file is a directory
func (fi *fileInfo) IsDir() bool {
	return fi.isDir
}

// Sys returns the underlying data source (always returns nil for this implementation)
func (fi *fileInfo) Sys() interface{} {
	return nil
}
