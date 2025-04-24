// Package natsfs provides a filesystem implementation backed by NATS Object Store.
// It implements a virtual filesystem that stores and retrieves files using NATS
// Object Store as the underlying storage mechanism, with support for caching
// and atomic operations.
package v1

import (
	"io"
	"os"
)

// NatsDirLister implements a directory listing iterator for the NATS object store.
// It maintains a list of file entries and keeps track of the current position.
type NatsDirLister struct {
	entries []os.FileInfo
	pos     int
}

// Next returns the next directory entry.
// It returns io.EOF when there are no more entries to read.
func (dl *NatsDirLister) Next() (os.FileInfo, error) {
	if dl.pos >= len(dl.entries) {
		return nil, io.EOF
	}
	entry := dl.entries[dl.pos]
	dl.pos++
	return entry, nil
}
