// Package natsfs provides a filesystem implementation backed by NATS Object Store.
// It implements a virtual filesystem that stores and retrieves files using NATS
// Object Store as the underlying storage mechanism, with support for caching
// and atomic operations.
package v1

import "bytes"

// staticReader implements a reader backed by a bytes.Reader.
type staticReader struct {
	r *bytes.Reader
}

// Read reads up to len(p) bytes into p. It returns the number of bytes
// read (0 <= n <= len(p)) and any error encountered.
func (sr *staticReader) Read(p []byte) (int, error) {
	return sr.r.Read(p)
}

// ReadAt reads len(p) bytes into p starting at offset off in the underlying input source.
// It returns the number of bytes read and any error encountered.
func (sr *staticReader) ReadAt(p []byte, off int64) (int, error) {
	return sr.r.ReadAt(p, off)
}

// Close implements io.Closer. This is a no-op that always returns nil.
func (sr *staticReader) Close() error {
	return nil
}

// setMetadata is a no-op method that discards the provided metadata.
func (sr *staticReader) setMetadata(_ map[string]string) {}

// setMetadataFromPointerVal is a no-op method that discards the provided pointer metadata.
func (sr *staticReader) setMetadataFromPointerVal(_ map[string]*string) {}

// Metadata returns nil as this implementation doesn't store any metadata.
func (sr *staticReader) Metadata() map[string]string {
	return nil
}
