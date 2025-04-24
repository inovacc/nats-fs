// Package natsfs provides a filesystem implementation backed by NATS Object Store.
// It implements a virtual filesystem that stores and retrieves files using NATS
// Object Store as the underlying storage mechanism, with support for caching
// and atomic operations.
package v1

import (
	"bytes"
	"fmt"

	"github.com/nats-io/nats.go"
)

// bufferWriter implements PipeWriter to write content to NATS Object Store.
// It buffers the written data in memory until Close/Flush is called,
// at which point the data is stored in NATS.
type bufferWriter struct {
	name string        // Name of the file/object to be written
	fs   *NatsFs       // Reference to the NATS filesystem
	buf  *bytes.Buffer // Buffer to temporarily store data
}

// Write writes len(p) bytes from p to the underlying buffer.
// It returns the number of bytes written and any error encountered.
func (bw *bufferWriter) Write(p []byte) (int, error) {
	return bw.buf.Write(p)
}

// WriteAt writes len(p) bytes from p to the underlying buffer at offset off.
// It returns an error if the offset does not match the current buffer length
// as random access writes are not supported.
func (bw *bufferWriter) WriteAt(p []byte, off int64) (int, error) {
	if int(off) != bw.buf.Len() {
		return 0, fmt.Errorf("WriteAt not supported correctly")
	}
	return bw.buf.Write(p)
}

// Close commits the buffered data to NATS Object Store.
// After Close, no other methods should be called on the bufferWriter.
func (bw *bufferWriter) Close() error {
	_, err := bw.fs.store.Put(&nats.ObjectMeta{Name: bw.name}, bytes.NewReader(bw.buf.Bytes()))
	return err
}

// CloseWithError is equivalent to Close and ignores the provided error.
// It exists to satisfy interfaces that require this method.
func (bw *bufferWriter) CloseWithError(err error) error {
	return bw.Close()
}

// Flush commits the buffered data to NATS Object Store.
// It is equivalent to calling Close.
func (bw *bufferWriter) Flush() error {
	return bw.Close()
}

// SetOffset is a no-op method that exists to satisfy interfaces.
// It does not affect the buffer position.
func (bw *bufferWriter) SetOffset(offset int64) {}

// Done is a no-op method that exists to satisfy interfaces.
// It does not affect the buffer state.
func (bw *bufferWriter) Done(err error) {}

// GetWrittenBytes returns the total number of bytes currently stored in the buffer.
func (bw *bufferWriter) GetWrittenBytes() int64 {
	return int64(bw.buf.Len())
}
