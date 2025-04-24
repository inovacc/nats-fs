// Package natsfs provides a filesystem implementation backed by NATS Object Store.
// It implements a virtual filesystem that stores and retrieves files using NATS
// Object Store as the underlying storage mechanism, with support for caching
// and atomic operations.
package v1

// PipeReader is an interface that defines methods for reading data from a pipe.
type PipeReader interface {
	// Read reads up to len(p) bytes into p. It returns the number of bytes
	// read (0 <= n <= len(p)) and any error encountered. If some data is
	// available but not len(p) bytes, Read conventionally returns what is
	// available instead of waiting for more.
	Read(p []byte) (n int, err error)

	// ReadAt reads len(p) bytes into p starting at offset off. It returns the
	// number of bytes read and any error encountered. When ReadAt returns n < len(p),
	// it returns a non-nil error explaining why more bytes were not returned.
	ReadAt(p []byte, off int64) (n int, err error)

	// Close closes the reader and releases any associated resources.
	Close() error

	// Metadata returns a map of metadata associated with the pipe reader.
	// The returned map contains string key-value pairs of metadata.
	Metadata() map[string]string

	// setMetadata sets the metadata for the pipe reader using a map of
	// string key-value pairs.
	setMetadata(metadata map[string]string)

	// setMetadataFromPointerVal sets the metadata for the pipe reader using a map
	// where values are pointers to strings.
	setMetadataFromPointerVal(metadata map[string]*string)
}

// PipeWriter is an interface for writing data to a destination with buffering capabilities.
// It provides methods for sequential and positioned writings, as well as control operations
// for managing the writing process.
type PipeWriter interface {
	// Write writes len(p) bytes from p to the underlying buffer.
	// Returns the number of bytes written and any error encountered.
	Write(p []byte) (int, error)

	// WriteAt writes len(p) bytes from p to the underlying buffer at offset off.
	// Returns the number of bytes written and any error encountered.
	WriteAt(p []byte, off int64) (int, error)

	// Close commits any buffered data and releases associated resources.
	// After Close, no other methods should be called on the writer.
	Close() error

	// CloseWithError closes the writer with the specified error.
	CloseWithError(err error) error

	// Flush commits any buffered data to the underlying storage.
	Flush() error

	// SetOffset sets the writing position to the specified offset.
	SetOffset(offset int64)

	// Done signals completion of the write operation with an optional error.
	Done(err error)

	// GetWrittenBytes returns the total number of bytes written.
	GetWrittenBytes() int64
}
