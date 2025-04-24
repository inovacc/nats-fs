package awesomeProjectNATS_fs

import (
	"errors"
	"io"
	"io/fs"
	"os"
	"sync"
	"time"
)

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
	f.fs.lockFile(f.data.ObjectName)
	defer f.fs.unlockFile(f.data.ObjectName)

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
