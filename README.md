# nats-fs [![Test](https://github.com/inovacc/nats-fs/actions/workflows/test.yml/badge.svg)](https://github.com/inovacc/nats-fs/actions/workflows/test.yml)

A virtual file system backed by NATS JetStream Object Store, mountable via FUSE and accessible using a standard `os.File`-like API.

> "Don't communicate by sharing memory, share memory by communicating." — Go Proverbs

## Features
- Implements `os.FileInfo` and `os.FS`-like interfaces
- Full support for `Create`, `Open`, `Write`, `Read`, `Rename`, `Remove`, `Stat`, `Chmod`, `Chtimes`
- Memory and disk-backed NATS object store modes
- Embeddable with FUSE using [go-fuse](https://github.com/hanwen/go-fuse)
- Full integration test suite with an embedded NATS server

## Installation

```
go get github.com/inovacc/natsfs
```

## Example Usage

### Mount NATS-FS with FUSE
```go
func main() {
    nc, _ := nats.Connect(nats.DefaultURL)
    js, _ := nc.JetStream()

    fsys, _ := natsfs.NewNatsFs(js, natsfs.Config{
        Bucket: "fuse-natsfs",
        Storage: natsfs.MemoryStorage,
    })

    mountPoint := filepath.Join(os.Getenv("HOME"), "mnt", "natsfs")
    _ = os.MkdirAll(mountPoint, 0755)
    _ = natsfs.MountNatsFs(mountPoint, fsys.(*natsfs.NatsFs))
}
```

### File Operations
```go
f, _ := fsys.Create("hello.txt")
f.WriteString("Hello NATS-FS!")
f.Close()

f2, _ := fsys.Open("hello.txt")
buf := make([]byte, 16)
f2.Read(buf)
fmt.Println(string(buf))
```

## Testing

```
go test -v ./...        # Run unit tests with embedded NATS server
go test -bench=.        # Benchmark file write/read operations
```

## TODO
- Directory support via `fs.NodeReaddir`
- File permission and ownership emulation
- Support symbolic links and hard links
- FUSE mount integration tests

## License
MIT
