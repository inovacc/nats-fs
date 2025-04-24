package main

import (
	"log"
	"os"
	"path/filepath"

	"awesomeProjectNATS_fs/internal/natsfs"
	"github.com/nats-io/nats.go"
)

func main() {
	nc, err := nats.Connect(nats.DefaultURL)
	if err != nil {
		log.Fatal(err)
	}
	defer nc.Close()

	js, err := nc.JetStream()
	if err != nil {
		log.Fatal(err)
	}

	nfs, err := natsfs.NewNatsFs(js, natsfs.Config{
		Bucket:      "fuse-natsfs",
		Description: "mounted via go-fuse",
		Storage:     natsfs.MemoryStorage,
	})
	if err != nil {
		log.Fatal(err)
	}

	mountPath := filepath.Join(os.Getenv("HOME"), "mnt", "natsfs")
	if _, err := os.Stat(mountPath); err == nil {
		if err := os.MkdirAll(mountPath, 0755); err != nil {
			log.Fatal(err)
		}

		if err := os.Chown(mountPath, os.Getuid(), os.Getgid()); err != nil {
			log.Fatal(err)
		}
	}

	if err := natsfs.MountNatsFs(mountPath, nfs.(*natsfs.NatsFs)); err != nil {
		log.Fatal(err)
	}
}
