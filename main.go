package main

import (
	"log"

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

	if err := natsfs.MountNatsFs("/mnt/natsfs", nfs.(*natsfs.NatsFs)); err != nil {
		log.Fatal(err)
	}
}
