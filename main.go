package main

import (
	"awesomeProjectNATS_fs/internal/natsfs"
	"github.com/nats-io/nats.go"
)

func main() {
	nc, err := nats.Connect(nats.DefaultURL)
	if err != nil {
		panic(err)
	}
	defer nc.Close()

	js, err := nc.JetStream()
	if err != nil {
		panic(err)
	}

	newNatsFs, err := natsfs.NewNatsFs()
}
