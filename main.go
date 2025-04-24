package main

import (
	"log"

	"awesomeProjectNATS_fs/internal/natsfs"
	"github.com/inovacc/utils/v2/uid"
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

	config := natsfs.Config{
		ConnectionID: uid.GenerateUUID(),
		Bucket:       "test",
	}

	nfs, err := natsfs.NewNatsFs(js, config)
	if err != nil {
		log.Fatal(err)
	}

	file, err := nfs.Create("file.txt")
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	_, err = file.Write([]byte("Hello World"))
}
