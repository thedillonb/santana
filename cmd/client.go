package main

import (
	"context"
	"fmt"
	"log"
	"santana/protocol"
	"time"

	"google.golang.org/grpc"
)

const address = "127.0.0.1:9932"

func main() {
	// Set up a connection to the server.
	conn, err := grpc.Dial(address, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}

	defer conn.Close()
	c := protocol.NewLogClient(conn)

	var index uint64
	for {
		resp, err := c.Read(context.Background(), &protocol.ReadRequest{
			LogName: "analytics",
			Index:   index,
		})
		if err != nil {
			fmt.Printf("Err: %s\n", err.Error())
			time.Sleep(time.Second * 1)
			continue
		}

		fmt.Printf("%s\n", string(resp.Data[0][4:]))
		index++
	}
}
