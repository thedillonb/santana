package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"os/signal"
	"santana/protocol"
	"strings"
	"sync"
	"syscall"
	"time"

	"google.golang.org/grpc"
)

var (
	address   = flag.String("address", "127.0.0.1:9932", "the address to connect to")
	logName   = flag.String("log", "analytics", "the log to read from")
	indexFile = flag.String("index", "index.idx", "the index file")

	offsets map[string]int

	offsetLock sync.Mutex
)

func readOffsets(indexFile string) error {
	if _, err := os.Stat(indexFile); !os.IsNotExist(err) {
		data, err := ioutil.ReadFile(indexFile)
		if err != nil {
			return err
		}

		if err := json.Unmarshal(data, &offsets); err != nil {
			return err
		}
	}

	offsets = make(map[string]int)

	return nil
}

func readFromQueue(address string, index int, ch chan<- []string) {
	conn, err := grpc.Dial(address, grpc.WithInsecure())
	if err != nil {
		panic(err)
	}

	defer conn.Close()
	c := protocol.NewLogClient(conn)

	for {
		resp, err := c.Read(context.Background(), &protocol.ReadRequest{
			LogName: *logName,
			Index:   int64(index),
		})

		if err != nil {
			time.Sleep(time.Second * 1)
			continue
		}

		arry := make([]string, 0, len(resp.Data))
		for _, d := range resp.Data {
			str := string(d)

			if !strings.HasPrefix(str, "analytics") {
				arry = append(arry, str)
			}
		}

		ch <- arry

		index = index + len(resp.Data)

		offsetLock.Lock()
		offsets[address] = index
		offsetLock.Unlock()
	}
}

func consumer() chan<- []string {
	client := &http.Client{}
	url := fmt.Sprintf("http://localhost:8086/write?db=%s", *logName)
	c := make(chan []string)

	go func() {
		for r := range c {
			b := strings.Join(r, "\n")
			r := strings.NewReader(b)
			resp, err := client.Post(url, "text/plain", r)
			if err != nil {
				fmt.Printf("Error sending to influx: %s\n", err.Error())
				continue
			}

			if resp.StatusCode != 204 && resp.StatusCode != 200 {
				fmt.Printf("Status code was %v\n", resp.Status)
				continue
			}
		}
	}()

	return c
}

func main() {
	flag.Parse()

	readOffsets(*indexFile)
	addrs := strings.Split(*address, ",")

	ch := consumer()

	for _, addr := range addrs {
		idx, ok := offsets[addr]
		if !ok {
			offsets[addr] = 0
			idx = 0
		}

		go readFromQueue(addr, idx, ch)
	}

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	<-c

	offsetData, err := json.Marshal(offsets)
	if err != nil {
		panic(err)
	}

	ioutil.WriteFile(*indexFile, offsetData, 0777)

	os.Exit(0)
}
