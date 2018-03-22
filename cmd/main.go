package main

import (
	"fmt"
	"os"
	"os/signal"
	"path"
	"santana/commitlog"
	"santana/server"
	"syscall"
)

func main() {
	path := path.Join(os.TempDir(), "santana")
	if err := os.MkdirAll(path, 0755); err != nil {
		panic(err)
	}

	fmt.Printf("Path: %s\n", path)

	mgrOpts := commitlog.LogManagerOptions{
		Dir: path,
	}

	mgr, err := commitlog.NewLogManager(mgrOpts)
	if err != nil {
		panic(err)
	}

	logs := mgr.GetLogs()
	dbExists := false

	for _, l := range logs {
		if l.Name == "analytics" {
			dbExists = true
			break
		}
	}

	if !dbExists {
		if _, err := mgr.CreateLog("analytics", commitlog.CommitLogOptions{}); err != nil {
			panic(err)
		}
	}

	fmt.Printf("Loaded logs: %v\n", mgr.GetLogs())

	kafkaServer := server.NewKafkaServer(mgr)
	if err := kafkaServer.Listen(":9092"); err != nil {
		panic(err)
	}

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	<-c

	fmt.Printf("Closing...\n")
	mgr.Close()
}
