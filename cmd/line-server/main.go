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

	lineServer := server.LineServer{LogManager: mgr}
	if err := lineServer.Listen(":9931"); err != nil {
		panic(err)
	}

	rpcServer := server.NewServer(server.ServerOptions{LogManager: mgr})
	if err := rpcServer.Listen(":9932"); err != nil {
		panic(err)
	}

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	<-c

	fmt.Printf("Closing...\n")
	lineServer.Close()
	rpcServer.Close()
	mgr.Close()
}
