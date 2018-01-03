package main

import (
	"fmt"
	"os"
	"path"
	"santana/commitlog"
	"strconv"
	"strings"

	ishell "gopkg.in/abiosoft/ishell.v2"
)

func main() {
	shell := ishell.New()
	path := path.Join(os.TempDir(), "santana")

	commitLogOptions := commitlog.CommitLogOptions{
		Path: path,
	}

	log, err := commitlog.NewCommitLog(commitLogOptions)
	if err != nil {
		panic(err)
	}

	closeLog := func() {
		shell.Printf("Closing log...\n")
		_ = log.Close()
	}

	defer closeLog()

	// b := make([]byte, 1024)
	// n, err := log.ReadAt(b, 1)
	// if err != nil {
	// 	panic(err)
	// }

	// shell.Printf("Read %v bytes: %v\n", n, string(b))

	shell.Printf("Log file: %v\n", path)

	shell.AddCmd(&ishell.Cmd{
		Help: "Add an item into the log",
		Name: "add",
		Func: func(c *ishell.Context) {
			data := []byte(strings.Join(c.Args, " "))
			offset, err := log.Append(data)
			if err != nil {
				c.Err(err)
				return
			}

			c.Printf("Item inserted at %v\n", offset)
		},
	})

	shell.AddCmd(&ishell.Cmd{
		Help: "List all the entries within the log",
		Name: "list",
		Func: func(c *ishell.Context) {
			c.Printf("Going to do the stuff")
		},
	})

	shell.AddCmd(&ishell.Cmd{
		Help: "Read an entry within the log",
		Name: "read",
		Func: func(c *ishell.Context) {
			if len(c.Args) != 1 {
				c.Err(fmt.Errorf("missing arguments"))
				return
			}

			i, err := strconv.Atoi(c.Args[0])
			if err != nil {
				c.Err(err)
				return
			}

			b := make([]byte, 1024)
			n, err := log.ReadAt(b, int64(i))
			if err != nil {
				c.Err(err)
				return
			}

			c.Printf("Read %v bytes: %v\n", n, string(b))
		},
	})

	shell.Interrupt(func(count int, c *ishell.Context) {
		c.Stop()
	})

	shell.Start()
}
