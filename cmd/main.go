package main

import (
	"fmt"
	"os"
	"path"
	"santana/commitlog"
	"strings"

	ishell "gopkg.in/abiosoft/ishell.v2"
)

func main() {
	shell := ishell.New()

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

	fmt.Printf("Loaded logs: %v\n", mgr.GetLogs())

	closeLog := func() {
		shell.Printf("Closing log...\n")
		mgr.Close()
	}

	defer closeLog()

	shell.AddCmd(&ishell.Cmd{
		Help: "Create a log",
		Name: "create",
		Func: func(c *ishell.Context) {
			name := strings.Join(c.Args, " ")
			opts := commitlog.CommitLogOptions{
				LogMaxBytes: 25,
			}

			if _, err := mgr.CreateLog(name, opts); err != nil {
				c.Err(err)
				return
			}

			c.Printf("Log %s created!\n", name)
		},
	})

	shell.AddCmd(&ishell.Cmd{
		Help: "Add an item into the log",
		Name: "add",
		Func: func(c *ishell.Context) {
			log := c.Args[0]
			data := []byte(strings.Join(c.Args[1:], " "))

			l, err := mgr.GetLog(log)
			if err != nil {
				c.Err(err)
				return
			}

			offset, err := l.Append(data)
			if err != nil {
				c.Err(err)
				return
			}

			c.Printf("Item inserted at %v\n", offset)
		},
	})

	shell.AddCmd(&ishell.Cmd{
		Help: "List all the logs",
		Name: "list",
		Func: func(c *ishell.Context) {
			for _, l := range mgr.GetLogs() {
				c.Printf("%s\n", l)
			}
		},
	})

	shell.AddCmd(&ishell.Cmd{
		Help: "Read all entries from a log",
		Name: "readall",
		Func: func(c *ishell.Context) {
			log := c.Args[0]
			l, err := mgr.GetLog(log)
			if err != nil {
				c.Err(err)
				return
			}

			for i := 0; ; i++ {
				b := make([]byte, 1024)
				n, err := l.ReadAt(b, int64(i))
				if err != nil {
					c.Err(err)
					return
				}

				c.Printf("Read %v bytes: %v\n", n-4, string(b[4:]))
			}
		}})

	shell.AddCmd(&ishell.Cmd{
		Help: "Read an entry within the log",
		Name: "read",
		Func: func(c *ishell.Context) {
			// if len(c.Args) != 1 {
			// 	c.Err(fmt.Errorf("missing arguments"))
			// 	return
			// }

			// i, err := strconv.Atoi(c.Args[0])
			// if err != nil {
			// 	c.Err(err)
			// 	return
			// }

			// b := make([]byte, 1024)
			// n, err := log.ReadAt(b, int64(i))
			// if err != nil {
			// 	c.Err(err)
			// 	return
			// }

			// c.Printf("Read %v bytes: %v\n", n, string(b))
		},
	})

	shell.Interrupt(func(count int, c *ishell.Context) {
		c.Stop()
	})

	shell.Start()
}
