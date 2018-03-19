package server

import (
	"bufio"
	"net"
	"santana/commitlog"
	"time"
)

type LineServer struct {
	LogManager *commitlog.LogManager
	listener   *net.TCPListener
}

func (s *LineServer) Listen(address string) error {
	protocolAddr, err := net.ResolveTCPAddr("tcp", address)
	if err != nil {
		return err
	}

	if s.listener, err = net.ListenTCP("tcp", protocolAddr); err != nil {
		return err
	}

	log, err := s.LogManager.GetLog("analytics")
	if err != nil {
		return err
	}

	ch := make(chan []byte, 100)
	closed := make(chan error, 1)

	go func() {
		for {
			conn, err := s.listener.Accept()
			if err != nil {
				closed <- err
				return
			}

			go s.read(conn, ch)
		}
	}()

	go func() {
		n := 0
		readBuff := make([][]byte, 50)
		var timer *time.Timer
		var timeout <-chan time.Time

		for {
			select {
			case d := <-ch:
				readBuff[n] = d
				n++

				if n == 1 {
					timer = time.NewTimer(500 * time.Millisecond)
					timeout = timer.C
				}

				if n == len(readBuff) {
					log.Append(readBuff)
					timer.Stop()
					n = 0
				}

			case <-timeout:
				if n > 0 {
					log.Append(readBuff[:n])
					timer.Stop()
					n = 0
				}

			case <-closed:
				return
			}
		}
	}()

	return nil
}

func (s *LineServer) read(conn net.Conn, data chan []byte) error {
	reader := bufio.NewReader(conn)
	defer conn.Close()

	for {
		line, _, err := reader.ReadLine()
		if err != nil {
			return err
		}

		buffer := make([]byte, len(line))
		copy(buffer, line)
		data <- buffer
	}
}

func (s *LineServer) Close() error {
	if s.listener == nil {
		return nil
	}

	return s.listener.Close()
}
