package server

import (
	"bufio"
	"net"
	"santana/commitlog"
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

	go func() {
		for {
			conn, err := s.listener.Accept()
			if err != nil {
				return
			}

			go s.Read(conn)
		}
	}()

	return nil
}

func (s *LineServer) Read(conn net.Conn) error {
	reader := bufio.NewReader(conn)
	defer conn.Close()

	for {
		line, _, err := reader.ReadLine()
		if err != nil {
			return err
		}

		l, err := s.LogManager.GetLog("analytics")
		if err != nil {
			return err
		}

		l.Append([][]byte{line})
	}
}

func (s *LineServer) Close() error {
	if s.listener == nil {
		return nil
	}

	return s.listener.Close()
}
