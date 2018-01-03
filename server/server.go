package server

import (
	"encoding/binary"
	"io"
	"net"
)

type ServerOptions struct {
	address string
}

type Server struct {
	ServerOptions
	listener *net.TCPListener
}

func (s *Server) Listen(address string) error {
	protocolAddr, err := net.ResolveTCPAddr("tcp", s.address)
	if err != nil {
		return err
	}

	l, err := net.ListenTCP("tcp", protocolAddr)
	if err != nil {
		return err
	}

	s.listener = l

	go func() {
		for {
			conn, err := l.Accept()
			if err != nil {
				break
			}

			go s.handleRequest(conn)
		}
	}()

	return nil
}

func (s *Server) Close() error {
	return s.listener.Close()
}

func (s *Server) handleRequest(conn net.Conn) {
	for {
		lenBuf := make([]byte, 4)
		i, err := io.ReadFull(conn, lenBuf)
		if err != nil {
			break
		}
		if i != 4 {
			break
		}

		len := int(binary.BigEndian.Uint32(lenBuf))
		messageBuf := make([]byte, len)
		i, err = io.ReadFull(conn, messageBuf)
		if err != nil {
			break
		}
		if i != len {
			break
		}

	}
}
