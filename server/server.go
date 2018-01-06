package server

import (
	"context"
	"net"
	"santana/commitlog"
	"santana/protocol"

	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

type ServerOptions struct {
	LogManager *commitlog.LogManager
}

type Server struct {
	ServerOptions
	listener  *net.TCPListener
	rpcServer *grpc.Server
}

type handler struct {
	mgr *commitlog.LogManager
}

func (s *handler) Append(ctx context.Context, req *protocol.AppendRequest) (*protocol.AppendReply, error) {
	l, err := s.mgr.GetLog(req.LogName)
	if err != nil {
		return nil, err
	}

	off, err := l.Append(req.Data)
	if err != nil {
		return nil, err
	}

	return &protocol.AppendReply{Index: uint64(off)}, nil
}

func (s *handler) Read(ctx context.Context, req *protocol.ReadRequest) (*protocol.ReadReply, error) {
	return nil, nil
}

func (s *handler) ListLogs(ctx context.Context, req *protocol.ListLogsRequest) (*protocol.ListLogsReply, error) {
	logs := s.mgr.GetLogs()
	return &protocol.ListLogsReply{Names: logs}, nil
}

func (s *handler) CreateLog(ctx context.Context, req *protocol.CreateLogRequest) (*protocol.CreateLogReply, error) {
	opts := commitlog.CommitLogOptions{}

	if _, err := s.mgr.CreateLog(req.Name, opts); err != nil {
		return nil, err
	}

	return &protocol.CreateLogReply{}, nil
}

func (s *handler) DeleteLog(ctx context.Context, req *protocol.DeleteLogRequest) (*protocol.DeleteLogResponse, error) {
	if err := s.mgr.DeleteLog(req.Name); err != nil {
		return nil, err
	}

	return &protocol.DeleteLogResponse{}, nil
}

func NewServer(opts ServerOptions) (*Server, error) {
	return &Server{ServerOptions: opts}, nil
}

func (s *Server) Listen(address string) error {
	protocolAddr, err := net.ResolveTCPAddr("tcp", address)
	if err != nil {
		return err
	}

	l, err := net.ListenTCP("tcp", protocolAddr)
	if err != nil {
		return err
	}

	s.listener = l
	s.rpcServer = grpc.NewServer()
	protocol.RegisterLogServer(s.rpcServer, &handler{s.LogManager})
	reflection.Register(s.rpcServer)

	go s.rpcServer.Serve(l)
	return nil
}

func (s *Server) Close() error {
	s.rpcServer.Stop()
	return s.listener.Close()
}
