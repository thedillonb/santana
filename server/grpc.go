package server

import (
	"context"
	"net"
	"santana/commitlog"
	"santana/protocol"

	"google.golang.org/grpc/codes"

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

	return &protocol.AppendReply{Index: off}, nil
}

func (s *handler) Read(ctx context.Context, req *protocol.ReadRequest) (*protocol.ReadReply, error) {
	l, err := s.mgr.GetLog(req.LogName)
	if err != nil {
		return nil, err
	}

	bufSize := req.MaxBytes
	if bufSize == 0 {
		bufSize = 1024 * 8
	}

	b := make([]byte, bufSize)
	start := 0
	var data [][]byte

	for {
		n, err := l.ReadAt(b[start:], int64(req.Index))
		if err == commitlog.ErrOffsetOutOfRange {
			break
		}
		if err == commitlog.ErrBufferToSmall {
			break
		}
		if err != nil {
			return nil, err
		}

		data = append(data, b[start+4:start+n])
		start = start + n
	}

	if len(data) == 0 {
		if err == commitlog.ErrOffsetOutOfRange {
			return nil, grpc.Errorf(codes.OutOfRange, err.Error())
		}
		if err == commitlog.ErrBufferToSmall {
			return nil, grpc.Errorf(codes.Unavailable, err.Error())
		}
	}

	return &protocol.ReadReply{Data: data}, nil
}

func (s *handler) ListLogs(ctx context.Context, req *protocol.ListLogsRequest) (*protocol.ListLogsReply, error) {
	var logs []*protocol.ListLogsReply_Log

	for _, l := range s.mgr.GetLogs() {
		logs = append(logs, &protocol.ListLogsReply_Log{
			Name:      l.Name,
			MaxOffset: l.MaxOffset,
			MinOffset: l.MinOffset,
			Retention: l.Retention,
		})
	}

	return &protocol.ListLogsReply{Logs: logs}, nil
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

func NewServer(opts ServerOptions) *Server {
	return &Server{ServerOptions: opts}
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
