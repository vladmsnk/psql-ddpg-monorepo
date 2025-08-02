package grpc_server

import (
	"fmt"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
	"net"
)

type GRPCConfiger interface {
	GetHost() string
	GetPort() int
}

type GRPCServer struct {
	Ser  *grpc.Server
	Addr string
	lis  net.Listener
}

func NewGRPCServer[T GRPCConfiger](cfg T, opts ...grpc.ServerOption) (*GRPCServer, error) {
	c := &GRPCServer{
		Ser:  grpc.NewServer(opts...),
		Addr: fmt.Sprintf("%s:%d", cfg.GetHost(), cfg.GetPort()),
	}

	var err error
	if c.lis, err = net.Listen("tcp", c.Addr); err != nil {
		return nil, err
	}

	reflection.Register(c.Ser)

	return c, nil
}

func (ser *GRPCServer) Run() {
	go func() {
		_ = ser.Ser.Serve(ser.lis)
	}()
}

func (ser *GRPCServer) Close() {
	if ser.lis == nil {
		return
	}
	ser.Ser.GracefulStop()
}
