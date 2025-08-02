package cmd

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"syscall"

	"psqlRecommendationsApi/lib/grpc_server"
	desc_discovery "psqlRecommendationsApi/pkg/discovery"
	desc_environment "psqlRecommendationsApi/pkg/environment"
)

func RunEnvironmentGRPCServer(implementation desc_environment.EnvironmentServer, cfg GRPCConfigEnvironment) (*grpc_server.GRPCServer, error) {
	grpcServer, err := grpc_server.NewGRPCServer(cfg)
	if err != nil {
		return nil, fmt.Errorf("grpc_server.NewGRPCServer: %w", err)
	}

	desc_environment.RegisterEnvironmentServer(grpcServer.Ser, implementation)
	grpcServer.Run()

	log.Printf("started grpc server at %s:%s", cfg.Host, strconv.Itoa(cfg.Port))
	return grpcServer, nil
}

func RunDiscoveryRPCServer(implementation desc_discovery.DiscoveryServer, cfg GRPCConfigDiscovery) (*grpc_server.GRPCServer, error) {
	grpcServer, err := grpc_server.NewGRPCServer(cfg)
	if err != nil {
		return nil, fmt.Errorf("grpc_server.NewGRPCServer: %w", err)
	}

	desc_discovery.RegisterDiscoveryServer(grpcServer.Ser, implementation)
	grpcServer.Run()

	log.Printf("started grpc server at %s:%s", cfg.Host, strconv.Itoa(cfg.Port))
	return grpcServer, nil
}

func (g GRPCConfigEnvironment) GetHost() string { return g.Host }
func (g GRPCConfigEnvironment) GetPort() int    { return g.Port }

type GRPCConfigEnvironment struct {
	Host string `yaml:"host"`
	Port int    `yaml:"port"`
}

func (g GRPCConfigDiscovery) GetHost() string { return g.Host }
func (g GRPCConfigDiscovery) GetPort() int    { return g.Port }

type GRPCConfigDiscovery struct {
	Host string `yaml:"host"`
	Port int    `yaml:"port"`
}

func Lock(ch chan os.Signal) {
	defer func() {
		ch <- os.Interrupt
	}()
	signal.Notify(ch,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT,
	)
	<-ch
}
