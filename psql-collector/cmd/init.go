package cmd

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"os/signal"
	"postgresHelper/internal/config"
	"postgresHelper/lib/grpc_server"
	desc "postgresHelper/pkg/collector"
	"strconv"
	"syscall"
)

func RunGRPCServer(implementation desc.CollectorServer, cfg *grpc_server.GRPCConfig) (*grpc_server.GRPCServer, error) {
	grpcServer, err := grpc_server.NewGRPCServer(cfg)
	if err != nil {
		return nil, fmt.Errorf("grpc_server.NewGRPCServer: %w", err)
	}

	desc.RegisterCollectorServer(grpcServer.Ser, implementation)
	grpcServer.Run()

	log.Printf("started grpc server at %s:%s", cfg.Host, strconv.Itoa(cfg.Port))
	return grpcServer, nil
}

func CreatePostgresConn(cfg *config.Postgres) (*sql.DB, error) {
	connStr := cfg.ConnectionString()

	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return nil, fmt.Errorf("error opening database: %v", err)
	}

	if err = db.Ping(); err != nil {
		return nil, fmt.Errorf("error pinging database: %v", err)
	}
	_, err = db.Exec("CREATE EXTENSION IF NOT EXISTS pg_stat_statements;")
	if err != nil {
		return nil, fmt.Errorf("db.Exec: %w", err)
	}
	fmt.Println("Extension created successfully")

	log.Println("Successfully connected to postgres database")
	return db, nil
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
