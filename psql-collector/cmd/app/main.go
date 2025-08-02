package main

import (
	_ "github.com/lib/pq"
	"log"
	"os"
	"postgresHelper/cmd"
	psql_helper "postgresHelper/internal/app/psql-helper"
	"postgresHelper/internal/collector"
	"postgresHelper/internal/config"
	"postgresHelper/internal/pgbench"
	"postgresHelper/internal/usecase/loader"
	"postgresHelper/internal/usecase/selector"
	"postgresHelper/internal/usecase/setter"
)

func main() {
	defer func() {
		recover()
	}()

	err := config.Init()
	if err != nil {
		log.Fatal(err)
	}

	conn, err := cmd.CreatePostgresConn(&config.ConfigStruct.PG)
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	collect := collector.NewCollector(conn)

	benchLoader := loader.New(pgbench.New(conn, config.ConfigStruct))
	metricsSelector := selector.New(collect, config.ConfigStruct.PG)
	knobsSetter := setter.New(collect)

	delivery := psql_helper.New(metricsSelector, benchLoader, knobsSetter)
	grpcServer, err := cmd.RunGRPCServer(delivery, &config.ConfigStruct.GRPC)
	if err != nil {
		log.Fatal(err)
	}
	defer grpcServer.Close()

	cmd.Lock(make(chan os.Signal, 1))
}
