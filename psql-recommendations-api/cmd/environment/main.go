package main

import (
	"log"
	"os"
	"psqlRecommendationsApi/cmd"
	"psqlRecommendationsApi/cmd/clients"
	"psqlRecommendationsApi/internal/adapters/connections"
	discovery_adapter "psqlRecommendationsApi/internal/adapters/discovery"
	"psqlRecommendationsApi/internal/app/environment"
	config "psqlRecommendationsApi/internal/config/environment"
	"psqlRecommendationsApi/internal/usecase/environment/selector"
	"psqlRecommendationsApi/internal/usecase/environment/setter"
)

func main() {
	if err := config.Init(); err != nil {
		log.Fatal(err)
	}
	discoveryClient, err := clients.NewDiscoveryClient(config.ConfigStruct.DiscoveryClient)
	if err != nil {
		log.Fatal(err)
	}

	var (
		discovery          = discovery_adapter.New(discoveryClient)
		connectionProvider = connections.New(discovery)
		metricsSelector    = selector.New(connectionProvider)
		metricsSetter      = setter.New(connectionProvider)
		app                = environment.New(metricsSelector, metricsSetter)
	)

	grpcServer, err := cmd.RunEnvironmentGRPCServer(app, config.ConfigStruct.EnvironmentGRPCServer)
	if err != nil {
		log.Fatal(err)
	}
	defer grpcServer.Close()

	cmd.Lock(make(chan os.Signal, 1))
}
