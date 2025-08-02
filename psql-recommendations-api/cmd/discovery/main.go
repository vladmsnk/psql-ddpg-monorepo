package main

import (
	"log"
	"os"
	"psqlRecommendationsApi/cmd"
	"psqlRecommendationsApi/cmd/clients"
	"psqlRecommendationsApi/internal/adapters/docker"
	"psqlRecommendationsApi/internal/adapters/instance_storage"
	"psqlRecommendationsApi/internal/app/discovery"
	config "psqlRecommendationsApi/internal/config/discovery"
	"psqlRecommendationsApi/internal/usecase/discovery/registrator"
)

func main() {
	if err := config.Init(); err != nil {
		log.Fatal(err)
	}

	redisClient, err := clients.NewRedisClient(config.ConfigStruct.Redis)
	if err != nil {
		log.Fatal(err)
	}
	defer redisClient.Close()

	dockerClient, err := clients.NewDockerClient()
	if err != nil {
		log.Fatal(err)
	}

	var (
		storage         = instance_storage.New(redisClient)
		instanceCreator = docker.New(dockerClient)
		reg             = registrator.New(storage, instanceCreator)
		app             = discovery.New(reg)
	)

	grpcServer, err := cmd.RunDiscoveryRPCServer(app, config.ConfigStruct.DiscoveryGRPCServer)
	if err != nil {
		log.Fatal(err)
	}

	defer grpcServer.Close()

	cmd.Lock(make(chan os.Signal, 1))
}
