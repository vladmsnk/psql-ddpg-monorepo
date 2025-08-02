package clients

import (
	"fmt"

	docker_client "github.com/docker/docker/client"
	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	discovery_config "psqlRecommendationsApi/internal/config/discovery"
	env_config "psqlRecommendationsApi/internal/config/environment"
	pb_collector "psqlRecommendationsApi/pkg/collector"
	pb_discovery "psqlRecommendationsApi/pkg/discovery"
	pb_redommendation "psqlRecommendationsApi/pkg/recommendations_api"
)

type CollectorClient struct {
	Client pb_collector.CollectorClient
	Conn   *grpc.ClientConn
}

type RecommendationApiClient struct {
	Client pb_redommendation.RecommendationsAPIClient
	Conn   *grpc.ClientConn
}

type DiscoveryClient struct {
	Client pb_discovery.DiscoveryClient
	Conn   *grpc.ClientConn
}

type DockerClient struct {
	Client *docker_client.Client
}

type RedisClient struct {
	Client *redis.Client
}

func (cc *CollectorClient) Close() {
	if cc.Conn != nil {
		cc.Conn.Close()
	}
}
func (cc *DiscoveryClient) Close() {
	if cc.Conn != nil {
		cc.Conn.Close()
	}
}

func (cc *DockerClient) Close() {
	if cc.Client != nil {
		cc.Client.Close()
	}
}

func (cc *RedisClient) Close() {
	if cc.Client != nil {
		cc.Client.Close()
	}
}

func NewDockerClient() (*DockerClient, error) {
	client, err := docker_client.NewClientWithOpts(docker_client.FromEnv, docker_client.WithAPIVersionNegotiation())
	if err != nil {
		return nil, fmt.Errorf("docker_client.NewClientWithOpts: %w", err)
	}
	return &DockerClient{Client: client}, nil
}

func NewCollectorClient(config env_config.CollectorClient) (*CollectorClient, error) {
	conn, err := grpc.NewClient(fmt.Sprintf("%s:%d", config.Host, config.Port), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("grpc.NewClient: %w", err)
	}
	client := pb_collector.NewCollectorClient(conn)
	return &CollectorClient{Client: client, Conn: conn}, err
}

func NewDiscoveryClient(config env_config.DiscoveryClient) (*DiscoveryClient, error) {
	conn, err := grpc.NewClient(fmt.Sprintf("%s:%d", config.Host, config.Port), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("grpc.NewClient: %w", err)
	}
	client := pb_discovery.NewDiscoveryClient(conn)
	return &DiscoveryClient{Client: client, Conn: conn}, nil
}

func NewRedisClient(config discovery_config.Redis) (*RedisClient, error) {
	redisClient := redis.NewClient(&redis.Options{
		Addr:     fmt.Sprintf("%s:%d", config.Host, config.Port),
		Password: "",
		DB:       0,
	})

	return &RedisClient{Client: redisClient}, nil
}
