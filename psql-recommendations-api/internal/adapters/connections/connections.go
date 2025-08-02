package connections

import (
	"context"
	"fmt"
	"psqlRecommendationsApi/cmd/clients"
	"sync"

	config "psqlRecommendationsApi/internal/config/environment"
	discovery_model "psqlRecommendationsApi/internal/model/discovery"
)

var (
	ErrConnectionNotFound   = fmt.Errorf("Connection not found")
	ErrConnectionAlreadySet = fmt.Errorf("Connection already set")
)

type ConnectionProvider interface {
	GetConnection(ctx context.Context, instanceName string) (*clients.CollectorClient, error)
	SetConnection(ctx context.Context, instanceName string) error
}

type Discovery interface {
	GetInstanceInfo(ctx context.Context, instanceName string) (discovery_model.CollectorInstance, error)
}

type Implementation struct {
	storage   map[string]*clients.CollectorClient
	mu        sync.Mutex
	discovery Discovery
}

func New(discovery Discovery) *Implementation {
	return &Implementation{
		storage:   make(map[string]*clients.CollectorClient),
		discovery: discovery,
	}
}

func (i *Implementation) GetConnection(_ context.Context, instanceName string) (*clients.CollectorClient, error) {
	i.mu.Lock()
	defer i.mu.Unlock()

	connection, ok := i.storage[instanceName]
	if !ok {
		return nil, ErrConnectionNotFound
	}

	return connection, nil
}

func (i *Implementation) SetConnection(ctx context.Context, instanceName string) error {
	i.mu.Lock()
	defer i.mu.Unlock()

	_, ok := i.storage[instanceName]
	if ok {
		return ErrConnectionAlreadySet
	}

	collectorInfo, err := i.discovery.GetInstanceInfo(ctx, instanceName)
	if err != nil {
		return fmt.Errorf("discovery.GetCollector: %w", err)
	}

	conn, err := clients.NewCollectorClient(config.CollectorClient{Host: "localhost", Port: collectorInfo.Port})
	if err != nil {
		return fmt.Errorf(" clients.NewCollectorClient: %w", err)
	}

	i.storage[instanceName] = conn

	return nil
}
