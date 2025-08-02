package discovery

import (
	"context"
	"fmt"
	"psqlRecommendationsApi/cmd/clients"
	model "psqlRecommendationsApi/internal/model/discovery"
	desc "psqlRecommendationsApi/pkg/discovery"
)

type Adapter interface {
	RegisterInstance(ctx context.Context, instanceName string, config []byte) (model.CollectorInstance, error)
	GetInstanceInfo(ctx context.Context, instanceName string) (model.CollectorInstance, error)
}

type Implementation struct {
	client *clients.DiscoveryClient
}

func New(client *clients.DiscoveryClient) *Implementation {
	return &Implementation{
		client: client,
	}
}

func (i *Implementation) RegisterInstance(ctx context.Context, instanceName string, config []byte) (model.CollectorInstance, error) {
	instance, err := i.client.Client.RegisterInstance(ctx, &desc.RegisterInstanceRequest{
		InstanceName: instanceName,
		Config:       config,
	})
	if err != nil {
		return model.CollectorInstance{}, fmt.Errorf("client.GetInstanceInfo: %w", err)
	}

	return model.CollectorInstance{
		Id:   instance.ContainerId,
		Name: instance.InstanceName,
		Host: instance.Host,
		Port: int(instance.Port),
	}, nil
}

func (i *Implementation) GetInstanceInfo(ctx context.Context, instanceName string) (model.CollectorInstance, error) {
	instance, err := i.client.Client.GetInstanceInfo(ctx, &desc.GetInstanceInfoRequest{
		InstanceName: instanceName,
	})
	if err != nil {
		return model.CollectorInstance{}, fmt.Errorf("client.GetInstanceInfo: %w", err)
	}

	return model.CollectorInstance{
		Id:   instance.ContainerId,
		Name: instance.InstanceName,
		Host: instance.Host,
		Port: int(instance.Port),
	}, nil
}
