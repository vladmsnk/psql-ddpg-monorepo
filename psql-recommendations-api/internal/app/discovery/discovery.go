package discovery

import (
	"context"
	"errors"
	"fmt"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"os"
	model "psqlRecommendationsApi/internal/model/discovery"
	desc "psqlRecommendationsApi/pkg/discovery"
)

type Registrator interface {
	RegisterInstance(ctx context.Context, instanceName string, config []byte) (model.CollectorInstance, error)
	GetInstanceInfo(ctx context.Context, instanceName string) (model.CollectorInstance, error)
}

type Delivery struct {
	desc.DiscoveryServer
	registrator Registrator
}

func New(registrator Registrator) *Delivery {
	return &Delivery{
		registrator: registrator,
	}
}

func (d *Delivery) RegisterInstance(ctx context.Context, req *desc.RegisterInstanceRequest) (*desc.RegisterInstanceResponse, error) {
	instanceName := req.GetInstanceName()
	if instanceName == "" {
		return nil, status.Error(codes.InvalidArgument, "instance_name should not be empty")
	}

	rawYaml, err := os.ReadFile("config/instance1.yaml")
	if err != nil {
		return nil, fmt.Errorf("os.ReadFile: %w", err)
	}

	//config := req.GetConfig()
	//if len(config) == 0 {
	//	return nil, status.Error(codes.InvalidArgument, "config should not be empty")
	//}

	instance, err := d.registrator.RegisterInstance(ctx, instanceName, rawYaml)
	if err != nil {
		if errors.Is(err, model.ErrInstanceAlreadyExists) {
			return nil, status.Error(codes.AlreadyExists, err.Error())
		}
		return nil, fmt.Errorf("registrator.RegisterInstance: %w", err)
	}

	return &desc.RegisterInstanceResponse{
		ContainerId:  instance.Id,
		InstanceName: instance.Name,
		Host:         instance.Host,
		Port:         int64(instance.Port),
	}, nil
}

func (d *Delivery) GetInstanceInfo(ctx context.Context, req *desc.GetInstanceInfoRequest) (*desc.GetInstanceInfoResponse, error) {
	instanceName := req.GetInstanceName()
	if instanceName == "" {
		return nil, status.Error(codes.InvalidArgument, "instance_name should not be empty")
	}

	instanceInfo, err := d.registrator.GetInstanceInfo(ctx, instanceName)
	if err != nil {
		return nil, fmt.Errorf("instanceInfoGetter.GetInstanceInfo: %w", err)
	}

	return &desc.GetInstanceInfoResponse{
		InstanceName: instanceInfo.Name,
		ContainerId:  instanceInfo.Id,
		Host:         instanceInfo.Host,
		Port:         int64(instanceInfo.Port),
	}, nil
}
