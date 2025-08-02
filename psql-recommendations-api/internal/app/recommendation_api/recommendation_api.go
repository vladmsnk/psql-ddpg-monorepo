package recommendation_api

import (
	"context"
	"fmt"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	discovery_model "psqlRecommendationsApi/internal/model/discovery"
	desc "psqlRecommendationsApi/pkg/recommendations_api"
)

type Delivery struct {
	desc.RecommendationsAPIServer
	setter Setter
}

type Setter interface {
	AddInstance(ctx context.Context, instanceName, dbDsn string) (discovery_model.CollectorInstance, error)
}

func New(setter Setter) *Delivery {
	return &Delivery{
		setter: setter,
	}
}

func (d *Delivery) AddInstance(ctx context.Context, req *desc.AddInstanceRequest) (*desc.AddInstanceResponse, error) {
	instanceName := req.GetInstanceName()
	if instanceName == "" {
		return nil, status.Error(codes.InvalidArgument, "instance_name should not be empty")
	}

	dbDsn := req.GetDbDsn()
	if dbDsn == "" {
		return nil, status.Error(codes.InvalidArgument, "db_dsn should not be empty")
	}

	instance, err := d.setter.AddInstance(ctx, instanceName, dbDsn)
	if err != nil {
		return nil, fmt.Errorf("setter.AddInstance: %w", err)
	}

	return &desc.AddInstanceResponse{
		InstanceName: instance.Name,
		Id:           instance.Id,
		Host:         instance.Host,
		Port:         int64(instance.Port),
		Status:       toDescStatus(instance.Status),
	}, nil
}

func toDescStatus(instanceStatus discovery_model.InstanceStatus) desc.Status {
	switch instanceStatus {
	case discovery_model.InstanceStatusNew:
		return desc.Status_New
	default:
		return desc.Status_Unspecified
	}
}
