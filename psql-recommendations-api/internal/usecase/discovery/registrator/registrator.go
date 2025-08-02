package registrator

import (
	"context"
	"fmt"

	model "psqlRecommendationsApi/internal/model/discovery"
)

type Storage interface {
	CheckInstanceExists(ctx context.Context, instanceName string) (bool, error)
	SaveInstance(ctx context.Context, instance model.CollectorInstance) error
	GetInstance(ctx context.Context, instanceName string) (model.CollectorInstance, error)
}

type InstanceCreator interface {
	CreateInstance(ctx context.Context, instanceName string, config []byte) (model.CollectorInstance, error)
}

type Implementation struct {
	storage         Storage
	instanceCreator InstanceCreator
}

func New(storage Storage, instanceCreator InstanceCreator) *Implementation {
	return &Implementation{
		storage:         storage,
		instanceCreator: instanceCreator,
	}
}

func (i *Implementation) RegisterInstance(ctx context.Context, instanceName string, config []byte) (model.CollectorInstance, error) {

	exists, err := i.storage.CheckInstanceExists(ctx, instanceName)
	if err != nil {
		return model.CollectorInstance{}, fmt.Errorf("storage.CheckInstanceExists: %w", err)
	}
	if exists {
		return model.CollectorInstance{}, model.ErrInstanceAlreadyExists
	}

	instance, err := i.instanceCreator.CreateInstance(ctx, instanceName, config)
	if err != nil {
		return model.CollectorInstance{}, fmt.Errorf("instanceCreator.CreateInstance: %w", err)
	}

	err = i.storage.SaveInstance(ctx, instance)
	if err != nil {
		return model.CollectorInstance{}, fmt.Errorf("storage.SaveInstance: %w", err)
	}

	return instance, nil
}

func (i *Implementation) GetInstanceInfo(ctx context.Context, instanceName string) (model.CollectorInstance, error) {
	instance, err := i.storage.GetInstance(ctx, instanceName)
	if err != nil {
		return model.CollectorInstance{}, fmt.Errorf("storage.GetInstance: %w", err)
	}

	return instance, nil
}
