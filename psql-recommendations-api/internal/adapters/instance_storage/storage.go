package instance_storage

import (
	"context"
	"encoding/json"
	"fmt"

	"psqlRecommendationsApi/cmd/clients"
	model "psqlRecommendationsApi/internal/model/discovery"
)

type Adapter interface {
	CheckInstanceExists(ctx context.Context, instanceName string) (bool, error)
	SaveInstance(ctx context.Context, instance model.CollectorInstance) error
	GetInstance(ctx context.Context, instanceName string) (model.CollectorInstance, error)
}

type Implementation struct {
	redisClient *clients.RedisClient
}

func New(redisClient *clients.RedisClient) *Implementation {
	return &Implementation{
		redisClient: redisClient,
	}
}

func (i *Implementation) CheckInstanceExists(ctx context.Context, instanceName string) (bool, error) {

	i.redisClient.Client.Del(ctx, instanceName)

	res, err := i.redisClient.Client.Exists(ctx, instanceName).Result()
	if err != nil {
		return false, fmt.Errorf("redisClient.Client.Exists: %w", err)
	}

	return res > 0, nil
}

func (i Implementation) SaveInstance(ctx context.Context, instance model.CollectorInstance) error {
	jsonStr, err := json.Marshal(instance)
	if err != nil {
		return fmt.Errorf("json.Marshal: %w", err)
	}

	status := i.redisClient.Client.Set(ctx, instance.Name, jsonStr, 0)
	if status.Err() != nil {
		return status.Err()
	}

	return nil
}

func (i *Implementation) GetInstance(ctx context.Context, instanceName string) (model.CollectorInstance, error) {
	result, err := i.redisClient.Client.Get(ctx, instanceName).Result()
	if err != nil {
		return model.CollectorInstance{}, fmt.Errorf("redisClient.Client.Get: %w", err)
	}

	var instance model.CollectorInstance

	err = json.Unmarshal([]byte(result), &instance)
	if err != nil {
		return model.CollectorInstance{}, fmt.Errorf("json.Unmarshal: %w", err)
	}

	return instance, nil
}
