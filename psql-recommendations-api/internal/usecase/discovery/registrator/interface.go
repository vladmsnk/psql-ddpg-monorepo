package registrator

import (
	"context"
	model "psqlRecommendationsApi/internal/model/discovery"
)

type Registrator interface {
	RegisterInstance(ctx context.Context, instanceName string, config []byte) (model.CollectorInstance, error)
	GetInstanceInfo(ctx context.Context, instanceName string) (model.CollectorInstance, error)
}
