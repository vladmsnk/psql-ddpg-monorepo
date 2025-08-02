package setter

import (
	"context"
	"fmt"

	discovery_model "psqlRecommendationsApi/internal/model/discovery"
)

type Setter interface {
	AddInstance(ctx context.Context, instanceName, dbDsn string) (discovery_model.CollectorInstance, error)
}

type Registrator interface {
	RegisterInstance(ctx context.Context, instanceName, dbDsn string) (discovery_model.CollectorInstance, error)
}

type Implementation struct {
	registrator Registrator
}

func (i *Implementation) AddInstance(ctx context.Context, instanceName, dbDsn string) (discovery_model.CollectorInstance, error) {
	instance, err := i.registrator.RegisterInstance(ctx, instanceName, dbDsn)
	if err != nil {
		return discovery_model.CollectorInstance{}, fmt.Errorf("registrator.RegisterInstance: %w", err)
	}
	return instance, nil
}
