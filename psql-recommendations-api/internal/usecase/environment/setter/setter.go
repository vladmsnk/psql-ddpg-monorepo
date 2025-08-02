package setter

import (
	"context"
	"errors"
	"fmt"
	"psqlRecommendationsApi/cmd/clients"
	"psqlRecommendationsApi/internal/adapters/collector"
	"psqlRecommendationsApi/internal/adapters/connections"
	"psqlRecommendationsApi/internal/model"
)

type Setter interface {
	SetActions(ctx context.Context, instanceName string, actions []model.Action) error
	InitEnvironment(ctx context.Context, instanceName string) error
}

type ConnectionProvider interface {
	GetConnection(ctx context.Context, instanceName string) (*clients.CollectorClient, error)
	SetConnection(_ context.Context, instanceName string) error
}

type Implementation struct {
	connectionProvider ConnectionProvider
}

func New(connectionProvider ConnectionProvider) *Implementation {
	return &Implementation{
		connectionProvider: connectionProvider,
	}
}

func (i *Implementation) SetActions(ctx context.Context, instanceName string, actions []model.Action) error {
	var knobs []model.Knob
	for _, action := range actions {
		knobs = append(knobs, model.Knob{Name: action.Name, Value: action.Value})
	}

	collectorAdapter, err := i.getCollectorAdapter(ctx, instanceName)
	if err != nil {
		return fmt.Errorf("i.getCollectorAdapter: %w", err)
	}

	err = collectorAdapter.SetKnobs(ctx, knobs)
	if err != nil {
		return fmt.Errorf("setter.SetKnobs: %w", err)
	}

	return nil

}

func (i *Implementation) InitEnvironment(ctx context.Context, instanceName string) error {

	collectorAdapter, err := i.getCollectorAdapter(ctx, instanceName)
	if err != nil {
		return fmt.Errorf("i.getCollectorAdapter: %w", err)
	}

	err = collectorAdapter.InitLoad(ctx)
	if err != nil {
		return fmt.Errorf("setter.InitLoad: %w", err)
	}
	return nil
}

func (i *Implementation) getCollectorAdapter(ctx context.Context, instanceName string) (collector.Adapter, error) {
	connection, err := i.connectionProvider.GetConnection(ctx, instanceName)
	if err != nil {
		if errors.Is(err, connections.ErrConnectionNotFound) {
			err := i.connectionProvider.SetConnection(ctx, instanceName)
			if err != nil {
				return nil, fmt.Errorf("connectionProvider.GetConnection: %w", err)
			}

			connection, err = i.connectionProvider.GetConnection(ctx, instanceName)
			if err != nil {
				return nil, fmt.Errorf("connectionProvider.GetConnection: %w", err)
			}
		} else {
			return nil, fmt.Errorf("connectionProvider.GetConnection: %w", err)
		}
	}

	collectorAdapter := collector.New(connection)
	return collectorAdapter, nil
}
