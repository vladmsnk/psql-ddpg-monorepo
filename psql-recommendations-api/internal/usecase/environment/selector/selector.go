package selector

import (
	"context"
	"errors"
	"fmt"
	"psqlRecommendationsApi/cmd/clients"
	"psqlRecommendationsApi/internal/adapters/collector"
	"psqlRecommendationsApi/internal/adapters/connections"
	"psqlRecommendationsApi/internal/model"
)

type Selector interface {
	ListTrainingMetrics(ctx context.Context, instanceName string) ([]model.TrainingMetric, error)
	ListRewardMetrics(ctx context.Context, instanceName string) (model.ExternalMetrics, error)
	ListKnobs(ctx context.Context, instanceName string) ([]model.Knob, error)
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

func (i *Implementation) ListRewardMetrics(ctx context.Context, instanceName string) (model.ExternalMetrics, error) {
	collectorAdapter, err := i.getCollectorAdapter(ctx, instanceName)
	if err != nil {
		return model.ExternalMetrics{}, fmt.Errorf("getCollectorAdapter: %w", err)
	}

	metrics, err := collectorAdapter.CollectExternalMetrics(ctx)
	if err != nil {
		return model.ExternalMetrics{}, fmt.Errorf("collector.CollectExternalMetrics: %w", err)
	}

	return model.ExternalMetrics{Tps: metrics.Tps, Latency: metrics.Latency}, nil
}

func (i *Implementation) ListTrainingMetrics(ctx context.Context, instanceName string) ([]model.TrainingMetric, error) {
	var res []model.TrainingMetric

	collectorAdapter, err := i.getCollectorAdapter(ctx, instanceName)
	if err != nil {
		return nil, fmt.Errorf("getCollectorAdapter: %w", err)
	}

	internalMetrics, err := collectorAdapter.CollectInternalMetrics(ctx)
	if err != nil {
		return nil, fmt.Errorf("collector.CollectInternalMetrics: %w", err)
	}
	for _, internalMetric := range internalMetrics {
		res = append(res, model.TrainingMetric{Value: float32(internalMetric.Value)})
	}

	return res, nil
}

func (i *Implementation) ListKnobs(ctx context.Context, instanceName string) ([]model.Knob, error) {
	var res []model.Knob

	collectorAdapter, err := i.getCollectorAdapter(ctx, instanceName)
	if err != nil {
		return nil, fmt.Errorf("getCollectorAdapter: %w", err)
	}

	knobs, err := collectorAdapter.CollectKnobs(ctx)
	if err != nil {
		return nil, fmt.Errorf("collector.CollectKnobs: %w", err)
	}
	for _, knob := range knobs {
		res = append(res, model.Knob{Name: knob.Name, Value: knob.Value, MinVal: knob.MinVal, MaxVal: knob.MaxVal})
	}

	return res, nil
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
