package collector

import (
	"context"
	"fmt"
	"psqlRecommendationsApi/cmd/clients"
	"psqlRecommendationsApi/internal/model"
	desc "psqlRecommendationsApi/pkg/collector"
	"slices"
)

type Adapter interface {
	InitLoad(ctx context.Context) error
	SetKnobs(ctx context.Context, knobs []model.Knob) error
	CollectExternalMetrics(ctx context.Context) (ExternalMetrics, error)
	CollectInternalMetrics(ctx context.Context) ([]InternalMetrics, error)
	CollectKnobs(ctx context.Context) ([]Knob, error)
}

type Implementation struct {
	collectorClient *clients.CollectorClient
}

func New(collectorClient *clients.CollectorClient) *Implementation {
	if collectorClient == nil {
		return &Implementation{}
	}
	return &Implementation{
		collectorClient: collectorClient,
	}
}

func (i *Implementation) InitLoad(ctx context.Context) error {
	_, err := i.collectorClient.Client.InitLoad(ctx, &desc.InitLoadRequest{})
	if err != nil {
		return fmt.Errorf("collectorClient.Client.InitLoad: %w", err)
	}

	return nil
}

func (i *Implementation) CollectKnobs(ctx context.Context) ([]Knob, error) {
	resp, err := i.collectorClient.Client.CollectKnobs(ctx, &desc.CollectKnobsRequest{})
	if err != nil {
		return nil, fmt.Errorf("collectorClient.Client.CollectKnobs: %w", err)
	}
	knobs := resp.GetKnobs()
	modelKnobs := make([]Knob, 0, len(knobs))
	for _, knob := range knobs {
		modelKnobs = append(modelKnobs, Knob{
			Name:   knob.GetName(),
			Value:  float64(knob.GetFloatValue()),
			MaxVal: float64(knob.GetMaxValue()),
			MinVal: float64(knob.GetMinValue()),
		})
	}

	return modelKnobs, nil
}

func (i *Implementation) SetKnobs(ctx context.Context, knobs []model.Knob) error {
	descActions := make([]*desc.SetKnobsRequest_Knob, 0, len(knobs))
	for _, knob := range knobs {
		descActions = append(descActions, &desc.SetKnobsRequest_Knob{
			Name:  knob.Name,
			Value: float32(knob.Value),
		})
	}

	_, err := i.collectorClient.Client.SetKnobs(ctx, &desc.SetKnobsRequest{
		Knobs: descActions,
	})
	if err != nil {
		return fmt.Errorf("collectorClient.Client.SetKnobs: %w", err)
	}

	return nil
}

func (i *Implementation) CollectExternalMetrics(ctx context.Context) (ExternalMetrics, error) {
	resp, err := i.collectorClient.Client.CollectExternalMetrics(ctx, &desc.CollectExternalMetricsRequest{})
	if err != nil {
		return ExternalMetrics{}, fmt.Errorf("collectorClient.Client.CollectExternalMetrics: %w", err)
	}
	return ExternalMetrics{Latency: float64(resp.GetLatency()), Tps: float64(resp.GetTps())}, nil
}

func (i *Implementation) CollectInternalMetrics(ctx context.Context) ([]InternalMetrics, error) {
	resp, err := i.collectorClient.Client.CollectInternalMetrics(ctx, &desc.CollectInternalMetricsRequest{})
	if err != nil {
		return nil, fmt.Errorf("collectorClient.CollectInternalMetrics: %w", err)
	}
	var metrics []InternalMetrics

	for _, descMetric := range resp.GetMetrics() {
		value, ok := descMetric.Value.(*desc.CollectInternalMetricsResponse_Metric_FloatValue)
		if !ok {
			continue
		}
		metrics = append(metrics, InternalMetrics{
			Name:  descMetric.Name,
			Value: float64(value.FloatValue),
		})
	}

	slices.SortFunc(metrics, func(a, b InternalMetrics) int {
		if a.Name > b.Name {
			return 1
		}
		return -1
	})

	return metrics, nil
}
