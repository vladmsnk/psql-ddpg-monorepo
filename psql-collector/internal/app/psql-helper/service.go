package psql_helper

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/samber/lo"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"postgresHelper/internal/model"
	desc "postgresHelper/pkg/collector"
	"reflect"
	"time"
)

type Delivery struct {
	desc.CollectorServer
	selector Selector
	loader   Loader
	setter   Setter
}

func New(selector Selector, loader Loader, setter Setter) *Delivery {
	return &Delivery{
		selector: selector,
		loader:   loader,
		setter:   setter,
	}
}

type Loader interface {
	RunLoad(ctx context.Context) (<-chan model.ExternalMetric, <-chan error)
	InitLoad(ctx context.Context) error
}

type Selector interface {
	ListAllMetrics(ctx context.Context) ([]model.InternalMetric, error)
	ListAllAggregatedMetrics(ctx context.Context) ([]model.InternalMetric, error)
	ListKnobs(ctx context.Context) ([]model.Knob, error)
}

type Setter interface {
	SetKnobs(ctx context.Context, knobs []model.Knob) error
}

func (d *Delivery) CollectKnobs(ctx context.Context, _ *desc.CollectKnobsRequest) (*desc.CollectKnobsResponse, error) {
	knobs, err := d.selector.ListKnobs(ctx)
	if err != nil {
		return nil, fmt.Errorf("selector.ListKnobs: %w", err)
	}

	return toDescKnobs(knobs)
}

func toDescKnobs(knobs []model.Knob) (*desc.CollectKnobsResponse, error) {
	var err error

	descKnobs := lo.Map(knobs, func(knob model.Knob, _ int) *desc.CollectKnobsResponse_Knob {
		protoKnob := &desc.CollectKnobsResponse_Knob{
			Name: knob.Name,
		}

		switch v := knob.Value.(type) {
		case string:
			protoKnob.Value = &desc.CollectKnobsResponse_Knob_StrValue{StrValue: v}
		case float64:
			protoKnob.Value = &desc.CollectKnobsResponse_Knob_FloatValue{FloatValue: float32(v)}
			protoKnob.MinValue = float32(knob.MinVal.(float64))
			protoKnob.MaxValue = float32(knob.MaxVal.(float64))
		case bool:
			protoKnob.Value = &desc.CollectKnobsResponse_Knob_BoolValue{BoolValue: v}
		default:
			err = fmt.Errorf("Unsupported type for knob %s: %v\n", knob.Name, reflect.TypeOf(knob.Value))
		}
		return protoKnob
	})
	if err != nil {
		return nil, err
	}

	return &desc.CollectKnobsResponse{Knobs: descKnobs}, nil
}

func (d *Delivery) InitLoad(ctx context.Context, _ *desc.InitLoadRequest) (*desc.InitLoadResponse, error) {
	err := d.loader.InitLoad(ctx)
	if err != nil {
		return nil, fmt.Errorf("loader.InitLoad: %w", err)
	}
	return &desc.InitLoadResponse{}, nil
}

func (d *Delivery) SetKnobs(ctx context.Context, req *desc.SetKnobsRequest) (*desc.SetKnobsResponse, error) {
	knobs := req.GetKnobs()
	if len(knobs) == 0 {
		return nil, status.Error(codes.InvalidArgument, "knobs should be specified")
	}

	var foundEmptyKnob bool
	lo.ForEach(knobs, func(knob *desc.SetKnobsRequest_Knob, _ int) {
		if knob.Name == "" {
			foundEmptyKnob = true
		}
	})
	if foundEmptyKnob {
		return nil, status.Error(codes.InvalidArgument, "knob name should be specified")
	}

	modelKnobs := lo.Map(knobs, func(knob *desc.SetKnobsRequest_Knob, index int) model.Knob {
		return model.Knob{
			Name:  knob.Name,
			Value: knob.Value,
		}
	})

	err := d.setter.SetKnobs(ctx, modelKnobs)
	if err != nil {
		return nil, fmt.Errorf("setter.SetKnobs: %w", err)
	}
	return &desc.SetKnobsResponse{}, nil
}

func (d *Delivery) CollectExternalMetrics(ctx context.Context, _ *desc.CollectExternalMetricsRequest) (*desc.CollectExternalMetricsResponse, error) {
	metricsCh, errCh := d.loader.RunLoad(ctx)
	select {
	case metrics := <-metricsCh:
		return &desc.CollectExternalMetricsResponse{
			Tps:     float32(metrics.Tps),
			Latency: float32(metrics.Latency),
		}, nil
	case err := <-errCh:
		return nil, fmt.Errorf("loader.RunLoad: %w", err)
	}
}

func (d *Delivery) CollectInternalMetrics(ctx context.Context, _ *desc.CollectInternalMetricsRequest) (*desc.CollectInternalMetricsResponse, error) {
	metrics, err := d.selector.ListAllAggregatedMetrics(ctx)
	if err != nil {
		return nil, err
	}

	protoMetrics := lo.Map(metrics, func(metric model.InternalMetric, index int) *desc.CollectInternalMetricsResponse_Metric {
		return toDescMetric(metric)
	})

	return &desc.CollectInternalMetricsResponse{Metrics: protoMetrics}, nil
}

func toDescMetric(metric model.InternalMetric) *desc.CollectInternalMetricsResponse_Metric {
	protoMetric := &desc.CollectInternalMetricsResponse_Metric{
		Name:  metric.Name,
		Scope: metric.Scope.String(),
	}

	switch v := metric.Value.(type) {
	case string:
		protoMetric.Value = &desc.CollectInternalMetricsResponse_Metric_StrValue{StrValue: v}
	case float64:
		protoMetric.Value = &desc.CollectInternalMetricsResponse_Metric_FloatValue{FloatValue: float32(v)}
	case int64:
		protoMetric.Value = &desc.CollectInternalMetricsResponse_Metric_FloatValue{FloatValue: float32(v)}
	case bool:
		protoMetric.Value = &desc.CollectInternalMetricsResponse_Metric_BoolValue{BoolValue: v}
	case sql.Null[time.Time]:
		if v.Valid {
			val, _ := v.Value()
			protoMetric.Value = &desc.CollectInternalMetricsResponse_Metric_StrValue{StrValue: val.(time.Time).String()}
		}

	default:
		//err = fmt.Errorf("Unsupported type for knob %s: %v\n", metric.Name, reflect.TypeOf(metric.Value))
	}
	return protoMetric
}
