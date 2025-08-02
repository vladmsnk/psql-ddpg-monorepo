package environment

import (
	"context"
	"fmt"
	"github.com/samber/lo"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"psqlRecommendationsApi/internal/model"
	desc "psqlRecommendationsApi/pkg/environment"
)

type Delivery struct {
	desc.EnvironmentServer
	selector Selector
	setter   Setter
}

func New(selector Selector, setter Setter) *Delivery {
	return &Delivery{
		selector: selector,
		setter:   setter,
	}
}

type Selector interface {
	ListTrainingMetrics(ctx context.Context, instanceName string) ([]model.TrainingMetric, error)
	ListRewardMetrics(ctx context.Context, instanceName string) (model.ExternalMetrics, error)
	ListKnobs(ctx context.Context, instanceName string) ([]model.Knob, error)
}

type Setter interface {
	SetActions(ctx context.Context, instanceName string, actions []model.Action) error
	InitEnvironment(ctx context.Context, instanceName string) error
}

func (d *Delivery) InitEnvironment(ctx context.Context, req *desc.InitEnvironmentRequest) (*desc.InitEnvironmentResponse, error) {
	instanceName := req.GetInstanceName()
	if instanceName == "" {
		return nil, status.Error(codes.InvalidArgument, "instance_name should not be empty")
	}

	err := d.setter.InitEnvironment(ctx, instanceName)
	if err != nil {
		return nil, fmt.Errorf("setter.InitEnvironment: %w", err)
	}
	return &desc.InitEnvironmentResponse{}, nil
}

func (d *Delivery) GetStates(ctx context.Context, req *desc.GetStatesRequest) (*desc.GetStatesResponse, error) {
	instanceName := req.GetInstanceName()
	if instanceName == "" {
		return nil, status.Error(codes.InvalidArgument, "instance_name should not be empty")
	}

	metrics, err := d.selector.ListTrainingMetrics(ctx, instanceName)
	if err != nil {
		return nil, fmt.Errorf("selector.ListTrainingMetrics: %w", err)
	}

	descMetrics := lo.Map(metrics, func(metric model.TrainingMetric, _ int) float32 {
		return metric.Value
	})

	return &desc.GetStatesResponse{Metrics: descMetrics}, nil
}

func (d *Delivery) GetRewardMetrics(ctx context.Context, req *desc.GetRewardMetricsRequest) (*desc.GetRewardMetricsResponse, error) {
	instanceName := req.GetInstanceName()
	if instanceName == "" {
		return nil, status.Error(codes.InvalidArgument, "instance_name should not be empty")
	}

	metrics, err := d.selector.ListRewardMetrics(ctx, instanceName)
	if err != nil {
		return nil, fmt.Errorf("selector.ListRewardMetrics: %w", err)
	}
	return &desc.GetRewardMetricsResponse{Tps: float32(metrics.Tps), Latency: float32(metrics.Latency)}, nil
}

func (d *Delivery) ApplyActions(ctx context.Context, req *desc.ApplyActionsRequest) (*desc.ApplyActionsResponse, error) {
	instanceName := req.GetInstanceName()
	if instanceName == "" {
		return nil, status.Error(codes.InvalidArgument, "instance_name should not be empty")
	}

	knobsToApply := lo.Map(req.GetActions(), func(action *desc.ApplyActionsRequest_Action, _ int) model.Action {
		return model.Action{
			Name:  action.GetName(),
			Value: float64(action.GetValue()),
		}
	})

	err := d.setter.SetActions(ctx, instanceName, knobsToApply)
	if err != nil {
		return nil, fmt.Errorf("setter.ApplyActions: %w", err)
	}

	return &desc.ApplyActionsResponse{}, nil
}

func (d *Delivery) GetActionState(ctx context.Context, req *desc.GetActionStateRequest) (*desc.GetActionStateResponse, error) {
	instanceName := req.GetInstanceName()
	if instanceName == "" {
		return nil, status.Error(codes.InvalidArgument, "instance_name should not be empty")
	}

	filterKnobsNames := req.GetKnobs()
	if len(filterKnobsNames) == 0 {
		return nil, status.Error(codes.InvalidArgument, "knobs should not be empty")
	}

	knobs, err := d.selector.ListKnobs(ctx, instanceName)
	if err != nil {
		return nil, fmt.Errorf("selector.ListKnobs: %w", err)
	}

	knobs = lo.Filter(knobs, func(knob model.Knob, _ int) bool {
		return lo.Contains(filterKnobsNames, knob.Name)
	})

	descKnobs := lo.Map(knobs, func(knob model.Knob, _ int) *desc.GetActionStateResponse_Knob {
		return &desc.GetActionStateResponse_Knob{
			Name:     knob.Name,
			Value:    float32(knob.Value),
			MaxValue: float32(knob.MaxVal),
			MinValue: float32(knob.MinVal),
		}
	})

	return &desc.GetActionStateResponse{Knobs: descKnobs}, nil
}
