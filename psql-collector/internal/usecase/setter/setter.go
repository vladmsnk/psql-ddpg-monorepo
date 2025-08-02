package setter

import (
	"context"
	"postgresHelper/internal/model"
)

type Setter interface {
	SetKnobs(ctx context.Context, knobs []model.Knob) error
}

type Collector interface {
	SetKnobs(ctx context.Context, knobs []model.Knob) error
}

type Implementation struct {
	collector Collector
}

func New(collector Collector) *Implementation {
	return &Implementation{
		collector: collector,
	}
}

func (i *Implementation) SetKnobs(ctx context.Context, knobs []model.Knob) error {
	return i.collector.SetKnobs(ctx, knobs)
}
