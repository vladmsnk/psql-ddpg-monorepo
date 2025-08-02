package loader

import (
	"context"
	"postgresHelper/internal/model"
)

type Bench interface {
	InitializePgbench(ctx context.Context) error
	RunPgbench(ctx context.Context) (float64, float64, error)
}

type Loader interface {
	RunLoad(ctx context.Context) (<-chan model.ExternalMetric, <-chan error)
	InitLoad(ctx context.Context) error
}

type Implementation struct {
	bench Bench
}

func New(bench Bench) *Implementation {
	return &Implementation{bench: bench}
}

func (i *Implementation) RunLoad(ctx context.Context) (<-chan model.ExternalMetric, <-chan error) {
	metricCh, errCh := make(chan model.ExternalMetric), make(chan error)
	go func() {
		defer func() {
			close(metricCh)
			close(errCh)
		}()

		tps, latency, err := i.bench.RunPgbench(ctx)
		if err != nil {
			errCh <- err
		}
		metricCh <- model.ExternalMetric{
			Tps:     tps,
			Latency: latency,
		}
	}()

	return metricCh, errCh

}

func (i *Implementation) InitLoad(ctx context.Context) error {
	return i.bench.InitializePgbench(ctx)
}
