package runner

import (
	"context"
	"fmt"
	"log"
	"postgresHelper/internal/model"
	"time"
)

const (
	defaultCollectInterval = time.Second * 10
)

type Runner interface {
	Run(ctx context.Context)
}

type Implementation struct {
	collect         Collector
	storage         Storager
	collectInterval time.Duration
}

type Collector interface {
	CollectKnobs(ctx context.Context) ([]model.Knob, error)
}

type Storager interface {
	SetKnobs(knobs []model.Knob)
	GetKnobs() []model.Knob
}

func New(collect Collector, storage Storager) *Implementation {
	return &Implementation{
		collect:         collect,
		storage:         storage,
		collectInterval: defaultCollectInterval,
	}
}

func (i *Implementation) Run() {
	ctx := context.Background()

	go func() {
		ticker := time.NewTicker(i.collectInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				knobs, err := i.collect.CollectKnobs(ctx)
				if err != nil {
					log.Println(err)
					continue
				}
				i.storage.SetKnobs(knobs)
				fmt.Println("Collected knobs")
			}
		}
	}()
}
