package storage

import (
	"postgresHelper/internal/model"
	"sync"
)

type Storager interface {
	Setter
	Getter
}

type Setter interface {
	SetKnobs(knobs []model.Knob)
}

type Getter interface {
	GetKnobs() []model.Knob
}

type Storage struct {
	knobs []model.Knob
	mu    sync.Mutex
}

func New() *Storage {
	return &Storage{
		knobs: make([]model.Knob, 0),
	}
}
