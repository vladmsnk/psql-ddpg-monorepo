package storage

import (
	"postgresHelper/internal/model"
)

func (s *Storage) GetKnobs() []model.Knob {
	if s == nil {
		return nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	return s.knobs
}
