package model

type TrainingMetric struct {
	Value float32
}

type InternalMetrics struct {
	Name  string
	Scope string
	Value interface{}
}

type ExternalMetrics struct {
	Tps     float64
	Latency float64
}

type Knob struct {
	Name   string
	Value  float64
	MinVal float64
	MaxVal float64
}

type Action struct {
	Name  string
	Value float64
}
