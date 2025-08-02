package collector

type ExternalMetrics struct {
	Latency float64
	Tps     float64
}

type InternalMetrics struct {
	Name  string
	Value float64
}

type Knob struct {
	Name   string
	Value  float64
	MinVal float64
	MaxVal float64
}
