package utils

import (
	"time"

	"github.com/maxpert/marmot/telemetry"
	"github.com/rs/zerolog"
)

type StopWatch struct {
	startTime time.Time
	name      string
}

func NewStopWatch(name string) *StopWatch {
	return &StopWatch{
		startTime: time.Now(),
		name:      name,
	}
}

func (t *StopWatch) Stop() time.Duration {
	return time.Since(t.startTime)
}

func (t *StopWatch) Log(e *zerolog.Event, hist telemetry.Histogram) {
	dur := t.Stop()
	if hist != nil {
		hist.Observe(float64(dur.Microseconds()))
	}

	e.Dur("duration", dur).Str("name", t.name).Send()
}
