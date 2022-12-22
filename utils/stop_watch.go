package utils

import (
	"github.com/rs/zerolog"
	"time"
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

func (t *StopWatch) Log(e *zerolog.Event) {
	e.Dur("duration", t.Stop()).Str("name", t.name).Send()
}
