package utils

import (
	"github.com/asaskevich/EventBus"
	"github.com/rs/zerolog/log"
	"time"
)

type TimeoutPublisher struct {
	duration  time.Duration
	ticker    *time.Ticker
	publisher chan time.Time
}

func EventBusTimeout(bus EventBus.Bus, eventName string, duration time.Duration) *TimeoutPublisher {
	t := NewTimeoutPublisher(duration)
	err := bus.Subscribe(eventName, func(args ...any) {
		t.Reset()
	})

	if err != nil {
		log.Panic().Err(err).Msg("Unable to subscribe timeout event bus")
	}

	return t
}

func NewTimeoutPublisher(duration time.Duration) *TimeoutPublisher {
	if duration == 0 {
		return &TimeoutPublisher{
			duration:  duration,
			ticker:    nil,
			publisher: make(chan time.Time),
		}
	}

	ticker := time.NewTicker(duration)
	ticker.Stop()
	return &TimeoutPublisher{duration: duration, ticker: ticker, publisher: nil}
}

func (t *TimeoutPublisher) Reset() {
	if t.ticker != nil {
		t.ticker.Reset(t.duration)
	}
}

func (t *TimeoutPublisher) Next() <-chan time.Time {
	if t.publisher != nil {
		return t.publisher
	}

	return t.ticker.C
}
