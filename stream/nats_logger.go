package stream

import "github.com/rs/zerolog"

type natsLogger struct {
	zerolog.Logger
}

func (n *natsLogger) Noticef(format string, v ...interface{}) {
	n.Info().Msgf(format, v...)
}

func (n *natsLogger) Warnf(format string, v ...interface{}) {
	n.Warn().Msgf(format, v...)
}

func (n *natsLogger) Fatalf(format string, v ...interface{}) {
	n.Fatal().Msgf(format, v...)
}

func (n *natsLogger) Errorf(format string, v ...interface{}) {
	n.Error().Msgf(format, v...)
}

func (n *natsLogger) Debugf(format string, v ...interface{}) {
	n.Debug().Msgf(format, v...)
}

func (n *natsLogger) Tracef(format string, v ...interface{}) {
	n.Trace().Msgf(format, v...)
}
