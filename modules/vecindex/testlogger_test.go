package vecindex

import (
	"io"

	"github.com/rs/zerolog"
)

// newTestLogger returns a zerolog.Logger that discards all output.
func newTestLogger() zerolog.Logger {
	return zerolog.New(io.Discard)
}
