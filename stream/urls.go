package stream

import (
	"net/url"

	"github.com/rs/zerolog"
)

type UrlsArray []*url.URL

func (u UrlsArray) MarshalZerologArray(a *zerolog.Array) {
	for _, x := range u {
		a.Str(x.String())
	}
}
