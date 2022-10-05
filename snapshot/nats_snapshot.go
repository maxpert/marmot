package snapshot

import (
	"github.com/nats-io/nats.go"
)

type NatsSnapshot interface {
	SaveSnapshot(nats *nats.Conn) error
	RestoreSnapshot(nats *nats.Conn) error
}
