package app

import (
	"github.com/BrobridgeOrg/gravity-transmitter-kafka/pkg/message_queue"
)

type App interface {
	GetWriter() message_queue.Writer
}
