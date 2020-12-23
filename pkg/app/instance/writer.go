package instance

import (
	message_queue "github.com/BrobridgeOrg/gravity-transmitter-kafka/pkg/message_queue"
)

func (a *AppInstance) initWriter() error {
	return a.writer.Init()
}

func (a *AppInstance) GetWriter() message_queue.Writer {
	return message_queue.Writer(a.writer)
}
