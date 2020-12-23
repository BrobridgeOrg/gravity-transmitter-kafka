package app

import (
	"github.com/BrobridgeOrg/gravity-transmitter-kafka/pkg/grpc_server"
	"github.com/BrobridgeOrg/gravity-transmitter-kafka/pkg/message_queue"
	"github.com/BrobridgeOrg/gravity-transmitter-kafka/pkg/mux_manager"
)

type App interface {
	GetGRPCServer() grpc_server.Server
	GetMuxManager() mux_manager.Manager
	GetWriter() message_queue.Writer
}
