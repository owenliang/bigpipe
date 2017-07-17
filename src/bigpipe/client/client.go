package client

import (
	"bigpipe/config"
	"bigpipe/proto"
)

type IClient interface {
	Call(message *proto.CallMessage)
	PendingCount() int
}

func CreateClient(info *config.ConsumerInfo) (IClient, error) {
	return CreateAsyncClient(info)
}
