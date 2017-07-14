package client

import (
	"bigpipe"
	"bigpipe/proto"
)

type IClient interface {
	Call(message *proto.CallMessage)
	PendingCount() int
}

func CreateClient(info *bigpipe.ConsumerInfo) (IClient, error) {
	return CreateAsyncClient(info)
}
