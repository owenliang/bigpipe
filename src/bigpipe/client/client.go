package client

import (
	"bigpipe"
	"bigpipe/proto"
	"time"
)

type IClient interface {
	Call(message *proto.CallMessage, endChan chan byte) bool
}

func CreateClient(info *bigpipe.ConsumerInfo) (IClient, error) {
	// 根据配置创建不同类型的客户端
	client := AsyncClient{
		rateLimit: info.RateLimit,
		retries: info.Retries,
		timeout: info.Timeout,
	}
	// 客户端超时时间
	client.httpClient.Timeout = time.Duration(client.timeout) * time.Millisecond
	return &client, nil
}
