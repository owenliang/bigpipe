package client

import (
	"net/http"
	"bigpipe"
	"bigpipe/log"
	"bigpipe/proto"
)

// 顺序阻塞调用
type SyncClient struct {
	httpClient http.Client	// 线程安全
	rateLimit int 	// 每秒限速
	retries int	// 重试次数
	timeout int // 请求超时时间
}

type IClient interface {
	Call(message *proto.CallMessage)
}

func CreateClient(info *bigpipe.ConsumerInfo) (*SyncClient, error) {
	client := SyncClient{
		rateLimit: info.RateLimit,
		retries: info.Retries,
		timeout: info.Timeout,
	}
	return &client, nil
}

func (client *SyncClient) Call(message *proto.CallMessage) {
	// client.httpClient.Do()

	log.DEBUG("HTTP调用结束:%v", *message)
}
