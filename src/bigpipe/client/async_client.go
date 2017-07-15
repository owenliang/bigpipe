package client

import (
	"net/http"
	"bigpipe/proto"
	"strings"
	"bigpipe/log"
	"time"
	"bigpipe"
)

// 顺序阻塞调用
type AsyncClient struct {
	httpClient http.Client	// 线程安全
	retries int	// 重试次数
	timeout int // 请求超时时间
	pending chan byte	// 正在并发中的http请求个数
	rateLimit *TokenBucket
}

func CreateAsyncClient(info *bigpipe.ConsumerInfo) (IClient, error) {
	// 根据配置创建不同类型的客户端
	client := AsyncClient{
		retries: info.Retries,
		timeout: info.Timeout,
	}
	// 并发控制管道
	client.pending = make(chan byte, info.Concurrency)
	// 流速控制器
	client.rateLimit = CreateBucketForRate(float64(info.RateLimit))
	// 客户端超时时间
	client.httpClient.Timeout = time.Duration(client.timeout) * time.Millisecond
	return &client, nil
}

func (client *AsyncClient) callWithRetry(message *proto.CallMessage) {
	for i := 0; i < client.retries + 1; i++ {
		req, err := http.NewRequest("POST", message.Url, strings.NewReader(message.Data))
		if err != nil {
			log.WARNING("HTTP调用失败（%d）:（%v）（%v）", i, *message, err)
			continue
		}
		req.Header = message.Headers
		response, rErr := client.httpClient.Do(req)
		if rErr != nil {
			log.WARNING("HTTP调用失败（%d）：（%v）（%v）", i, *message, err)
			continue
		}

		// 不读应答体
		response.Body.Close()

		// 判断返回码是200即可
		if response.StatusCode != 200 {
			log.WARNING("HTTP调用失败（%d）：(%v)，(%d)", i, *message, response.StatusCode)
			continue
		}
		log.INFO("HTTP调用成功:（%v）", *message)
		break
	}
	<- client.pending // 取出pending的字节
}

func (client *AsyncClient) Call(message *proto.CallMessage) {
	// 并发控制
	client.pending <- byte(1)

	// 流速控制
	client.rateLimit.getToken(1)

	// 启动协程发送请求
	go client.callWithRetry(message)
}

func (client *AsyncClient) PendingCount() int {
	return len(client.pending)
}