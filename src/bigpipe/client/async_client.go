package client

import (
	"net/http"
	"bigpipe/proto"
	"strings"
	"bigpipe/log"
	"time"
	"bigpipe/config"
	"bigpipe/stats"
	"strconv"
)

// 顺序阻塞调用
type AsyncClient struct {
	httpClient http.Client	// 线程安全
	retries int	// 重试次数
	timeout int // 请求超时时间
	pending chan byte	// 正在并发中的http请求个数
	rateLimit *TokenBucket // 令牌桶限速
	circuitBreaker *CircuitBreaker // 熔断器
}

func CreateAsyncClient(info *config.ConsumerInfo) (IClient, error) {
	// 根据配置创建不同类型的客户端
	client := AsyncClient{
		retries: info.Retries,
		timeout: info.Timeout,
	}
	// 熔断器
	if info.CircuiteBreakerInfo != nil {
		client.circuitBreaker = CreateCircuitBreaker(info.CircuiteBreakerInfo)
	}
	// 并发控制管道
	client.pending = make(chan byte, info.Concurrency)
	// 流速控制器
	client.rateLimit = CreateBucketForRate(float64(info.RateLimit))
	// 客户端超时时间
	client.httpClient.Timeout = time.Duration(client.timeout) * time.Millisecond
	return &client, nil
}

func (client *AsyncClient) notifyCircuitBreaker(success bool) {
	if client.circuitBreaker != nil {
		if success {
			client.circuitBreaker.Success()
		} else {
			client.circuitBreaker.Fail()
		}
	}
}

func (client *AsyncClient) callWithRetry(message *proto.CallMessage) {
	success := false
	for i := 0; i < client.retries + 1; i++ {
		// 非首次调用为重试
		if i != 0 {
			stats.ClientStats_rpcRetries(&message.Topic)
		}
		req, err := http.NewRequest("POST", message.Url, strings.NewReader(message.Data))
		if err != nil {
			log.WARNING("HTTP调用失败（%d）:（%v）（%v）", i, *message, err)
			continue
		}
		req.Header = message.Headers
		req.Header["Content-Length"] = []string{strconv.Itoa(len(message.Data))}
		req.Header["Content-Type"] = []string{"application/octet-stream"}

		reqStartTime := time.Now().UnixNano()
		response, rErr := client.httpClient.Do(req)
		reqUsedTime := int64((time.Now().UnixNano() - reqStartTime) / 1000000)

		if rErr != nil {
			client.notifyCircuitBreaker(false)
			log.WARNING("HTTP调用失败（%d）（%dms）：（%v）（%v）", i, reqUsedTime, *message, err)
			continue
		}

		// 不读应答体
		response.Body.Close()

		// 判断返回码是200即可
		if response.StatusCode != 200 {
			client.notifyCircuitBreaker(false)
			log.WARNING("HTTP调用失败（%d）（%dms）：(%v)，(%d)", i, reqUsedTime, *message, response.StatusCode)
			continue
		}
		success = true
		client.notifyCircuitBreaker(true)
		log.INFO("HTTP调用成功（%d）（%dms）:（%v）", i, reqUsedTime, *message)
		break
	}
	<- client.pending // 取出pending的字节

	if success {
		stats.ClientStats_rpcSuccess(&message.Topic)
	} else {
		stats.ClientStats_rpcFail(&message.Topic)
	}
}

func (client *AsyncClient) Call(message *proto.CallMessage, termChan chan int) {
	stats.ClientStats_rpcTotal(&message.Topic)

	// 熔断控制
	if client.circuitBreaker != nil {
	CIRCUIT_LOOP:
		for {
			isBreak, isHealthy, healthRate := client.circuitBreaker.IsBreak()
			if isBreak { // 熔断则等待1秒再检查
				select {
					case <- termChan: // 来自调用方的关闭信号, 为了避免熔断影响退出时间, 一旦调用方关闭则暂停熔断控制, 快速消化剩余流量
						log.DEBUG("Client调用方通知关闭, 熔断逻辑失效.")
						break CIRCUIT_LOOP
					case <- time.After(1 * time.Second):	// 正常情况下间隔1秒确认熔断状态
						stats.ClientStats_setCircuitIsBreak(&message.Topic, true)	// 熔断开关打点统计
						log.WARNING("熔断[触发] Topic=%s isHealthy=%v healthRate=%v", message.Topic, isHealthy, healthRate)
						continue
				}
			} else {
				break
			}
		}
	}
	stats.ClientStats_setCircuitIsBreak(&message.Topic, false) // 熔断开关打点统计

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