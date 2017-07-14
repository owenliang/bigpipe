package client

import (
	"net/http"
	"bigpipe/proto"
	"strings"
	"bigpipe/log"
)

// 顺序阻塞调用
type AsyncClient struct {
	httpClient http.Client	// 线程安全
	rateLimit int 	// 每秒限速
	retries int	// 重试次数
	timeout int // 请求超时时间
}

func (client *AsyncClient) Call(message *proto.CallMessage, endChan chan byte) bool {
	ret := false
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
		ret = true
		break
	}
	<- endChan // 取出pending的字节
	return ret
}