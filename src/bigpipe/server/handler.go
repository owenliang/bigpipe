package server

import "net/http"
import "bigpipe/kafka"
import librdkafka "github.com/confluentinc/confluent-kafka-go/kafka"

type Handler struct {
	mux *http.ServeMux
	producer *kafka.Producer
}

// 闭包提供上下文
func makeHandler(handler *Handler, f func(*Handler, http.ResponseWriter, *http.Request)) func(http.ResponseWriter, *http.Request) {
	return func (w http.ResponseWriter, r *http.Request) {
		f(handler, w, r)
	}
}

// 接口: /rpc/call
func handleRpcCall(handler *Handler, w http.ResponseWriter, r *http.Request) {
	topic := "smzdm"
	handler.producer.SendMessage(&topic, librdkafka.PartitionAny, []byte("hello"))
	w.Write([]byte("do you call me?"))
}

func CreateHandler(producer *kafka.Producer) *Handler {
	handler := Handler{
		producer: producer,
	}

	// 路由
	handler.mux = http.NewServeMux()

	// 添加路由项目
	handler.mux.HandleFunc("/rpc/call", makeHandler(&handler, handleRpcCall))

	return &handler
}

func (handler *Handler) getMux () *http.ServeMux {
	return handler.mux
}
