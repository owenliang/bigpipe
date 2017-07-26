package server

import (
	"io/ioutil"
	"bigpipe/kafka"
	"net/http"
	"encoding/json"
	"bigpipe/config"
	"time"
	"bigpipe/util"
	"bigpipe/proto"
	"bigpipe/stats"
	"runtime"
	"bigpipe/log"
)

type Handler struct {
	mux *http.ServeMux
	producer *kafka.Producer
	bigConf *config.Config

	callChan chan *callContext

	termChan chan byte
	waitChan chan byte

	reloadingWChan chan byte
	reloadingRChan chan byte

	reloadDoneWChan chan byte
	reloadDoneRChan chan byte
}

type callContext struct {
	message *proto.CallMessage
	topic *string
	partitionKey *string
}

type callRequest struct {
	url string
	data string
	partitionKey string
	acl config.ProducerACL
}

// 闭包提供上下文
func makeHandler(handler *Handler, f func(*Handler, http.ResponseWriter, *http.Request) bool) func(http.ResponseWriter, *http.Request) {
	return func (w http.ResponseWriter, r *http.Request) {
		f(handler, w, r)
	}
}

func packResponse(w http.ResponseWriter, errno int, msg string, data interface{}) bool {
	resp := map[string]interface{} {}
	resp["errno"] = errno
	resp["msg"] = msg
	resp["data"] = data

	respJson, err := json.Marshal(resp)
	if err != nil {
		w.Write([]byte("{\"errno\":1,\"msg\": \"服务内部错误\",\"data\":[]"))
		return false
	}
	w.Write(respJson)
	return true
}

// 检查ACL权限
func (handler *Handler)aclCheck(request *callRequest) bool {
	if aclItem, exist := handler.bigConf.Kafka_producer_acl[request.acl.Name]; exist {
		request.acl.Topic = aclItem.Topic
		return true
	}
	return false
}

// 生成kafka消息
func makeCallMessage(req *http.Request, call *callRequest) *proto.CallMessage {
	now := time.Now()
	msg := proto.CallMessage{
		Headers: req.Header,
		Url: call.url,
		Data: call.data,
		Topic: call.acl.Topic,
		CreateTime: int(now.UnixNano() / 1000000),
	}
	return &msg
}

// POST JSON
// {
// 		"acl": {"name": "system-1", "secret": "you are good"},
// 		"url": "http://www.baidu.com",
//		"data": "serialized data in json or other format"
//		"partition_key": "uid-10203031"
// }
// acl：权限验证
// url: 调用的地址
// data：序列化的数据，将会POST到url
// partition_key：分区key，相同的key会进入同一个kafka分区，传空串则随机分发
func parseRequest(body []byte) (*callRequest, string) {
	// 按map解析Json
	dict := map[string]interface{} {}
	if err := json.Unmarshal(body, &dict); err != nil {
		return nil, "JSON无效"
	}

	request := callRequest{}
	getOk := false

	if request.url, getOk = util.JsonGetString(&dict, "url"); !getOk {
		return nil, "url无效"
	}
	if request.data, getOk = util.JsonGetString(&dict, "data"); !getOk {
		return nil, "data无效"
	}
	if request.partitionKey, getOk = util.JsonGetString(&dict, "partition_key"); !getOk {
		return nil, "partition_key无效"
	}
	if acl, exsit := dict["acl"]; exsit {
		if aclMap, exsit := acl.(map[string]interface{}); exsit {
			if request.acl.Name, getOk = util.JsonGetString(&aclMap, "name"); !getOk {
				return nil, "acl.name"
			}
			if request.acl.Secret, getOk = util.JsonGetString(&aclMap, "secret"); !getOk {
				return nil, "acl.secret"
			}
		} else {
			return nil, "acl无效"
		}
	} else {
		return nil, "acl无效"
	}
	return &request, ""
}

// 接口: /rpc/call
func handleRpcCall(handler *Handler, w http.ResponseWriter, r *http.Request) bool {
	stats.ServerStats_receivedCall()

	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		return packResponse(w,1, "内部错误", "")
	}
	// 解析请求
	request, errmsg := parseRequest(body)
	if len(errmsg) != 0 {
		return packResponse(w,-1, errmsg, "")
	}
	// ACL检测
	if !handler.aclCheck(request) {
		return packResponse(w, -1, "权限错误", "")
	}
	// 构造消息
	message := makeCallMessage(r, request)

	// 队列上下文
	context := callContext{
		message: message,
		topic: &request.acl.Topic,
		partitionKey: &request.partitionKey,
	}

	// 消息排队
	select {
	case handler.callChan <- &context:
		stats.ServerStats_acceptedCall()
		return packResponse(w, 0, "发送成功", "")
	default:
		stats.ServerStats_overloadCall()
		return packResponse(w, -1, "超过负载", "")
	}
}

// 测试用的rpc响应接口
func handleStats(handler *Handler, w http.ResponseWriter, r *http.Request) bool {
	return packResponse(w, 0, "success", stats.StatsInfo())
}

// 测试用的rpc响应接口
func handleServerMock(handler *Handler, w http.ResponseWriter, r *http.Request) bool {
	return true
}

func (handler *Handler)forwardCallMessage() {
	// 正常消费
loop:
	for {
		select {
		case ctx := <-handler.callChan:
			handler.producer.SendMessage(ctx.topic, ctx.partitionKey, ctx.message)
		case <-handler.reloadingWChan:
			handler.reloadingRChan <- 1
			goto reloading
		case <-handler.termChan:
			goto finalLoop
		}
	}

	// 等待重置完成
reloading:
	for {
		select {
		case <-handler.reloadDoneWChan:
			handler.reloadDoneRChan <- 1
			goto loop
		}
	}

	// 退出前销毁
finalLoop:
	for {
		select {
		case ctx := <-handler.callChan:
			handler.producer.SendMessage(ctx.topic, ctx.partitionKey, ctx.message)
		default:
			break finalLoop
		}
	}
	handler.waitChan <- 1
}

func CreateHandler(producer *kafka.Producer, bigConf *config.Config) *Handler {
	handler := Handler{
		producer: producer,
		bigConf: bigConf,
	}

	// 路由
	handler.mux = http.NewServeMux()

	// 添加路由
	handler.mux.HandleFunc("/rpc/call", makeHandler(&handler, handleRpcCall))
	handler.mux.HandleFunc("/stats", makeHandler(&handler, handleStats))
	handler.mux.HandleFunc("/rpc/server/mock", makeHandler(&handler, handleServerMock))

	// 启动处理器
	handler.callChan = make(chan *callContext, bigConf.Http_server_handler_channel_size)

	// 关闭通知
	handler.termChan = make(chan byte, runtime.NumCPU())
	handler.waitChan = make(chan byte, runtime.NumCPU())

	// 重载通知
	handler.reloadingWChan = make(chan byte, runtime.NumCPU())
	handler.reloadingRChan = make(chan byte, runtime.NumCPU())

	// 重载完成通知
	handler.reloadDoneWChan = make(chan byte, runtime.NumCPU())
	handler.reloadDoneRChan = make(chan byte, runtime.NumCPU())

	for i := 0; i < runtime.NumCPU(); i = i + 1 {
		go handler.forwardCallMessage();
	}
	return &handler
}

func DestroyHandler(handler *Handler) {
	for i := 0; i < runtime.NumCPU(); i = i + 1 {
		handler.termChan <- 1
	}
	for i := 0; i < runtime.NumCPU(); i = i + 1 {
		<-handler.waitChan
	}
	log.INFO("handler关闭成功")
}

func (handler *Handler)Reloading() {
	// 通知停止工作
	for i := 0; i < runtime.NumCPU(); i = i + 1 {
		handler.reloadingWChan <- 1
	}
	// 等待工作停止
	for i := 0; i < runtime.NumCPU(); i = i + 1 {
		<-handler.reloadingRChan
	}
}

func (handler *Handler)ReloadDone(producer *kafka.Producer) {
	handler.producer = producer
	// 通知恢复工作
	for i := 0; i < runtime.NumCPU(); i = i + 1 {
		handler.reloadDoneWChan <- 1
	}
	// 等待工作恢复
	for i := 0; i < runtime.NumCPU(); i = i + 1 {
		<-handler.reloadDoneRChan
	}
}

func (handler *Handler) getMux () *http.ServeMux {
	return handler.mux
}
