package server

import (
	"io/ioutil"
	"bigpipe/kafka"
	"net/http"
	"encoding/json"
	"bigpipe"
	"time"
	"bigpipe/util"
	"bigpipe/proto"
)

type Handler struct {
	mux *http.ServeMux
	producer *kafka.Producer
}
type callRequest struct {
	url string
	data string
	partitionKey string
	acl bigpipe.ProducerACL
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
func aclCheck(request *callRequest) bool {
	conf := bigpipe.GetConfig()
	if aclItem, exist := conf.Kafka_producer_acl[request.acl.Name]; exist {
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
	if !aclCheck(request) {
		return packResponse(w, -1, "权限错误", "")
	}
	// 构造消息上下文
	message := makeCallMessage(r, request)

	// 在新协程中发送, 避免阻塞用户请求
 	if sent := handler.producer.SendMessage(&request.acl.Topic, &request.partitionKey, message); sent {
		return packResponse(w, 0, "发送成功", "")
	} else {
		return packResponse(w, -1, "超过负载", "")
	}
}

// 测试用的rpc响应接口
func handleServerMock(handler *Handler, w http.ResponseWriter, r *http.Request) bool {
	return true
}

func CreateHandler(producer *kafka.Producer) *Handler {
	handler := Handler{
		producer: producer,
	}

	// 路由
	handler.mux = http.NewServeMux()

	// 添加路由
	handler.mux.HandleFunc("/rpc/call", makeHandler(&handler, handleRpcCall))
	handler.mux.HandleFunc("/rpc/server/mock", makeHandler(&handler, handleServerMock))

	return &handler
}

func (handler *Handler) getMux () *http.ServeMux {
	return handler.mux
}
