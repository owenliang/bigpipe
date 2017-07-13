package kafka

import (
	"encoding/json"
	"bigpipe/util"
)

// Kafka消息体
type CallMessage struct {
	Headers map[string][]string	// 原始http头
	Url string	// 发送目标
	Data string	// 发送数据
	Topic string	// 消息所处的Topic
	Partition int	// 消息所处的分区ID
	CreateTime int	// 消息创建的时间
}

// JSON编码消息体
func EncodeMessage(message *CallMessage) ([]byte, error) {
	return json.Marshal(*message)
}

// JSON解码消息体
func DecodeMessage(data []byte) (*CallMessage, bool) {
	dict := map[string]interface{} {}
	if err := json.Unmarshal(data, &dict); err != nil {
		return nil, false
	}

	message := CallMessage{}

	getOk := false

	if message.Url, getOk = util.JsonGetString(&dict, "Url"); !getOk {
		return nil, false
	}
	if message.Data, getOk = util.JsonGetString(&dict, "Data"); !getOk {
		return nil, false
	}
	if message.Topic, getOk = util.JsonGetString(&dict, "Topic"); !getOk {
		return nil, false
	}
	if message.Partition, getOk = util.JsonGetInt(&dict, "Partition"); !getOk {
		return nil, false
	}
	if message.CreateTime, getOk = util.JsonGetInt(&dict, "Partition"); !getOk {
		return nil, false
	}

	if headers, exist := dict["Headers"]; exist {
		if headersDict, isMap := headers.(map[string]interface{}); isMap {
			for key, value := range headersDict {
				if headerArr, isArr := value.([]string); isArr {
					message.Headers[key] = headerArr
				} else {
					return nil, false
				}
			}
		} else {
			return nil, false
		}
	} else {
		return nil, false
	}
	return &message, true
}