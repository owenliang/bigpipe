package kafka

import "encoding/json"

// Kafka消息体
type CallMessage struct {
	Headers map[string][]string	// 原始http头
	Url string	// 发送目标
	Data string	// 发送数据
	Topic string	// 消息所处的Topic
	Partition int32	// 消息所处的分区ID
	CreateTime int64	// 消息创建的时间
}

// JSON编码消息体
func EncodeMessage(message *CallMessage) ([]byte, error) {
	return json.Marshal(*message)
}
