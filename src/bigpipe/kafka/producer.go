package kafka

import (
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"bigpipe"
	"math/rand"
)

type Producer struct {
	client *kafka.Producer
}

func CreateProducer() (*Producer, error) {
	producer := Producer{}

	bigConf := bigpipe.GetConfig()

	// kafka服务器配置
	conf := kafka.ConfigMap{
		"go.produce.channel.size" : bigConf.Kafka_producer_channel_size,
		"bootstrap.servers" : bigConf.Kafka_bootstrap_servers,
		"retries": bigConf.Kafka_producer_retries,
	}

	// 创建生产者
	client, err := kafka.NewProducer(&conf)
	if err != nil {
		return nil, err
	}
	producer.client = client
	return &producer, nil
}

func getPartition(partitions int, partitionKey *string) int32 {
	if len(*partitionKey) == 0 {
		return rand.Int31() / int32(partitions)
	}
	var hash uint64 = 0
	for _, c := range *partitionKey {
		hash = ((hash * 33) + uint64(c)) % uint64(partitions)
	}
	return int32(hash % uint64(partitions))
}

// 发送一条数据到kafka
func (producer *Producer) SendMessage(topic *string, partitionKey *string, message *CallMessage) {
	conf := bigpipe.GetConfig()

	// 计算分区
	message.Partition = getPartition(conf.Kafka_producer_topics[*topic].Partitions, partitionKey)

	// 序列化消息
	value, err := EncodeMessage(message)
	if err != nil {
		return	// 序列化失败
	}

	// 推送消息
	msg := kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: topic, Partition: message.Partition},
		Value: value,
	}
	producer.client.ProduceChannel() <- &msg
}