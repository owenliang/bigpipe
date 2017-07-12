package kafka

import "github.com/confluentinc/confluent-kafka-go/kafka"

type Producer struct {
	client *kafka.Producer

}

func CreateProducer() (*Producer, error) {
	producer := Producer{}

	// kafka服务器配置
	conf := kafka.ConfigMap{
		"go.produce.channel.size" : 1000000,
		"bootstrap.servers" : "localhost:10100,localhost:10101,localhost:10102",
		"retries": 3,
	}

	// 创建生产者
	client, err := kafka.NewProducer(&conf)
	if err != nil {
		return nil, err
	}
	producer.client = client
	return &producer, nil
}

// 发送一条数据到kafka
func (producer *Producer) SendMessage(topic *string, partition int32, value []byte) {
	msg := kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: topic, Partition: partition},
		Value: value,
	}
	producer.client.ProduceChannel() <- &msg
}