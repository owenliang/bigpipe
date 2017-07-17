package kafka

import (
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"bigpipe"
	"math/rand"
	"runtime"
	"bigpipe/log"
	"time"
	"bigpipe/proto"
	"bigpipe/stats"
)

type Producer struct {
	client *kafka.Producer
}

// 处理消息发送结果
func handleEvents(producer *kafka.Producer) {
	for e := range producer.Events() {
		switch ev := e.(type) {
		case *kafka.Message:
			if ev.TopicPartition.Error != nil {
				stats.ProducerStats_deliveryFail(ev.TopicPartition.Topic)
				log.WARNING("Delivery failed: %v\n", ev.TopicPartition.Error)
			} else {
				stats.ProducerStats_deliverySuccess(ev.TopicPartition.Topic)
				log.INFO("Delivered message to topic %s [%d] at offset %v\n",
					*ev.TopicPartition.Topic, ev.TopicPartition.Partition, ev.TopicPartition.Offset)
			}
		default:
			log.INFO("Ignored event: %s\n", ev)
		}
	}
}

// 创建生产者
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

	// 处理消息发送结果（多个goroutine并发处理）
	for i := 0; i < runtime.NumCPU(); i = i + 1 {
		go handleEvents(client)
	}

	producer.client = client
	return &producer, nil
}

func DestroyProducer(producer *Producer) {
	// 等待producer堆积数量降为0
	for producer.client.Len() != 0 {
		time.Sleep(1 * time.Second)	// 睡眠1秒再次检测
	}
	log.INFO("Producer关闭成功")
}

func getPartition(partitions int, partitionKey *string) int {
	if len(*partitionKey) == 0 {
		return int(rand.Int31() / int32(partitions))
	}
	var hash uint64 = 0
	for _, c := range *partitionKey {
		hash = ((hash * 33) + uint64(c)) % uint64(partitions)
	}
	return int(hash % uint64(partitions))
}

// 发送一条数据到kafka
func (producer *Producer) SendMessage(topic *string, partitionKey *string, message *proto.CallMessage) bool {
	conf := bigpipe.GetConfig()

	// 计算分区
	message.Partition = getPartition(conf.Kafka_topics[*topic].Partitions, partitionKey)

	// 序列化消息
	value, err := proto.EncodeMessage(message)
	if err != nil {
		return false	// 序列化失败
	}

	msg := kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: topic, Partition: int32(message.Partition)},
		Value:          value,
	}
	select {
	case producer.client.ProduceChannel() <- &msg:	// 推送消息
		return true
	default:	// 队列满了, 那么返回失败
		return false
	}
}