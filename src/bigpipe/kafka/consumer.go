package kafka

import (
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"bigpipe"
	"fmt"
	"os"
)

type Consumer struct {
	clients []*kafka.Consumer	// 消费者数组
}

// 创建消费者（多个彼此独立）
func CreateConsumer() (*Consumer, error) {
	bigConf := bigpipe.GetConfig()

	consumer := Consumer{}
	for _, consumerInfo := range bigConf.Kafka_consumer_list {
		client, err := kafka.NewConsumer(&kafka.ConfigMap{
			"bootstrap.servers":               bigConf.Kafka_bootstrap_servers,
			"group.id":                        consumerInfo.GroupId,
			"session.timeout.ms":              30000,
			"go.events.channel.enable":        true,	// 通过管道读取数据
			"go.application.rebalance.enable": true,	// 启动负载均衡
			"auto.offset.reset": 	"latest",
		})
		if err != nil {
			return nil, err
		}
		// 订阅topic
		topics := []string{consumerInfo.Topic}
		err = client.SubscribeTopics(topics, nil)
		if err != nil {
			return nil, err
		}
		consumer.clients = append(consumer.clients, client)
	}
	return &consumer, nil
}

// 处理kafka消息
func consumeLoop(consumer *Consumer, client *kafka.Consumer, topic *string, rateLimit int) {
	run := true

	for run {
		select {
		case ev := <-client.Events():
			switch e := ev.(type) {
			case kafka.AssignedPartitions:
				fmt.Fprintf(os.Stderr, "%% %v\n", e)
				client.Assign(e.Partitions)
			case kafka.RevokedPartitions:
				fmt.Fprintf(os.Stderr, "%% %v\n", e)
				client.Unassign()
			case *kafka.Message:
				// 反序列化请求
				if msg, isValid := DecodeMessage(e.Value); isValid {
					fmt.Println(msg)
				}
				fmt.Printf("%% Message on %s:\n%s\n",
					e.TopicPartition, string(e.Value))
			case kafka.PartitionEOF:
				fmt.Printf("%% Reached %v\n", e)
			case kafka.Error:
				fmt.Fprintf(os.Stderr, "%% Error: %v\n", e)
				run = false
			}
		}
	}
}

// 为每个消费者启动一个独立的协程
func (consumer *Consumer) Run() bool {
	bigConf := bigpipe.GetConfig()

	for i, client := range consumer.clients {
		go consumeLoop(
			consumer,
			client,
			&bigConf.Kafka_consumer_list[i].Topic,
			bigConf.Kafka_consumer_list[i].RateLimit,
		)
	}
	return true
}