package kafka

import (
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"bigpipe"
	"fmt"
	"bigpipe/log"
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
			"heartbeat.interval.ms": 1000, // 消费者1秒心跳一次
			"session.timeout.ms":              30000,	// 30秒没有心跳响应则退出
			"go.events.channel.enable":        true,	// 通过管道读取数据
			"go.application.rebalance.enable": true,	// 负载均衡变化反馈给应用处理
			"auto.offset.reset": 	"latest",	// 如果之前没有offset，那么从最新位置开始消费
			"enable.auto.commit":	true, 	// 自动提交offset
			"auto.commit.interval.ms": 1000, 	// 1秒提交一次offset
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
func consumeLoop(consumer *Consumer, client *kafka.Consumer, info *bigpipe.ConsumerInfo) {
	for true {
		select {
		case ev := <-client.Events():
			switch e := ev.(type) {
			case kafka.AssignedPartitions:	// 分配partition
				log.INFO( "%% %v\n", e)
				client.Assign(e.Partitions)
			case kafka.RevokedPartitions:	// 重置partition
				log.INFO("%% %v\n", e)
				client.Unassign()
			case *kafka.Message:
				// 反序列化请求
				if msg, isValid := DecodeMessage(e.Value); isValid {
					fmt.Println(msg)
				}
				log.INFO("%% Message on %s:\n%s\n",
					e.TopicPartition, string(e.Value))
			case kafka.PartitionEOF:
				log.INFO("%% Reached %v\n", e)
			case kafka.Error:
				log.FATAL("%% Error: %v\n", e)
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
			&bigConf.Kafka_consumer_list[i],
		)
	}
	return true
}