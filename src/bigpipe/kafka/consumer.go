package kafka

import "github.com/confluentinc/confluent-kafka-go/kafka"

type Consumer struct {
	clients []*kafka.Consumer	// 消费者数组
}