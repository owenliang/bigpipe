package kafka

import (
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"bigpipe"
	"bigpipe/log"
)

type Consumer struct {
	clients []*kafka.Consumer	// 消费者数组
	termCh chan int 	// 退出通知
	waitCh chan int 	// 退出等待
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

	consumer.termCh = make(chan int, len(bigConf.Kafka_consumer_list))
	consumer.waitCh = make(chan int, len(bigConf.Kafka_consumer_list))
	return &consumer, nil
}

func DestroyConsumer(consumer *Consumer) {
	// 通知各个consumer goroutine退出
	for i := 0; i < len(consumer.clients); i = i + 1 {
		consumer.termCh <- 1
	}
	// 等待各个consumer goroutine退出
	for i := 0; i < len(consumer.clients); i = i + 1 {
		<- consumer.waitCh
	}
	log.INFO("Consumer关闭成功")
}

// 解析消息，发起http调用
func (consumer *Consumer) handleMessage(value []byte) {
	// 反序列化请求
	if msg, isValid := DecodeMessage(value); isValid {
		log.INFO("消费消息:%s", string(value))
		msg = msg
	} else {
		log.ERROR("消息格式错误:%s", string(value))
	}
}

// 退出前只处理普通消息
func (consumer *Consumer) handleLeftEvent(ev kafka.Event, client *kafka.Consumer, info *bigpipe.ConsumerInfo) {
	switch e := ev.(type) {
	case *kafka.Message:
		consumer.handleMessage(e.Value)
		log.INFO("%% Message on %s:\n%s\n", e.TopicPartition, string(e.Value))
	}
}

// 处理事件
func (consumer *Consumer)handleEvent(ev kafka.Event, client *kafka.Consumer, info *bigpipe.ConsumerInfo) {
	switch e := ev.(type) {
	case kafka.AssignedPartitions:	// 分配partition
		log.INFO( "%% %v\n", e)
		client.Assign(e.Partitions)
	case kafka.RevokedPartitions:	// 重置partition
		log.INFO("%% %v\n", e)
		client.Unassign()
	case *kafka.Message:
		consumer.handleMessage(e.Value)
		log.INFO("%% Message on %s:\n%s\n", e.TopicPartition, string(e.Value))
	case kafka.PartitionEOF:
		log.INFO("%% Reached %v\n", e)
	case kafka.Error:
		log.FATAL("%% Error: %v\n", e)
	}
}

// 处理kafka消息
func consumeLoop(consumer *Consumer, client *kafka.Consumer, info *bigpipe.ConsumerInfo) {
loop:
	for true {
		select {
		case ev := <-client.Events():
			// 处理事件
			consumer.handleEvent(ev, client, info)
		case <- consumer.termCh:
			// 终止consumer继续向channel内投放数据
			client.Close()
			break loop
		}
	}

	// 消费掉管道内剩余的events
finalLoop:
	for true {
		select {
		case ev := <-client.Events():
			// 处理事件
			consumer.handleLeftEvent(ev, client, info)
		default:
			break finalLoop
		}
	}

	// TODO: 等待所有http调用结束

	consumer.waitCh <- 1
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
