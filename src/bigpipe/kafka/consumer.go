package kafka

import (
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"bigpipe/config"
	"bigpipe/log"
	"bigpipe/client"
	"bigpipe/proto"
	"bigpipe/stats"
)

type Consumer struct {
	clients []*kafka.Consumer	// 消费者数组
	httpClients []client.IClient	// HTTP客户端
	termCh chan int 	// 退出通知
	waitCh chan int 	// 退出等待
	bigConf *config.Config // 配置快照
}

// 创建消费者（多个彼此独立）
func CreateConsumer(bigConf *config.Config) (*Consumer, error) {
	consumer := Consumer{
		bigConf: bigConf,
	}
	for _, consumerInfo := range bigConf.Kafka_consumer_list {
		// Kafka 客户端
		cli, err := kafka.NewConsumer(&kafka.ConfigMap{
			"bootstrap.servers":               bigConf.Kafka_bootstrap_servers,
			"group.id":                        consumerInfo.GroupId,
			"heartbeat.interval.ms": 1000, // 消费者1秒心跳一次
			"session.timeout.ms":              30000,	// 30秒没有心跳响应则退出
			"go.events.channel.enable":        true,	// 通过管道读取数据
			"go.events.channel.size":	100,	// 管道尺寸，避免热加载花费太长时间等待
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
		err = cli.SubscribeTopics(topics, nil)
		if err != nil {
			return nil, err
		}
		consumer.clients = append(consumer.clients, cli)

		// Http 客户端
		hCli, herr := client.CreateClient(&consumerInfo)
		if herr != nil {
			return nil, herr
		}
		consumer.httpClients = append(consumer.httpClients, hCli)
	}

	consumer.termCh = make(chan int, 1)
	consumer.waitCh = make(chan int, len(bigConf.Kafka_consumer_list))
	return &consumer, nil
}

func DestroyConsumer(consumer *Consumer) {
	// 通知各个consumer goroutine退出
	close(consumer.termCh)

	// 等待各个consumer goroutine退出
	for i := 0; i < len(consumer.clients); i = i + 1 {
		<- consumer.waitCh
	}
	log.INFO("Consumer关闭成功")
}

// 解析消息，发起http调用
func (consumer *Consumer) handleMessage(value []byte, idx int) {
	stats.ConsumerStats_handleMessage(idx)

	// 反序列化请求
	if msg, isValid := proto.DecodeMessage(value); isValid {
		log.INFO("消费消息:%s", string(value))
		// 发起HTTP调用
		cli := consumer.httpClients[idx]
		cli.Call(msg, consumer.termCh)
	} else {
		stats.ConsumerStats_invalidMessage(idx)
		log.ERROR("消息格式错误:%s", string(value))
	}
}

// 退出前只处理普通消息
func (consumer *Consumer) handleLeftEvent(ev kafka.Event, idx int, info *config.ConsumerInfo) {
	switch e := ev.(type) {
	case *kafka.Message:
		consumer.handleMessage(e.Value, idx)
		log.DEBUG("%% Message on %s:\n%s\n", e.TopicPartition, string(e.Value))
	}
}

// 处理事件
func (consumer *Consumer)handleEvent(ev kafka.Event, idx int, info *config.ConsumerInfo) {
	client := consumer.clients[idx]

	switch e := ev.(type) {
	case kafka.AssignedPartitions:	// 分配partition
		log.DEBUG( "%% %v\n", e)
		client.Assign(e.Partitions)
	case kafka.RevokedPartitions:	// 重置partition
		log.DEBUG("%% %v\n", e)
		client.Unassign()
	case *kafka.Message:
		consumer.handleMessage(e.Value, idx)
		log.DEBUG("%% Message on %s:\n%s\n", e.TopicPartition, string(e.Value))
	case kafka.PartitionEOF:
		log.DEBUG("%% Reached %v\n", e)
	case kafka.Error:
		log.FATAL("%% Error: %v\n", e)
	}
}

// 处理kafka消息
func (consumer *Consumer)consumeLoop(idx int, info *config.ConsumerInfo) {
	client := consumer.clients[idx]
loop:
	for true {
		select {
		case ev := <-client.Events():
			// 处理事件
			consumer.handleEvent(ev, idx, info)
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
			consumer.handleLeftEvent(ev, idx, info)
		default:
			break finalLoop
		}
	}

	// 等待所有异步http调用结束
	for true {
		if consumer.httpClients[idx].PendingCount() == 0 {
			log.DEBUG("Consumer Goroutine（%d）Exit", idx)
			break
		}
	}
	consumer.waitCh <- 1
}

// 为每个消费者启动一个独立的协程
func (consumer *Consumer) Run() bool {
	for i, _ := range consumer.clients {
		go consumer.consumeLoop(
			i,
			&consumer.bigConf.Kafka_consumer_list[i],
		)
	}
	return true
}
