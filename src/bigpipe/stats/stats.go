package stats

import (
	"bigpipe"
	"sync/atomic"
)

// 服务端统计
type ServerStats struct {
	receivedCall int64	 	// 总调用次数
	acceptedCall int64		// 最终接受的调用次数
	overloadCall int64		// 超过负载的调用次数
}

// 生产者统计
type ProducerStats struct {
	deliverySuccess int64	// 投递成功次数
	deliveryFail int64	// 投递失败次数
}

// 消费者统计
type ConsumerStats struct {
	handleMessage int64 // 处理消息数量
	invalidMessage int64 // 无效的消息数量（<=handleMessage）
}

// 客户端统计
type ClientStats struct {
	rpcTotal int64 // 调用总次数（rpcSuccess+rpcFail）
	rpcSuccess int64 // 调用成功次数
	rpcFail int64 // 调用失败次数
	rpcRetries int64 // 调用重试次数（>=rpcFail）
}

type stats struct {
	// 服务端统计
	serverStats ServerStats

	// 生产者统计（按topic统计）
	producerStats map[string]*ProducerStats

	// 消费者统计（每个消费者独立统计）
	consumerStats map[int]*ConsumerStats

	// 客户端统计（按topic统计）
	clientStats map[string]*ClientStats
}

// 单例
var gStats stats

func InitStats() {
	bigConf := bigpipe.GetConfig()

	gStats = stats{
		producerStats: make(map[string]*ProducerStats),
		consumerStats: make(map[int]*ConsumerStats),
		clientStats: make(map[string]*ClientStats),
	}

	// 生产者初始化
	for _, acl := range bigConf.Kafka_producer_acl {
		gStats.producerStats[acl.Topic] = &ProducerStats{}
	}
	for idx, consumerInfo := range bigConf.Kafka_consumer_list {
		// 消费者初始化
		gStats.consumerStats[idx] = &ConsumerStats{}
		// 客户端初始化
		gStats.clientStats[consumerInfo.Topic] = &ClientStats{}
	}
}

// 输出json格式统计信息
func StatsInfo() (interface{}){
	bigConf := bigpipe.GetConfig()

	info := make(map[string]interface{})

	// 服务端统计
	serverStats := make(map[string]interface{})
	serverStats["receivedCall"] = atomic.LoadInt64(&gStats.serverStats.receivedCall)
	serverStats["acceptedCall"] = atomic.LoadInt64(&gStats.serverStats.acceptedCall)
	serverStats["overloadCall"] = atomic.LoadInt64(&gStats.serverStats.overloadCall)
	info["serverStats"] = serverStats

	// 生产者统计
	producerStats := make(map[string]interface{})
	for topic, item := range gStats.producerStats {
		stats := make(map[string]interface{})
		stats["deliverySuccess"] = atomic.LoadInt64(&item.deliverySuccess)
		stats["deliveryFail"] = atomic.LoadInt64(&item.deliveryFail)
		producerStats[topic] = stats
	}
	info["producerStats"] = producerStats

	// 消费者统计
	consumerStats := make([]interface{}, 0)
	for idx, item := range gStats.consumerStats {
		stats := make(map[string]interface{})
		stats["topic"] = bigConf.Kafka_consumer_list[idx].Topic
		stats["groupId"] = bigConf.Kafka_consumer_list[idx].GroupId
		stats["handleMessage"] = atomic.LoadInt64(&item.handleMessage)
		stats["invalidMessage"] = atomic.LoadInt64(&item.invalidMessage)
		consumerStats = append(consumerStats, stats)
	}
	info["consumerStats"] = consumerStats

	// 客户端统计
	clientStats := make(map[string]interface{})
	for topic, item := range gStats.clientStats {
		stats := make(map[string]interface{})
		stats["rpcTotal"] = atomic.LoadInt64(&item.rpcTotal)
		stats["rpcSuccess"] = atomic.LoadInt64(&item.rpcSuccess)
		stats["rpcFail"] = atomic.LoadInt64(&item.rpcFail)
		stats["rpcRetries"] = atomic.LoadInt64(&item.rpcRetries)
		clientStats[topic] = stats
	}
	info["clientStats"] = clientStats
	return info
}

// 服务端统计函数
func ServerStats_receivedCall() {
	atomic.AddInt64(&gStats.serverStats.receivedCall, 1)
}
func ServerStats_acceptedCall() {
	atomic.AddInt64(&gStats.serverStats.acceptedCall, 1)
}
func ServerStats_overloadCall() {
	atomic.AddInt64(&gStats.serverStats.overloadCall, 1)
}

// 生产者统计函数
func ProducerStats_deliverySuccess(topic *string) {
	atomic.AddInt64(&gStats.producerStats[*topic].deliverySuccess, 1)
}
func ProducerStats_deliveryFail(topic *string) {
	atomic.AddInt64(&gStats.producerStats[*topic].deliveryFail, 1)
}

// 消费者统计函数
func ConsumerStats_handleMessage(idx int) {
	atomic.AddInt64(&gStats.consumerStats[idx].handleMessage, 1)
}
func ConsumerStats_invalidMessage(idx int) {
	atomic.AddInt64(&gStats.consumerStats[idx].invalidMessage, 1)
}

// 客户端统计
func ClientStats_rpcTotal(topic *string) {
	atomic.AddInt64(&gStats.clientStats[*topic].rpcTotal, 1)
}
func ClientStats_rpcSuccess(topic *string) {
	atomic.AddInt64(&gStats.clientStats[*topic].rpcSuccess, 1)
}
func ClientStats_rpcFail(topic *string) {
	atomic.AddInt64(&gStats.clientStats[*topic].rpcFail, 1)
}
func ClientStats_rpcRetries(topic *string) {
	atomic.AddInt64(&gStats.clientStats[*topic].rpcRetries, 1)
}
