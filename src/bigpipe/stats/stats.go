package stats

import (
	"bigpipe/config"
	"sync/atomic"
	"unsafe"
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

	// 配置文件快照
	bigConf *config.Config
}

// 单例
var gStats unsafe.Pointer = nil

func InitStats(bigConf *config.Config) {
	stats := &stats{
		producerStats: make(map[string]*ProducerStats),
		consumerStats: make(map[int]*ConsumerStats),
		clientStats: make(map[string]*ClientStats),
		bigConf : bigConf,
	}

	// 生产者初始化
	for _, acl := range bigConf.Kafka_producer_acl {
		stats.producerStats[acl.Topic] = &ProducerStats{}
	}
	for idx, consumerInfo := range bigConf.Kafka_consumer_list {
		// 消费者初始化
		stats.consumerStats[idx] = &ConsumerStats{}
		// 客户端初始化
		stats.clientStats[consumerInfo.Topic] = &ClientStats{}
	}

	atomic.StorePointer(&gStats, unsafe.Pointer(stats))
}

func DestroyStats() {
	atomic.StorePointer(&gStats, nil)
}

func getStats() *stats {
	return (*stats)(atomic.LoadPointer(&gStats))
}

// 输出json格式统计信息
func StatsInfo() (interface{}){
	stats := getStats()
	if stats == nil { // stats已关闭
		return make(map[string]interface{})
	}

	info := make(map[string]interface{})

	// 服务端统计
	serverStats := make(map[string]interface{})
	serverStats["receivedCall"] = atomic.LoadInt64(&stats.serverStats.receivedCall)
	serverStats["acceptedCall"] = atomic.LoadInt64(&stats.serverStats.acceptedCall)
	serverStats["overloadCall"] = atomic.LoadInt64(&stats.serverStats.overloadCall)
	info["serverStats"] = serverStats

	// 生产者统计
	producerStats := make(map[string]interface{})
	for topic, item := range stats.producerStats {
		stats := make(map[string]interface{})
		stats["deliverySuccess"] = atomic.LoadInt64(&item.deliverySuccess)
		stats["deliveryFail"] = atomic.LoadInt64(&item.deliveryFail)
		producerStats[topic] = stats
	}
	info["producerStats"] = producerStats

	// 消费者统计
	consumerStats := make([]interface{}, 0)
	for idx, item := range stats.consumerStats {
		subStats := make(map[string]interface{})
		subStats["topic"] = stats.bigConf.Kafka_consumer_list[idx].Topic
		subStats["groupId"] = stats.bigConf.Kafka_consumer_list[idx].GroupId
		subStats["handleMessage"] = atomic.LoadInt64(&item.handleMessage)
		subStats["invalidMessage"] = atomic.LoadInt64(&item.invalidMessage)
		consumerStats = append(consumerStats, subStats)
	}
	info["consumerStats"] = consumerStats

	// 客户端统计
	clientStats := make(map[string]interface{})
	for topic, item := range stats.clientStats {
		subStats := make(map[string]interface{})
		subStats["rpcTotal"] = atomic.LoadInt64(&item.rpcTotal)
		subStats["rpcSuccess"] = atomic.LoadInt64(&item.rpcSuccess)
		subStats["rpcFail"] = atomic.LoadInt64(&item.rpcFail)
		subStats["rpcRetries"] = atomic.LoadInt64(&item.rpcRetries)
		clientStats[topic] = subStats
	}
	info["clientStats"] = clientStats
	return info
}

// 服务端统计函数
func ServerStats_receivedCall() {
	if stats := getStats(); stats != nil {
		atomic.AddInt64(&stats.serverStats.receivedCall, 1)
	}
}
func ServerStats_acceptedCall() {
	if stats := getStats(); stats != nil {
		atomic.AddInt64(&stats.serverStats.acceptedCall, 1)
	}
}
func ServerStats_overloadCall() {
	if stats := getStats(); stats != nil {
		atomic.AddInt64(&stats.serverStats.overloadCall, 1)
	}
}

// 生产者统计函数
func ProducerStats_deliverySuccess(topic *string) {
	if stats := getStats(); stats != nil {
		atomic.AddInt64(&stats.producerStats[*topic].deliverySuccess, 1)
	}
}
func ProducerStats_deliveryFail(topic *string) {
	if stats := getStats(); stats != nil {
		atomic.AddInt64(&stats.producerStats[*topic].deliveryFail, 1)
	}
}

// 消费者统计函数
func ConsumerStats_handleMessage(idx int) {
	if stats := getStats(); stats != nil {
		atomic.AddInt64(&stats.consumerStats[idx].handleMessage, 1)
	}
}
func ConsumerStats_invalidMessage(idx int) {
	if stats := getStats(); stats != nil {
		atomic.AddInt64(&stats.consumerStats[idx].invalidMessage, 1)
	}
}

// 客户端统计
func ClientStats_rpcTotal(topic *string) {
	if stats := getStats(); stats != nil {
		atomic.AddInt64(&stats.clientStats[*topic].rpcTotal, 1)
	}
}
func ClientStats_rpcSuccess(topic *string) {
	if stats := getStats(); stats != nil {
		atomic.AddInt64(&stats.clientStats[*topic].rpcSuccess, 1)
	}
}
func ClientStats_rpcFail(topic *string) {
	if stats := getStats(); stats != nil {
		atomic.AddInt64(&stats.clientStats[*topic].rpcFail, 1)
	}
}
func ClientStats_rpcRetries(topic *string) {
	if stats := getStats(); stats != nil {
		atomic.AddInt64(&stats.clientStats[*topic].rpcRetries, 1)
	}
}
