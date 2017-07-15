package client

import (
	"time"
	"sync"
	"math"
)

// 令牌桶限
type TokenBucket struct {
	lastFillTime time.Time	// 上一次填充时间
	tokenPerNano	float64 	// 每纳秒填充的令牌个数
	tokenCount float64	// 剩余令牌数量
	capacity float64 // 桶容量
	mutex sync.Mutex	// 线程安全
}

// 创建限制速率的令牌桶
func CreateBucketForRate(rate float64) *TokenBucket {
	r := TokenBucket{}
	r.tokenPerNano = rate / 1e9
	r.lastFillTime = time.Now()
	r.tokenCount = rate
	r.capacity = rate
	return &r
}

// 阻塞版本
func (bucket *TokenBucket) getToken(count int) {
	needWait, waitTime := bucket.getTokenWithoutBlock(count)
	if needWait {
		time.Sleep(waitTime)
	}
}

// 非阻塞版本
func (bucket *TokenBucket) getTokenWithoutBlock(count int) (needWait bool, waitTime time.Duration) {
	bucket.mutex.Lock()
	defer bucket.mutex.Unlock()

	// 填充令牌
	bucket.tryFillBucket()

	// 令牌足够
	if bucket.tokenCount >= float64(count) {
		bucket.tokenCount -= float64(count)
		return false, 0
	}

	// 需要等待的时间（浮点数不精确,取绝对值）
	waitTime = time.Duration( math.Abs( (float64(count) - bucket.tokenCount) / bucket.tokenPerNano) )
	// 更新令牌数量（先到先得）
	bucket.tokenCount -= float64(count)
	return true, waitTime
}

// 补充令牌
func (bucket *TokenBucket) tryFillBucket() {
	now := time.Now()
	passedTime := now.Sub(bucket.lastFillTime)

	bucket.tokenCount += float64(passedTime.Nanoseconds()) * bucket.tokenPerNano
	if bucket.tokenCount > bucket.capacity {
		bucket.tokenCount = bucket.capacity
	}

	bucket.lastFillTime = now
}