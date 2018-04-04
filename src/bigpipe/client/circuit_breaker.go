package client

import (
	"bigpipe/util"
	"bigpipe/config"
	"sync"
	"math/rand"
)

// 每秒一个桶, 记录该秒的请求成功、失败次数
type StatsBucket struct {
	success int
	fail int
}

// 健康统计, 维护最近N秒的滑动窗口
type HealthStats struct {
	buckets []StatsBucket // 滑动窗口, 每个桶1秒
	curTime int64 // 当前窗口末尾的秒级unix时间戳
	minStats int // 少于该打点数量直接返回健康
	healthRate float64 // 健康阀值
}

// 熔断器状态
type CircuitStatus int
const (
	CIRCUIT_NORMAL CircuitStatus = 1	// 正常
	CIRCUIT_BREAK = 2	// 熔断
	CIRCUIT_RECOVER = 3	// 恢复中
)

// 熔断器
type CircuitBreaker struct {
	mutex sync.Mutex
	healthStats *HealthStats // 健康统计
	status CircuitStatus // 熔断状态
	breakTime int64 // 熔断的时间点(秒)
	breakPeriod int // 熔断封锁时间
	recoverPeriod int // 熔断恢复时间
}

// 创建健康统计器
func createHealthStats(info *config.CircuitBreakerInfo) (healthStats *HealthStats) {
	healthStats = &HealthStats{
		minStats: info.MinStats,
		healthRate: info.HealthRate,
	}

	healthStats.buckets = make([]StatsBucket, info.WinSize)
	healthStats.resetBuckets(healthStats.buckets[:])

	healthStats.curTime = util.CurUnixSecond()
	return
}

// 重置桶状态
func (healthStats *HealthStats) resetBuckets(buckets []StatsBucket) {
	for idx, _ := range buckets {
		buckets[idx].success = 0
		buckets[idx].fail = 0
	}
}

// 窗口滑动
func (healthStats *HealthStats) shiftBuckets() {
	now := util.CurUnixSecond()

	timeDiff := int(now - healthStats.curTime)
	if timeDiff <= 0 {
		return
	}

	if timeDiff >= len(healthStats.buckets) {
		healthStats.resetBuckets(healthStats.buckets[:])
	} else {
		healthStats.buckets = append(healthStats.buckets[:0], healthStats.buckets[timeDiff:]...)
		for i := 0; i < timeDiff; i++ {
			healthStats.buckets = append(healthStats.buckets, StatsBucket{})
		}
	}

	healthStats.curTime = now
}

// 成功打点
func (healthStats *HealthStats) success() {
	healthStats.shiftBuckets()

	healthStats.buckets[len(healthStats.buckets) - 1].success++
}

// 失败打点
func (healthStats *HealthStats) fail() {
	healthStats.shiftBuckets()

	healthStats.buckets[len(healthStats.buckets) - 1].fail++
}

// 判断是否健康
func (healthStats *HealthStats) isHealthy() (bool, float64) {
	healthStats.shiftBuckets()

	success := 0
	fail := 0
	for _, bucket := range healthStats.buckets {
		success += bucket.success
		fail += bucket.fail
	}

	total := success + fail
	// 没有样本
	if total == 0 {
		return true, 1
	}

	rate :=  (float64(success) / float64(total))
	// 样本不足
	if total < healthStats.minStats {
		return true, rate
	}
	// 样本充足
	return rate >= healthStats.healthRate, rate
}

// 创建熔断器
func CreateCircuitBreaker(info *config.CircuitBreakerInfo) (circuitBreaker *CircuitBreaker)  {
	circuitBreaker = &CircuitBreaker{
		healthStats: createHealthStats(info),
		status: CIRCUIT_NORMAL,
		breakTime: 0,
		breakPeriod: info.BreakPeriod,
		recoverPeriod: info.RecoverPeriod,
	}
	return
}

func (circuitBreaker *CircuitBreaker) Success() {
	circuitBreaker.mutex.Lock()
	defer circuitBreaker.mutex.Unlock()

	circuitBreaker.healthStats.success()
}

func (circuitBreaker *CircuitBreaker) Fail()  {
	circuitBreaker.mutex.Lock()
	defer circuitBreaker.mutex.Unlock()

	circuitBreaker.healthStats.fail()
}

// 熔断器判定
func (circuitBreaker *CircuitBreaker) IsBreak() (isBreak bool, isHealthy bool, healthRate float64) {
	circuitBreaker.mutex.Lock()
	defer circuitBreaker.mutex.Unlock()

	now := util.CurUnixSecond()
	breakLastTime := now - circuitBreaker.breakTime

	isHealthy, healthRate = circuitBreaker.healthStats.isHealthy()

	isBreak = false

	switch circuitBreaker.status {
	case CIRCUIT_NORMAL:
		if !isHealthy {
			circuitBreaker.status = CIRCUIT_BREAK
			circuitBreaker.breakTime = now
			isBreak = true
		}
	case CIRCUIT_BREAK:
		if breakLastTime < int64(circuitBreaker.breakPeriod) || !isHealthy {
			isBreak = true
		} else {
			circuitBreaker.status = CIRCUIT_RECOVER
		}
	case CIRCUIT_RECOVER:
		if !isHealthy {
			circuitBreaker.status = CIRCUIT_BREAK
			circuitBreaker.breakTime = now
			isBreak = true
		} else {
			if breakLastTime >= int64(circuitBreaker.breakPeriod + circuitBreaker.recoverPeriod) {
				circuitBreaker.status = CIRCUIT_NORMAL
			} else {
				passRate := float64(breakLastTime) /  float64(circuitBreaker.breakPeriod + circuitBreaker.recoverPeriod)
				if rand.Float64() > passRate {
					isBreak = true
				}
			}
		}
	}
	return
}