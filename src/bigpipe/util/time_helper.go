package util

import "time"

// Unix时间戳
func CurUnixSecond() int64 {
	return time.Now().Unix()
}