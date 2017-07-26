package log

import (
	"fmt"
	"time"
	"bigpipe/config"
	"io"
	"unsafe"
	"sync/atomic"
)

// 日志管理
type logger struct {
	sinker ISink	// 日志输出类（接口抽象）
	level int // 日志级别
	directory string  // 存储目录
	waitChan chan int // 等待sink退出
}

// sink接口
type ISink interface {
	io.Writer	// 继承
	close()	// 关闭
}

const (
	LOG_LEVEL_FATAL = 1	// 严重错误
	LOG_LEVEL_ERROR = 2	// 普通错误
	LOG_LEVEL_WARNING = 3	// 警告
	LOG_LEVEL_INFO = 4	// 普通日志
	LOG_LEVEL_DEBUG = 5	// 调试日志
)

// 单例
var gLogger unsafe.Pointer = nil

// 日志级别
var levelStr map[int]string = map[int]string {
	LOG_LEVEL_FATAL: "FATAL",
	LOG_LEVEL_ERROR: "ERROR",
	LOG_LEVEL_WARNING: "WARNING",
	LOG_LEVEL_INFO: "INFO",
	LOG_LEVEL_DEBUG: "DEBUG",
}

func InitLogger(bigConf *config.Config) {
	logger := &logger{}

	logger.level = bigConf.Log_level
	logger.directory = bigConf.Log_directory
	logger.waitChan = make(chan int)

	// 输出器
	logger.sinker = newAsyncSink(logger, logger.waitChan)

	atomic.StorePointer(&gLogger, unsafe.Pointer(logger))
}

func getLogger() *logger {
	return (*logger)(atomic.LoadPointer(&gLogger))
}

func DestroyLogger() {
	// 获取当前logger
	logger := getLogger()
	// 释放logger,不再接收日志
	atomic.StorePointer(&gLogger, nil)
	// 将剩余日志消费完
	if logger.sinker != nil {
		logger.sinker.close()
		<- logger.waitChan
	}
}

func (logger *logger)queueLog(level int, userLog *string) {
	if level > logger.level {
		return
	}
	now := time.Now()
	row := fmt.Sprintf("[%s][%04d-%02d-%02d %02d:%02d:%02d:%03d] %s\n", levelStr[level],
		now.Year(), now.Month(), now.Day(), now.Hour(), now.Minute(), now.Second(), now.Nanosecond() / 1000000, *userLog)
	logger.sinker.Write([]byte(row))
}

func FATAL(format string, v ...interface{}) {
	if logger := getLogger(); logger != nil {
		userLog := fmt.Sprintf(format, v...)
		logger.queueLog(LOG_LEVEL_FATAL, &userLog)
	}
}

func ERROR(format string, v ...interface{}) {
	if logger := getLogger(); logger != nil {
		userLog := fmt.Sprintf(format, v...)
		logger.queueLog(LOG_LEVEL_ERROR, &userLog)
	}
}

func WARNING(format string, v ...interface{}) {
	if logger := getLogger(); logger != nil {
		userLog := fmt.Sprintf(format, v...)
		logger.queueLog(LOG_LEVEL_WARNING, &userLog)
	}
}

func INFO(format string, v ...interface{}) {
	if logger := getLogger(); logger != nil {
		userLog := fmt.Sprintf(format, v...)
		logger.queueLog(LOG_LEVEL_INFO, &userLog)
	}
}

func DEBUG(format string, v ...interface{}) {
	if logger := getLogger(); logger != nil {
		userLog := fmt.Sprintf(format, v...)
		logger.queueLog(LOG_LEVEL_DEBUG, &userLog)
	}
}
