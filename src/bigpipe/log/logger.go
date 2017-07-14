package log

import (
	"os"
	"log"
	"fmt"
	"time"
	"bigpipe"
	"io"
)

// 日志管理
type logger struct {
	log *log.Logger	// 内部挂载了asyncSink
	sinker iSink	// 日志输出类（接口抽象）
	level int // 日志级别
	directory string  // 存储目录
}

// sink接口
type iSink interface {
	io.Writer	// 继承
	close(*chan int)	// 关闭
}

// 异步日志输出
type asyncSink struct {
	curHour int	// 当前的UNIX小时
	file *os.File	// 文件指针
	logChan chan[]byte	// 日志队列
	termChan chan int 	// 退出事件
}

const (
	LOG_LEVEL_FATAL = 1	// 严重错误
	LOG_LEVEL_ERROR = 2	// 普通错误
	LOG_LEVEL_WARNING = 3	// 警告
	LOG_LEVEL_INFO = 4	// 普通日志
	LOG_LEVEL_DEBUG = 5	// 调试日志
)

// 单例
var gLogger logger

// 日志级别
var levelStr map[int]string = map[int]string {
	LOG_LEVEL_FATAL: "FATAL",
	LOG_LEVEL_ERROR: "ERROR",
	LOG_LEVEL_WARNING: "WARNING",
	LOG_LEVEL_INFO: "INFO",
	LOG_LEVEL_DEBUG: "DEBUG",
}

func InitLogger() {
	bigConf := bigpipe.GetConfig()

	gLogger.level = bigConf.Log_level
	gLogger.directory = bigConf.Log_directory

	// 输出器
	gLogger.sinker = newAsyncSink()
	// 系统log库
	gLogger.log = log.New(gLogger.sinker, "", 0)
}

func DestroyLogger() {
	if gLogger.sinker != nil {
		waitChan := make(chan int, 1)
		gLogger.sinker.close(&waitChan)
		<- waitChan
	}
}

func (logger *logger)queueLog(level int, userLog *string) {
	if level > logger.level {
		return
	}
	now := time.Now()
	logger.log.Printf("[%s][%04d-%02d-%02d %02d:%02d:%02d:%03d] %s", levelStr[level],
		now.Year(), now.Month(), now.Day(), now.Hour(), now.Minute(), now.Second(), now.Nanosecond() / 1000000, *userLog)
}

func (s *asyncSink) Write(p []byte) (n int, err error) {
	s.logChan <- p
	return len(p), nil
}

func (s *asyncSink) rotateFile() {
	now := time.Now()
	hour := int(now.Unix() / 3600)
	if hour != s.curHour {
		filename := fmt.Sprintf("%s/bigpipe.%02d%02d%02d_%02d.log", gLogger.directory, now.Year(), now.Month(), now.Day(), now.Hour())
		file, err := os.OpenFile(filename, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0666)
		if err != nil {
			return
		}
		if s.file != nil {
			file.Close()
		}
		s.curHour = hour
		s.file = file
	}
}

func (s *asyncSink) close(waitChan *chan int) {
	s.termChan <- 1
	*waitChan <- 1 // 通知looger退出
}

func (s *asyncSink) consumeLog() {
	isClose := false

	loop:
	for true {
		select {
			case log := <- s.logChan:
				s.rotateFile()	// 尝试日志滚动
				if s.file != nil {
					s.file.Write(log)
				}
			case <- s.termChan:
				isClose = true
			default:
				// 没有遗留数据,并且已关闭
				if len(s.logChan) == 0 && isClose {
					if s.file != nil {
						s.file.Close()
					}
					break loop
				}
		}
	}
}

func newAsyncSink() *asyncSink {
	sinker := asyncSink{}
	sinker.termChan = make(chan int, 1)
	sinker.logChan = make(chan []byte, 100000)
	go sinker.consumeLog()	// 独立的协程消费管道里的日志
	return &sinker
}

func FATAL(format string, v ...interface{}) {
	userLog := fmt.Sprintf(format, v...)
	gLogger.queueLog(LOG_LEVEL_FATAL, &userLog)
}

func ERROR(format string, v ...interface{}) {
	userLog := fmt.Sprintf(format, v...)
	gLogger.queueLog(LOG_LEVEL_ERROR, &userLog)
}

func WARNING(format string, v ...interface{}) {
	userLog := fmt.Sprintf(format, v...)
	gLogger.queueLog(LOG_LEVEL_WARNING, &userLog)
}

func INFO(format string, v ...interface{}) {
	userLog := fmt.Sprintf(format, v...)
	gLogger.queueLog(LOG_LEVEL_INFO, &userLog)
}

func DEBUG(format string, v ...interface{}) {
	userLog := fmt.Sprintf(format, v...)
	gLogger.queueLog(LOG_LEVEL_DEBUG, &userLog)
}
