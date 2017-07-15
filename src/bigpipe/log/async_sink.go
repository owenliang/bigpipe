package log

import (
	"time"
	"fmt"
	"os"
)

// 异步日志输出
type asyncSink struct {
	curHour int	// 当前的UNIX小时
	file *os.File	// 文件指针
	logChan chan[]byte	// 日志队列
	termChan chan int 	// 退出事件
	waitChan chan int	// 通知logger可以退出
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
			s.file.Close()
		}
		s.curHour = hour
		s.file = file
	}
}

func (s *asyncSink) close() {
	s.termChan <- 1
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
	s.waitChan <- 1 // 通知logger退出
}

func newAsyncSink(waitChan chan int) *asyncSink {
	sinker := asyncSink{}
	sinker.termChan = make(chan int, 1)
	sinker.logChan = make(chan []byte, 100000)
	sinker.waitChan = waitChan
	go sinker.consumeLog()	// 独立的协程消费管道里的日志
	return &sinker
}
