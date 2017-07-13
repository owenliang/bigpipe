package main

import (
	"bigpipe/server"
	"bigpipe/kafka"
	"os"
	"os/signal"
	"syscall"
	"fmt"
	"runtime"
	"bigpipe"
)

func waitSignal() {
	c := make(chan os.Signal, 10)
	// 通过channel接收信号
	signal.Notify(c, syscall.SIGINT, syscall.SIGTERM)
	for {
		select {
			case <- c :
				fmt.Println("bye, bigpipe ")
				os.Exit(0)
		}
	}
}

func initEnv() {
	// 开启多核
	runtime.GOMAXPROCS(runtime.NumCPU())
}

func main() {
	initEnv()

	if !bigpipe.LoadConfig("/Users/smzdm/Documents/github/bigpipe/conf/bigpipe.json") {
		os.Exit(-1)
	}

	// 创建kafka生产者
	producer, err := kafka.CreateProducer()
	if err != nil {
		os.Exit(-1)
	}

	// 创建http处理器
	handler := server.CreateHandler(producer)

	// 创建http服务端
	srv := server.CreateServer(handler)

	// 启动http服务端
	go srv.Run()

	// 等待命令行信号
	waitSignal()
}