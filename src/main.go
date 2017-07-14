package main

import (
	"bigpipe/server"
	"bigpipe/kafka"
	"os"
	"os/signal"
	"syscall"
	"runtime"
	"bigpipe"
	"bigpipe/log"
)

func waitSignal() {
	c := make(chan os.Signal, 10)
	// 通过channel接收信号
	signal.Notify(c, syscall.SIGINT, syscall.SIGTERM)

	loop:
	for {
		select {
			case <- c :
				log.INFO("收到退出命令,开始优雅退出")
				break loop
		}
	}
}

func initEnv() {
	// 开启多核
	runtime.GOMAXPROCS(runtime.NumCPU())
}

func main() {
	// 初始化运行环境
	initEnv()

	// 加载配置
	if !bigpipe.LoadConfig("/Users/smzdm/Documents/github/bigpipe/conf/bigpipe.json") {
		os.Exit(-1)
	}

	// 启动日志
	log.InitLogger()

	// 创建kafka生产者
	producer, errp := kafka.CreateProducer()
	if errp != nil {
		log.ERROR("创建kafka producer失败")
		os.Exit(-1)
	}

	// 创建kafka消费者
	consumer, errc := kafka.CreateConsumer()
	if errc != nil {
		log.ERROR("创建kafka consumer失败")
		os.Exit(-1)
	}
	// 启动kafka消费者
	if !consumer.Run() {
		log.ERROR("启动kafka生产者失败")
		os.Exit(-1);
	}

	// 创建http处理器
	handler := server.CreateHandler(producer)

	// 创建http服务端
	srv := server.CreateServer(handler)

	// 启动http服务端
	go srv.Run()

	log.INFO("bigpipe启动成功")

	// 等待命令行信号
	waitSignal()

	// 关闭server
	server.DestroyServer(srv)

	// 等待并销毁producer
	kafka.DestroyProducer(producer)

	// 等待并销毁consumer
	kafka.DestroyConsumer(consumer)

	log.INFO("bigpipe结束运行")

	// 关闭日志
	log.DestroyLogger()
}