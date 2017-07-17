package main

import (
	"bigpipe/server"
	"bigpipe/kafka"
	"bigpipe/config"
	"os"
	"os/signal"
	"syscall"
	"runtime"
	"bigpipe/log"
	"flag"
	"fmt"
	"bigpipe/stats"
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

var (
	configFile string	// 配置文件路径
)

func initCmd() {
	// 配置文件
	flag.StringVar(&configFile, "config", "./conf/bigpipe.json", "abs path to bigpipe.json")
	// 解析命令行参数
	flag.Parse()
}

func main() {
	// 初始化命令行参数
	initCmd()

	// 初始化运行环境
	initEnv()

	// 加载配置
	if !config.LoadConfig(configFile) {
		fmt.Println("配置文件加载失败:" + configFile)
		os.Exit(-1)
	}

	// 启动日志
	log.InitLogger()

	// 初始化统计
	stats.InitStats()

	// 创建kafka生产者
	producer, errp := kafka.CreateProducer()
	if errp != nil {
		log.ERROR("创建kafka producer失败")
		os.Exit(-1)
	}
	log.INFO("启动kafka producer成功")

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
	log.INFO("启动kafka consumer成功")

	// 创建http处理器
	handler := server.CreateHandler(producer)

	// 创建http服务端
	srv := server.CreateServer(handler)

	// 启动http服务端
	if !srv.Run() {
		log.ERROR("启动http服务器失败")
		os.Exit(-1)
	}
	log.INFO("启动http服务器成功")

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

	os.Exit(0)
}