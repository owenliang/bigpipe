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

func reload() {
	// 1, 解析最新配置
	bigConf := config.ParseConfig(configFile)
	if bigConf == nil {
		log.ERROR("[热重启]解析配置失败")
		return
	}

	log.INFO("[热重启]解析配置完成")

	// 暂停handler向producer转发流量
	log.INFO("[热重启]暂停handler")
	gHandler.Reloading(bigConf)

	// 释放生产者
	log.INFO("[热重启]关闭kafka生产者")
	kafka.DestroyProducer(gProducer)

	// 释放消费者
	log.INFO("[热重启]关闭kafka消费者")
	kafka.DestroyConsumer(gConsumer)

	// 关闭stats
	log.INFO("[热重启]关闭stats统计")
	stats.DestroyStats()

	// 关闭日志
	log.INFO("[热重启]关闭log日志")
	log.DestroyLogger()

	// 启动log
	log.INFO("[热重启]启动log日志")
	log.InitLogger(bigConf)

	// 启动stats
	log.INFO("[热重启]启动stats统计")
	stats.InitStats(bigConf)

	// 启动生产者
	var err error = nil
	gProducer, err = kafka.CreateProducer(bigConf)
	if err != nil {
		log.ERROR("[热重启]启动kafka producer失败")
		os.Exit(-1)
	}
	log.INFO("[热重启]创建kafka producer成功")

	// 恢复handler工作
	log.INFO("[热重启]恢复handler成功")
	gHandler.ReloadDone(gProducer)

	// 创建消费者
	gConsumer, err = kafka.CreateConsumer(bigConf) // 消费者
	if err != nil {
		log.ERROR("[热重启]创建kafka consumer失败")
		os.Exit(-1)
	}

	// 启动消费者
	if !gConsumer.Run() {
		log.ERROR("[热重启]启动kafka consumer失败")
		os.Exit(-1);
	}
	log.INFO("[热重启]启动kafka consumer成功")
}

func waitSignal() {
	c := make(chan os.Signal, 10)
	// 通过channel接收信号
	signal.Notify(c, syscall.SIGINT, syscall.SIGTERM, syscall.SIGUSR1)

	loop:
	for {
		select {
			case signal := <- c :
				// 优雅退出进程
				if signal == syscall.SIGINT || signal == syscall.SIGTERM {
					log.INFO("收到退出命令,开始优雅退出")
					break loop
				} else { // 在线热重启(毫秒级流量损失)
					reload()
				}
		}
	}
}

func initEnv() {
	// 开启多核
	runtime.GOMAXPROCS(runtime.NumCPU())
}

var (
	configFile string	// 配置文件路径
	gSrv *server.Server // 服务器
	gHandler *server.Handler // 处理器
	gProducer *kafka.Producer // 生产者
	gConsumer *kafka.Consumer // 消费者
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
	bigConf := config.ParseConfig(configFile)
	if bigConf == nil {
		fmt.Println("配置文件加载失败:" + configFile)
		os.Exit(-1)
	}

	// 启动日志
	log.InitLogger(bigConf)

	// 初始化统计
	stats.InitStats(bigConf)

	var err error = nil

	// 创建kafka生产者
	gProducer, err = kafka.CreateProducer(bigConf)
	if err != nil {
		fmt.Println("创建kafka producer失败:%v", err)
		os.Exit(-1)
	}
	log.INFO("启动kafka producer成功")

	// 创建kafka消费者
	gConsumer, err = kafka.CreateConsumer(bigConf)
	if err != nil {
		fmt.Println("创建kafka consumer失败:%v", err)
		os.Exit(-1)
	}
	// 启动kafka消费者
	if !gConsumer.Run() {
		fmt.Println("启动kafka生产者失败")
		os.Exit(-1);
	}
	log.INFO("启动kafka consumer成功")

	// 创建http处理器
	gHandler = server.CreateHandler(gProducer, bigConf)

	// 创建http服务端
	gSrv = server.CreateServer(gHandler, bigConf)

	// 启动http服务端
	if !gSrv.Run() {
		fmt.Println("启动http服务器失败")
		os.Exit(-1)
	}
	log.INFO("启动http服务器成功")

	log.INFO("bigpipe启动成功")

	// 等待命令行信号
	waitSignal()

	// 关闭server
	server.DestroyServer(gSrv)

	// 关闭处理器
	server.DestroyHandler(gHandler)

	// 等待并销毁producer
	kafka.DestroyProducer(gProducer)

	// 等待并销毁consumer
	kafka.DestroyConsumer(gConsumer)

	log.INFO("bigpipe结束运行")

	// 关闭日志
	log.DestroyLogger()

	os.Exit(0)
}
