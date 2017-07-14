# bigpipe
一个基于Kafka的中间件，旨在简化服务间异步Http调用的复杂度

# 环境要求
* Kafka >= 0.9
* Go >= 1.8

# 安装依赖
* [confluentinc-kafka-go](https://github.com/confluentinc/confluent-kafka-go)
* [librdkafka](https://github.com/edenhill/librdkafka)

# 调用示例
    curl localhost:10086/rpc/call -d '{"acl": {"name":"system-1","secret":"i am good"},"url":"http://localhost:10086/rpc/server/mock","data":"hello123123123","partition_key": "暂时用不到"}'

# 配置说明
    {
      "log.level": 5,
      "log.directory": "/Users/owenliang/Documents/github/bigpipe/logs",

      "kafka.bootstrap.servers": "localhost:9092",
      "kafka.topics": [
        {"name": "test", "partitions": 3}
      ],

      "kafka.producer.channel.size" : 200000,
      "kafka.producer.retries": 2,
      "kafka.producer.acl": [
        {"name": "system-1", "secret": "you are good", "topic": "test"},
        {"name": "system-2", "secret": "you are bad", "topic": "test"}
      ],

      "kafka.consumer.list": [
        {"topic": "test", "groupId": "G1", "rateLimit": 2000, "timeout": 3000, "retries": 2, "concurrency": 10},
        {"topic": "test", "groupId": "G2", "rateLimit": 2000, "timeout": 3000, "retries": 2, "concurrency": 10}
      ],

      "http.server.port": 10086,
      "http.server.read.timeout": 5000,
      "http.server.write.timeout": 5000
    }

# 工作原理
* server模块：接收异步Http调用
* producer模块：将异步调用序列化，投递到kafka
* log模块：线程安全的异步日志
* config模块：基于json的配置
* client模块：异步http客户端，支持超时、重试、并发控制、流速控制
* consumer模块：读取kafka中的消息，发送给下游

# TODO
* client支持流速控制

# 特别说明
* bigpipe基于rebalanced consumer group工作，可以多进程部署，自动负载均衡
* bigpipe支持优雅退出，不损失任何数据
* bigpipe在正常退出的情况下，保障at least once的投递语义