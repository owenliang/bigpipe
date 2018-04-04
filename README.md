# bigpipe
一个基于Kafka的中间件，旨在简化服务间异步Http调用的复杂度

[![Build Status](https://travis-ci.org/owenliang/bigpipe.svg?branch=master)](https://travis-ci.org/owenliang/bigpipe)

# 功能更新

* 2018-04-04：增加了consumer粒度的熔断器，防止下游不可用导致大量流量持续损失，这是一个可选功能

# 环境要求
* Kafka >= 0.9
* Go >= 1.8

# 特性
* 横向扩展：完全支持rebalanced-consumer-group，多进程部署即可实现partition自动负载均衡
* 配置热加载：在线服务无需重启即可加载配置，流量0损失
* 优雅退出：处理完剩余任务后退出，流量0损失
* 超强性能：无锁，协程并发，单进程即可充分利用多核，满足一般流量需求
* 容错机制：对下游实时限速、限并发、熔断三种保护机制

# 安装依赖
* [librdkafka（必须为0.9.5版本，编译时指定--prefix=/usr）](https://github.com/edenhill/librdkafka/releases/tag/v0.9.5)

# GO包依赖
* [confluentinc-kafka-go（无需自行下载，当前依赖为0.9.4版本）](https://github.com/confluentinc/confluent-kafka-go)

# 编译方法
* 设置GOPATH为项目根目录
* 进入GOPATH目录，安装glide包管理：mkdir -p bin && curl https://glide.sh/get | sh
* 执行sh build.sh

# 潜在的安装问题
* 编译bigpipe时候可能遇到如下报错，需要export PKG_CONFIG_PATH=/usr/lib/pkgconfig
    pkg-config --cflags rdkafka
    Package rdkafka was not found in the pkg-config search path.
    Perhaps you should add the directory containing `rdkafka.pc'
    to the PKG_CONFIG_PATH environment variable
    No package 'rdkafka' found
* 运行bigpipe时候可能遇到找不到符号librdkafka符号的问题，那是因为编译librdkafka没有指定--prefix=/usr，可以重新编译安装或者export LD_LIBRARY_PATH=/usr/local/lib

# 使用方法
* 运行：./bigpipe -config /path/to/bigpipe.json
* 退出：killall bigpipe
* 热加载：killall -USR1 bigpipe

# 调用示例
异步调用
    
    curl localhost:10086/rpc/call -d '{"acl": {"name":"system-1","secret":"i am good"},"url":"http://localhost:10086/rpc/server/mock","data":"hello123123123","partition_key": "暂时用不到"}'
    
    {
        "data": "",
        "errno": 0,
        "msg": "发送成功"
    }

接收端(PHP为例)

    $data = file_get_contents("php://input"); // $data的值为hello123123123

统计信息
    
    curl localhost:10086/stats
    
    {
        "data": {
            "clientStats": {
                "test": {
                    "rpcFail": 0,
                    "rpcRetries": 0,
                    "rpcSuccess": 22,
                    "rpcTotal": 22
                }
            },
            "consumerStats": [
                {
                    "groupId": "G1",
                    "handleMessage": 11,
                    "invalidMessage": 0,
                    "topic": "test"
                },
                {
                    "groupId": "G2",
                    "handleMessage": 11,
                    "invalidMessage": 0,
                    "topic": "test"
                }
            ],
            "producerStats": {
                "test": {
                    "deliveryFail": 0,
                    "deliverySuccess": 11
                }
            },
            "serverStats": {
                "acceptedCall": 11,
                "overloadCall": 0,
                "receivedCall": 11
            }
        },
        "errno": 0,
        "msg": "success"
    }

# 配置说明
    {
      "log.level": 5,
      "log.directory": "./logs",
    
      "kafka.bootstrap.servers": "localhost:9092",
      "kafka.topics": [
        {"name": "test", "partitions": 3}
      ],
    
      "kafka.producer.retries": 2,
      "kafka.producer.acl": [
        {"name": "system-1", "secret": "you are good", "topic": "test"},
        {"name": "system-2", "secret": "you are bad", "topic": "test"}
      ],
    
      "kafka.consumer.list": [
        {"topic": "test", "groupId": "G1", "rateLimit": 100, "timeout": 3000, "retries": 2, "concurrency": 5, "circuitBreaker": {"breakPeriod": 10, "recoverPeriod": 30, "winSize": 60, "minStats": 100, "healthRate": 0.85}},
        {"topic": "test", "groupId": "G2", "rateLimit": 100, "timeout": 3000, "retries": 2, "concurrency": 5}
      ],
    
      "http.server.port": 10086,
      "http.server.handler.channel.size": 500000,
      "http.server.read.timeout": 5000,
      "http.server.write.timeout": 5000
    }

下面加粗的配置项，支持热加载。
* **log.level**：日志级别，FATAL=1|ERROR=2|WARNING=3|INFO=4|DEBUG=5
* **log.directory**：日志存储路径，目录必须存在
* **kafka.bootstrap.servers**： kafka服务器列表，逗号分隔
* **kafka.topics**：允许读写的topic列表
* **kafka.producer.retries**：投递消息到kafka的重试次数
* **kafka.producer.acl**：投递消息的权限账号/密码，调用者必须符合其中的某条规则
* **kafka.producer.acl.name**：权限账号
* **kafka.producer.acl.secret**：权限密码
* **kafka.producer.acl.name.topic**：授权的topic名称
* **kafka.consumer.list**：kafka消费组列表
* **kafka.consumer.list.topic**：消费的topic
* **kafka.consumer.list.groupId**：消费组id，相同topic下相同groupId将彼此进行rebalanced负载均衡
* **kafka.consumer.list.rateLimit**：流速限制，即每秒的最大调用次数
* **kafka.consumer.list.timeout**：超时时间，即每个调用最大等待应答的时间（毫秒）
* **kafka.consumer.list.retries**：重试次数，即每个调用连续失败的最大次数
* **kafka.consumer.list.concurrency**：并发限制，即同一时刻最多并发的请求个数
* **kafka.consumer.list.circuitBreaker**: 熔断器，可选
* **kafka.consumer.list.circuitBreaker.breakPeriod**: 熔断冻结时间（此期间不透过任何请求），单位秒
* **kafka.consumer.list.circuitBreaker.recoverPeriod**: 熔断慢恢复时间（此期间逐渐增大放量请求），单位秒
* **kafka.consumer.list.circuitBreaker.winSize**: 统计滑动窗口（检查这段窗口时间内的成功率），单位秒
* **kafka.consumer.list.circuitBreaker.minStats**: 滑动窗口内样本少于此数值则直接认为服务健康
* **kafka.consumer.list.circuitBreaker.healthRate**: 统计的健康阀值，区间[0,1]，成功率低于此值认为服务不健康

* http.server.port：服务端监听地址，http协议
* http.server.handler.channel.size：收到的异步调用缓冲队列大小，堆积超过队列大小将返回请求失败
* http.server.read.timeout：服务端读取请求的超时（毫秒）
* http.server.write.timeout：服务端发送应答的超时（毫秒）


# 工作原理
* server模块：接收异步Http调用
* handler模块：处理Http请求并传递给producer
* producer模块：将异步调用序列化，投递到kafka
* log模块：线程安全的异步日志
* config模块：基于json的配置
* client模块：异步http客户端，支持超时、重试、并发控制、流速控制
* consumer模块：读取kafka中的消息，发送给下游
* stats模块：基于原子变量的程序统计

# 运维建议
* 关于扩展性：kafka topic预分配足够的partition，保证bigpipe可横向扩展
* 关于可用性：bigpipe至少部署2个等价节点，利用lvs/haproxy负载均衡，或者客户端负载均衡

# 参考文档
[bigpipe设计PPT](https://gitlab-team.smzdm.com/smzdm/bigpipe/raw/master/doc/bigpipe%E5%88%86%E4%BA%AB.pptx)
