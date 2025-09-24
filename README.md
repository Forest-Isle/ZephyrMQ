# ZephyrMQ - 自研消息队列

ZephyrMQ 是一个综合了 RabbitMQ、Kafka、RocketMQ 优点的自研消息队列组件，使用 Java 实现。

## 🚀 项目特点

- **高性能**: 借鉴 Kafka 的日志存储模式，支持百万级 TPS
- **高可靠**: 参考 RocketMQ 的事务消息和主从架构
- **易使用**: 简化 RabbitMQ 的路由复杂性，提供直观 API
- **功能丰富**: 集成延迟消息、顺序消息、消息过滤等企业级特性

## 📁 项目结构

```
zephyr-mq/
├── zephyr-common/      # 公共组件和工具类
├── zephyr-protocol/    # 通信协议定义和序列化
├── zephyr-storage/     # 存储引擎实现
├── zephyr-broker/      # 消息代理服务
├── zephyr-nameserver/  # 注册中心服务
├── zephyr-client/      # 客户端SDK
├── zephyr-admin/       # 管理控制台
├── zephyr-examples/    # 使用示例代码
└── TODO.md            # 开发计划和任务清单
```

## ✅ 已完成功能 (阶段一)

### 1. 项目基础架构
- ✅ Maven 多模块项目结构
- ✅ 依赖管理和版本控制
- ✅ 代码规范和编译配置

### 2. 网络通信层 (基于 Netty)
- ✅ 服务端 NettyRemotingServer
- ✅ 编解码器 (NettyEncoder/NettyDecoder)
- ✅ 请求处理器框架
- ✅ 连接管理和异常处理

### 3. 通信协议定义
- ✅ RemotingCommand 协议格式
- ✅ 请求/响应码定义
- ✅ JSON 序列化支持
- ✅ 协议编解码工具

### 4. 消息模型
- ✅ Message 消息实体
- ✅ MessageQueue 队列模型
- ✅ MessageExt 扩展消息
- ✅ SendResult 发送结果

### 5. Producer API
- ✅ ZephyrProducer 接口定义
- ✅ DefaultZephyrProducer 实现
- ✅ 同步/异步/单向发送
- ✅ 消息队列选择策略

### 6. Consumer API
- ✅ ZephyrPushConsumer 接口定义
- ✅ DefaultZephyrPushConsumer 实现
- ✅ MessageListener 消息监听器
- ✅ 订阅和消费管理

### 7. 使用示例
- ✅ ProducerExample 生产者示例
- ✅ ConsumerExample 消费者示例

## 🔧 技术栈

- **Java**: JDK 8+
- **网络**: Netty 4.x
- **序列化**: Jackson JSON
- **日志**: SLF4J + Logback
- **工具**: Guava, Apache Commons
- **构建**: Maven

## 📖 快速开始

### 编译项目
```bash
mvn clean compile
```

### 运行示例

#### 1. 生产者示例
```bash
mvn exec:java -pl zephyr-examples -Dexec.mainClass="com.zephyr.examples.ProducerExample"
```

#### 2. 消费者示例
```bash
mvn exec:java -pl zephyr-examples -Dexec.mainClass="com.zephyr.examples.ConsumerExample"
```

### API 使用示例

#### Producer
```java
// 创建生产者
DefaultZephyrProducer producer = new DefaultZephyrProducer("example_producer_group");
producer.setNameserverAddresses("127.0.0.1:9877");
producer.start();

// 发送消息
Message message = new Message("TestTopic", "TagA", "Hello ZephyrMQ".getBytes());
SendResult result = producer.send(message);

producer.shutdown();
```

#### Consumer
```java
// 创建消费者
DefaultZephyrPushConsumer consumer = new DefaultZephyrPushConsumer("example_consumer_group");
consumer.setNameserverAddresses("127.0.0.1:9877");

// 注册监听器
consumer.registerMessageListener(new MessageListener() {
    @Override
    public ConsumeOrderlyStatus consumeMessage(List<MessageExt> msgs, ConsumeOrderlyContext context) {
        // 处理消息
        return ConsumeOrderlyStatus.SUCCESS;
    }
});

// 订阅主题
consumer.subscribe("TestTopic", "*");
consumer.start();
```

## 🗺️ 开发路线图

### ✅ 阶段一：基础框架搭建 (已完成)
- 项目结构初始化
- 网络通信层
- 基础协议定义
- 简单 Producer/Consumer API

### 🔄 阶段二：核心存储引擎 (进行中)
- 消息存储格式设计
- 日志文件管理
- 读写机制优化
- 内存缓存层

### 📋 阶段三：Topic和分区机制
- Topic 管理
- 分区策略
- 消息路由
- 负载均衡

### 📋 阶段四：高级特性实现
- 消息确认机制
- 事务消息
- 延迟消息
- 消息过滤
- 顺序消息

### 📋 阶段五：集群和高可用
- NameServer 集群
- Broker 主从复制
- 故障转移
- 数据一致性

### 📋 阶段六：监控和管理
- Web 管理控制台
- 监控指标
- 日志和审计

## 🏆 性能目标

- **单机写入**: 100万 TPS
- **单机读取**: 200万 TPS
- **端到端延迟**: < 1ms (P99)
- **可用性**: 99.99%
- **数据丢失**: 0%

## 🤝 贡献指南

详见 `TODO.md` 文件中的开发规范和里程碑计划。

## 📄 许可证

本项目采用 MIT 许可证。

---

*生成时间: 2025-09-21*
*当前版本: 1.0.0-SNAPSHOT*