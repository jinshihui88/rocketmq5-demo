# rocketmq5-demo

Spring Boot 3 整合 RocketMQ 5.x 的示例工程，围绕「订单」业务演示 RocketMQ 5.x 新版客户端（gRPC 协议、连接 Proxy）的核心消息能力，配套幂等消费、事务消息、超时关单等生产级实践。

## 技术栈

| 组件 | 版本 | 说明 |
| --- | --- | --- |
| Java | 17 | |
| Spring Boot | 3.5.7 | Web + JDBC，优雅停机 |
| rocketmq-v5-client-spring-boot-starter | 2.3.6 | RocketMQ 5.x 新客户端，gRPC 协议直连 Proxy（不是 4.x 的 Remoting + NameServer 9876） |
| H2 Database | runtime | 内存库，零外部依赖，承载订单表与幂等表 |
| Lombok | optional | |

## 演示内容

| 能力 | Topic | 消费组 | 说明 |
| --- | --- | --- | --- |
| 普通消息（同步/异步） | `OrderNormalTopic` | `GID_order_created` | 订单创建通知；异步回调挂在方法返回的 future 上 |
| 顺序消息 | `OrderFifoTopic` | `GID_order_status` | 同一订单 CREATED → PAID → SHIPPED 三条状态按 messageGroup 保序 |
| 延迟消息 | `OrderDelayTopic` | `GID_order_close` | 超时关单，延迟时长以 `Duration` 传递，默认 10 秒便于演示 |
| 事务消息 | `OrderTransTopic` | `GID_order_trans` | 半消息 + 本地事务（订单落库）+ 提交/回滚 + 事务回查（`OrderTransactionChecker`） |
| SimpleConsumer 主动拉取 | `OrderNormalTopic` | `GID_order_simple` | `SimpleOrderPoller`，配置文件默认关闭，取消注释即启用 |
| 幂等消费 | — | — | `IdempotentService`：`mq_consume_log` 表 + 唯一索引，业务唯一键去重 |

## 工程结构

```
src/main/java/com/example/mq
├── Application.java          # 启动类
├── common/                   # 消息封装、发送异常
├── config/                   # MqConfig（TransactionTemplate）、Topic 名称集中配置
├── producer/MqProducer.java  # 生产者薄封装：JSON 消息体 + SendReceipt 判空 + 统一日志
├── consumer/                 # 5 个消费者（含 1 个 SimpleConsumer 轮询器）
├── order/                    # 订单业务：事务消息、幂等、通知、仓储（JDBC）
└── web/OrderController.java  # HTTP 接口，触发各类消息发送
tools/                        # 教程 Word 稿生成脚本（md → docx）
```

## 快速开始

### 1. 启动 RocketMQ 5.x（需要 Proxy）

本工程连接的是 **Proxy 的 gRPC 端口 8081**，不是 NameServer 的 9876。以本地二进制发行版为例：

```bash
# 先启动 NameServer、Broker，再启动 Proxy
sh bin/mqproxy -n 127.0.0.1:9876 --remotingListenPort 8080
```

并预先创建 Topic（顺序消息 Topic 必须为 FIFO 类型，延迟消息 Topic 必须为 DELAY 类型）：

```bash
sh bin/mqadmin updateTopic -n 127.0.0.1:9876 -c DefaultCluster -t OrderNormalTopic
sh bin/mqadmin updateTopic -n 127.0.0.1:9876 -c DefaultCluster -t OrderFifoTopic -a +message.type=FIFO
sh bin/mqadmin updateTopic -n 127.0.0.1:9876 -c DefaultCluster -t OrderDelayTopic -a +message.type=DELAY
sh bin/mqadmin updateTopic -n 127.0.0.1:9876 -c DefaultCluster -t OrderTransTopic -a +message.type=TRANSACTION
```

### 2. 启动应用

```bash
mvn spring-boot:run
```

应用端口 **8088**（8080 被 Proxy 的 Remoting 端口占用），启动时自动执行 `schema.sql` 初始化 H2 内存库。

### 3. 验证

| 接口 | 说明 |
| --- | --- |
| `POST /order/normal?orderNo=O1` | 同步发送普通消息 |
| `POST /order/async?orderNo=O2` | 异步发送普通消息 |
| `POST /order/fifo?orderNo=O3` | 顺序消息：同一订单三条状态流转 |
| `POST /order/delay?orderNo=O4&delaySeconds=10` | 延迟消息：10 秒后关单 |
| `POST /order/tx?orderNo=O5` | 事务消息：下单落库并提交 |
| `GET /order/{orderNo}` | 查询订单状态（验证落库与超时关单效果） |

## 关键配置（application.yml）

```yaml
rocketmq:
  producer:
    endpoints: 127.0.0.1:8081     # Proxy gRPC 地址，多个以分号分隔
    topic: OrderTransTopic        # 事务消息建议预先声明，用于预取路由
  push-consumer:
    endpoints: 127.0.0.1:8081     # 监听类未单独指定 endpoints 时使用
  # simple-consumer:              # 默认关闭；取消注释后 SimpleOrderPoller 自动启用
```

- Topic 名称通过 `app.mq.*` 集中管理，避免硬编码散落各处。
- 消息体统一为 JSON 字符串（业务层 Jackson 序列化），跨语言友好、序列化行为确定。
- 2.3.x 客户端发送失败可能返回 `null`，`MqProducer` 对 `SendReceipt` 统一判空防 NPE。
