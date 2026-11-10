-- 订单表（事务消息本地事务 + 超时关单状态判断）
CREATE TABLE IF NOT EXISTS order_info (
    order_no   VARCHAR(64) PRIMARY KEY,
    user_id    BIGINT        NOT NULL,
    amount     DECIMAL(18, 2) NOT NULL,
    status     VARCHAR(16)   NOT NULL,
    created_at TIMESTAMP     NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- 幂等消费记录表：业务唯一键 + 唯一索引
CREATE TABLE IF NOT EXISTS mq_consume_log (
    id            BIGINT       PRIMARY KEY AUTO_INCREMENT,
    consumer_group VARCHAR(128) NOT NULL,
    biz_key       VARCHAR(128) NOT NULL,
    created_at    TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uk_group_biz UNIQUE (consumer_group, biz_key)
);
