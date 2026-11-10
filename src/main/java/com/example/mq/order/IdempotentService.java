package com.example.mq.order;

import lombok.RequiredArgsConstructor;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

/**
 * 幂等消费：业务唯一键 + 唯一索引。RocketMQ 保证"至少一次"投递，消费端必须幂等。
 */
@Service
@RequiredArgsConstructor
public class IdempotentService {

    private final JdbcTemplate jdbcTemplate;

    /**
     * @return true 表示首次处理；false 表示重复消息
     */
    public boolean tryAcquire(String consumerGroup, String bizKey) {
        try {
            jdbcTemplate.update(
                    "INSERT INTO mq_consume_log(consumer_group, biz_key) VALUES (?, ?)",
                    consumerGroup, bizKey);
            return true;
        } catch (DuplicateKeyException e) {
            return false;
        }
    }
}
