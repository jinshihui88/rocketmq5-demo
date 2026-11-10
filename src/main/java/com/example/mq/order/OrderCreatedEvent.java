package com.example.mq.order;

import java.math.BigDecimal;

/**
 * 订单创建事件（统一 JSON 序列化后作为消息体）
 */
public record OrderCreatedEvent(String orderNo, Long userId, BigDecimal amount) {
}
