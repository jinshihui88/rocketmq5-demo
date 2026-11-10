package com.example.mq.order;

/**
 * 订单状态流转事件（顺序消息示例：同一 orderNo 的消息保持先进先出）
 */
public record OrderStatusEvent(String orderNo, String status, int seq) {
}
