package com.example.mq.order;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * 订单创建通知业务处理（演示幂等消费）。
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class OrderNotifyService {

    private final IdempotentService idempotent;

    @Transactional
    public void handle(OrderCreatedEvent event) {
        if (!idempotent.tryAcquire("GID_order_created", event.orderNo())) {
            log.info("duplicate message, skip. orderNo={}", event.orderNo());
            return; // 重复消息，直接确认
        }
        // 业务处理（与幂等记录同一事务，失败一起回滚）：此处模拟发送通知
        log.info("notify downstream for order created: orderNo={}, userId={}, amount={}",
                event.orderNo(), event.userId(), event.amount());
    }
}
