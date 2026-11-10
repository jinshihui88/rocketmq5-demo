package com.example.mq.order;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.annotation.RocketMQTransactionListener;
import org.apache.rocketmq.client.apis.message.MessageView;
import org.apache.rocketmq.client.apis.producer.TransactionResolution;
import org.apache.rocketmq.client.core.RocketMQTransactionChecker;

/**
 * 事务消息回查：Broker 长时间未收到 commit/rollback 时触发。
 * check() 必须只读、可重复执行：用业务主键查本地表，不要依赖内存状态。
 */
@Slf4j
@RocketMQTransactionListener
@RequiredArgsConstructor
public class OrderTransactionChecker implements RocketMQTransactionChecker {

    private final OrderRepository orderRepository;

    @Override
    public TransactionResolution check(MessageView messageView) {
        String orderNo = messageView.getProperties().get("orderNo");
        log.info("transaction check back, orderNo={}", orderNo);
        if (orderNo == null) {
            return TransactionResolution.ROLLBACK;
        }
        try {
            return orderRepository.existsByOrderNo(orderNo)
                    ? TransactionResolution.COMMIT
                    : TransactionResolution.ROLLBACK;
        } catch (Exception e) {
            log.warn("check local tx failed, orderNo={}", orderNo, e);
            return TransactionResolution.UNKNOWN; // 暂时无法判断，稍后再次回查
        }
    }
}
