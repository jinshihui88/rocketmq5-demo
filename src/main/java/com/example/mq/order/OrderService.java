package com.example.mq.order;

import com.example.mq.config.MqTopicProperties;
import com.example.mq.producer.MqProducer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.time.Duration;

/**
 * 业务侧调用示例：普通 / 顺序 / 延迟消息的发送入口。
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class OrderService {

    private final MqProducer mqProducer;
    private final MqTopicProperties topics;
    private final OrderRepository orderRepository;

    /** 订单创建通知（普通消息，Tag=created） */
    public void notifyCreated(OrderCreatedEvent event) {
        mqProducer.sendNormal(topics.normalTopic() + ":created", event);
    }

    /** 异步发送订单创建通知 */
    public void notifyCreatedAsync(OrderCreatedEvent event) {
        mqProducer.sendNormalAsync(topics.normalTopic() + ":created", event);
    }

    /** 订单状态流转（顺序消息，同一订单号保证有序） */
    public void publishStatus(String orderNo) {
        String[] statuses = {"CREATED", "PAID", "SHIPPED"};
        for (int i = 0; i < statuses.length; i++) {
            OrderStatusEvent event = new OrderStatusEvent(orderNo, statuses[i], i + 1);
            // messageGroup 选择需要保序的业务实体 ID（订单号）
            mqProducer.sendFifo(topics.fifoTopic() + ":status", event, orderNo);
        }
    }

    /** 未支付自动关单（延迟消息；演示时可用秒级延迟，生产一般 30 分钟） */
    public void scheduleClose(OrderCreatedEvent event, Duration delay) {
        mqProducer.sendDelay(topics.delayTopic() + ":close", event, delay);
    }

    /**
     * 超时关单：仅当订单仍为 CREATED 时关闭；已支付 / 已关闭 / 不存在则幂等跳过。
     */
    public void closeIfUnpaid(String orderNo) {
        boolean closed = orderRepository.closeIfUnpaid(orderNo);
        if (closed) {
            log.info("order closed by timeout. orderNo={}", orderNo);
        } else {
            log.info("order not in CREATED status (paid/closed/not-exist), skip. orderNo={}", orderNo);
        }
    }
}
