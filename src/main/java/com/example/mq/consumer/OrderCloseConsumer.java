package com.example.mq.consumer;

import com.example.mq.common.MqMessages;
import com.example.mq.order.OrderCreatedEvent;
import com.example.mq.order.OrderService;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.annotation.RocketMQMessageListener;
import org.apache.rocketmq.client.apis.consumer.ConsumeResult;
import org.apache.rocketmq.client.apis.message.MessageView;
import org.apache.rocketmq.client.core.RocketMQListener;
import org.springframework.stereotype.Component;

/**
 * 延迟消息消费者：超时未支付自动关单。
 * 消息到点只是"触发一次检查"，必须按业务状态幂等判断，不要假设此刻状态一定未变。
 */
@Slf4j
@Component
@RequiredArgsConstructor
@RocketMQMessageListener(
        consumerGroup = "GID_order_close",
        topic = "${app.mq.delay-topic:}",
        tag = "close")
public class OrderCloseConsumer implements RocketMQListener {

    private final ObjectMapper objectMapper;
    private final OrderService orderService;

    @Override
    public ConsumeResult consume(MessageView messageView) {
        try {
            OrderCreatedEvent event = objectMapper.readValue(
                    MqMessages.bodyAsString(messageView), OrderCreatedEvent.class);
            orderService.closeIfUnpaid(event.orderNo());
            return ConsumeResult.SUCCESS;
        } catch (Exception e) {
            log.warn("close order failed, messageId={}", messageView.getMessageId(), e);
            return ConsumeResult.FAILURE;
        }
    }
}
