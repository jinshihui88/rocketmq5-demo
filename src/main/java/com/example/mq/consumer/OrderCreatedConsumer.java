package com.example.mq.consumer;

import com.example.mq.common.MqMessages;
import com.example.mq.order.OrderCreatedEvent;
import com.example.mq.order.OrderNotifyService;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.annotation.RocketMQMessageListener;
import org.apache.rocketmq.client.apis.consumer.ConsumeResult;
import org.apache.rocketmq.client.apis.message.MessageView;
import org.apache.rocketmq.client.core.RocketMQListener;
import org.springframework.stereotype.Component;

/**
 * 普通消息消费者：订单创建后的异步通知。
 */
@Slf4j
@Component
@RequiredArgsConstructor
@RocketMQMessageListener(
        consumerGroup = "GID_order_created",
        topic = "${app.mq.normal-topic:}",
        tag = "created")
public class OrderCreatedConsumer implements RocketMQListener {

    private final ObjectMapper objectMapper;
    private final OrderNotifyService notifyService;

    @Override
    public ConsumeResult consume(MessageView messageView) {
        String body = MqMessages.bodyAsString(messageView);
        try {
            OrderCreatedEvent event = objectMapper.readValue(body, OrderCreatedEvent.class);
            notifyService.handle(event); // 内部做幂等
            return ConsumeResult.SUCCESS;
        } catch (JsonProcessingException e) {
            // 格式错误，重试没有意义：记录后确认，避免毒消息反复重投
            log.error("bad payload, messageId={}", messageView.getMessageId(), e);
            return ConsumeResult.SUCCESS;
        } catch (Exception e) {
            log.warn("consume failed, messageId={}, attempt={}",
                    messageView.getMessageId(), messageView.getDeliveryAttempt(), e);
            return ConsumeResult.FAILURE; // 触发重试
        }
    }
}
