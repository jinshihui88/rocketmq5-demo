package com.example.mq.consumer;

import com.example.mq.common.MqMessages;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.annotation.RocketMQMessageListener;
import org.apache.rocketmq.client.apis.consumer.ConsumeResult;
import org.apache.rocketmq.client.apis.message.MessageView;
import org.apache.rocketmq.client.core.RocketMQListener;
import org.springframework.stereotype.Component;

/**
 * 顺序消息消费者：写法与普通消费者一致，区别在于订阅 FIFO Topic。
 * 同一 messageGroup 的消息会按顺序、串行到达 —— 这里不要再丢线程池并发处理。
 */
@Slf4j
@Component
@RocketMQMessageListener(
        consumerGroup = "GID_order_status",
        topic = "${app.mq.fifo-topic:}",
        tag = "*")
public class OrderStatusConsumer implements RocketMQListener {

    @Override
    public ConsumeResult consume(MessageView messageView) {
        String body = MqMessages.bodyAsString(messageView);
        log.info("order status msg (fifo): {}", body);
        return ConsumeResult.SUCCESS;
    }
}
