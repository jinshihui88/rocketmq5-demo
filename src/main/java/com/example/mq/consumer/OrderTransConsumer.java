package com.example.mq.consumer;

import com.example.mq.common.MqMessages;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.annotation.RocketMQMessageListener;
import org.apache.rocketmq.client.apis.consumer.ConsumeResult;
import org.apache.rocketmq.client.apis.message.MessageView;
import org.apache.rocketmq.client.core.RocketMQListener;
import org.springframework.stereotype.Component;

/**
 * 事务消息消费者：模拟库存服务按订单号幂等扣减库存。
 * 只有半消息被 commit 后，这里才会收到消息。
 */
@Slf4j
@Component
@RocketMQMessageListener(
        consumerGroup = "GID_order_trans",
        topic = "${app.mq.trans-topic:}",
        tag = "created")
public class OrderTransConsumer implements RocketMQListener {

    @Override
    public ConsumeResult consume(MessageView messageView) {
        String body = MqMessages.bodyAsString(messageView);
        String orderNo = messageView.getProperties().get("orderNo");
        log.info("deduct stock for order (tx message committed), orderNo={}, body={}", orderNo, body);
        return ConsumeResult.SUCCESS;
    }
}
