package com.example.mq.consumer;

import com.example.mq.common.MqMessages;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.apis.ClientException;
import org.apache.rocketmq.client.apis.message.MessageView;
import org.apache.rocketmq.client.core.RocketMQClientTemplate;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.List;

/**
 * SimpleConsumer：应用主动拉取与确认，适合自定义拉取节奏、批处理的场景。
 * 仅在配置 rocketmq.simple-consumer.endpoints 后启用（默认关闭，避免与 Push 消费者抢消息）。
 */
@Slf4j
@Component
@RequiredArgsConstructor
@ConditionalOnProperty("rocketmq.simple-consumer.endpoints")
public class SimpleOrderPoller {

    private final RocketMQClientTemplate template;

    @Scheduled(fixedDelay = 100)
    public void poll() throws ClientException {
        // 一次最多拉 16 条；invisibleDuration 必须大于单条处理耗时
        List<MessageView> messages = template.receive(16, Duration.ofSeconds(30));
        for (MessageView message : messages) {
            try {
                handle(message);
                template.ack(message); // 处理成功后确认
            } catch (Exception e) {
                // 不 ack：invisibleDuration 到期后消息会被重新投递
                log.error("handle failed, messageId={}", message.getMessageId(), e);
            }
        }
    }

    private void handle(MessageView message) {
        log.info("simple recv: {}", MqMessages.bodyAsString(message));
    }
}
