package com.example.mq.producer;

import com.example.mq.common.MqSendException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.apis.producer.SendReceipt;
import org.apache.rocketmq.client.core.RocketMQClientTemplate;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * 生产者薄封装：统一做三件事 ——
 * 1. 以 JSON 字符串作为消息体（序列化行为确定、跨语言友好）；
 * 2. 对 SendReceipt 判空（2.3.x 发送失败可能返回 null）；
 * 3. 统一日志与异常。
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class MqProducer {

    private final RocketMQClientTemplate template;
    private final ObjectMapper objectMapper;

    /** 普通消息。destination 格式：topic 或 topic:tag */
    public SendReceipt sendNormal(String destination, Object event) {
        SendReceipt receipt = template.syncSendNormalMessage(destination, json(event));
        return verify(destination, receipt);
    }

    /** 带业务属性的普通消息；属性在消费端通过 getProperties() 读取 */
    public SendReceipt sendNormal(String destination, Object event, Map<String, Object> props) {
        // 注意：这里的 Message 是 org.springframework.messaging.Message
        Message<String> msg = MessageBuilder.withPayload(json(event))
                .copyHeaders(props)
                .build();
        SendReceipt receipt = template.syncSendNormalMessage(destination, msg);
        return verify(destination, receipt);
    }

    /** 顺序消息：messageGroup 相同的消息保持先进先出（Topic 必须为 FIFO 类型） */
    public SendReceipt sendFifo(String destination, Object event, String messageGroup) {
        SendReceipt receipt = template.syncSendFifoMessage(destination, json(event), messageGroup);
        return verify(destination, receipt);
    }

    /** 延迟消息（Topic 必须为 DELAY 类型），延迟时间用 Duration 表示 */
    public SendReceipt sendDelay(String destination, Object event, Duration delay) {
        SendReceipt receipt = template.syncSendDelayMessage(destination, json(event), delay);
        return verify(destination, receipt);
    }

    /** 异步发送：回调挂在方法返回的 future 上（挂在入参 future 上可能不会触发） */
    public CompletableFuture<SendReceipt> sendNormalAsync(String destination, Object event) {
        CompletableFuture<SendReceipt> future = template.asyncSendNormalMessage(
                destination, json(event), new CompletableFuture<>());
        return future.whenComplete((receipt, ex) -> {
            if (ex != null) {
                log.error("MQ async send failed, dest={}", destination, ex);
            } else {
                log.info("MQ async sent, dest={}, messageId={}", destination, receipt.getMessageId());
            }
        });
    }

    private SendReceipt verify(String destination, SendReceipt receipt) {
        if (receipt == null) {
            throw new MqSendException("MQ send failed, no receipt: " + destination);
        }
        log.info("MQ sent, dest={}, messageId={}", destination, receipt.getMessageId());
        return receipt;
    }

    private String json(Object event) {
        if (event instanceof String s) {
            return s;
        }
        try {
            return objectMapper.writeValueAsString(event);
        } catch (JsonProcessingException e) {
            throw new MqSendException("MQ payload serialize failed", e);
        }
    }
}
