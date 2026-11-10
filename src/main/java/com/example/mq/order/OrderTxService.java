package com.example.mq.order;

import com.example.mq.config.MqTopicProperties;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.apis.producer.SendReceipt;
import org.apache.rocketmq.client.apis.producer.Transaction;
import org.apache.rocketmq.client.common.Pair;
import org.apache.rocketmq.client.core.RocketMQClientTemplate;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Service;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * 事务消息：解决"本地数据库事务"与"消息发送"之间的最终一致性。
 * 流程：发送半消息 -> 执行本地事务 -> commit / rollback；
 * 若 Broker 长时间未收到二次确认，回查 OrderTransactionChecker.check()。
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class OrderTxService {

    private final RocketMQClientTemplate template;
    private final TransactionTemplate transactionTemplate;
    private final OrderRepository orderRepository;
    private final ObjectMapper objectMapper;
    private final MqTopicProperties topics;

    public void createOrder(OrderCreatedEvent event) throws Exception {
        Message<String> msg = MessageBuilder
                .withPayload(objectMapper.writeValueAsString(event))
                .setHeader("orderNo", event.orderNo())
                .build();

        // 1. 发送半消息（此时消费者不可见）
        String dest = topics.transTopic() + ":created";
        Pair<SendReceipt, Transaction> pair = template.sendTransactionMessage(dest, msg);
        Transaction tx = pair.getTransaction();
        log.info("half message sent, dest={}, messageId={}", dest, pair.getSendReceipt().getMessageId());

        // 2. 执行本地事务
        try {
            transactionTemplate.executeWithoutResult(status -> orderRepository.save(event));
        } catch (Exception e) {
            tx.rollback(); // 3a. 本地事务失败：回滚半消息
            log.error("local tx failed, half message rolled back. orderNo={}", event.orderNo(), e);
            throw e;
        }
        // 3b. 本地事务成功：提交半消息（若此处异常，Broker 回查时会发现订单已存在并 COMMIT）
        tx.commit();
        log.info("half message committed. orderNo={}", event.orderNo());
    }
}
