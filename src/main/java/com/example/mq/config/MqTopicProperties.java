package com.example.mq.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * 业务自定义配置：集中管理 Topic 名称，前缀 app.mq
 */
@ConfigurationProperties(prefix = "app.mq")
public record MqTopicProperties(
        String normalTopic,
        String fifoTopic,
        String delayTopic,
        String transTopic) {
}
