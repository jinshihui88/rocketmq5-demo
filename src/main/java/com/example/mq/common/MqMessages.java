package com.example.mq.common;

import org.apache.rocketmq.client.apis.message.MessageView;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * 消息体解析工具：MessageView.getBody() 返回 ByteBuffer，需自行转字符串再反序列化。
 */
public final class MqMessages {

    private MqMessages() {
    }

    public static String bodyAsString(MessageView view) {
        ByteBuffer buffer = view.getBody();
        byte[] bytes = new byte[buffer.remaining()];
        buffer.get(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }
}
