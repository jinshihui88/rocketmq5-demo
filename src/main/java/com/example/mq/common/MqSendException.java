package com.example.mq.common;

public class MqSendException extends RuntimeException {

    public MqSendException(String message) {
        super(message);
    }

    public MqSendException(String message, Throwable cause) {
        super(message, cause);
    }
}
