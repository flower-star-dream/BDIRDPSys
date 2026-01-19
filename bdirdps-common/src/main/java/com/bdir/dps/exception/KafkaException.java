package com.bdir.dps.exception;

/**
 * Kafka操作异常类
 * 用于处理Kafka消息发送、消费等相关异常
 */
public class KafkaException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    /**
     * 错误码
     */
    private String errorCode;

    /**
     * Kafka主题
     */
    private String topic;

    public KafkaException(String message) {
        super(message);
    }

    public KafkaException(String message, Throwable cause) {
        super(message, cause);
    }

    public KafkaException(String errorCode, String message) {
        super(message);
        this.errorCode = errorCode;
    }

    public KafkaException(String errorCode, String topic, String message) {
        super(message);
        this.errorCode = errorCode;
        this.topic = topic;
    }

    public KafkaException(String errorCode, String topic, String message, Throwable cause) {
        super(message, cause);
        this.errorCode = errorCode;
        this.topic = topic;
    }

    public String getErrorCode() {
        return errorCode;
    }

    public void setErrorCode(String errorCode) {
        this.errorCode = errorCode;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }
}