package com.thesis.sqlite.messages.kafka;

public class KafkaMessage<T> {
    private final String topic;
    private final T message;

    public KafkaMessage(String topic, T message) {
        this.topic = topic;
        this.message = message;
    }

    public String getTopic() {
        return topic;
    }

    public T getMessage() {
        return message;
    }
}
