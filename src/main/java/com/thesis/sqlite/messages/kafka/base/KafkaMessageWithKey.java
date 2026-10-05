package com.thesis.sqlite.messages.kafka.base;

public class KafkaMessageWithKey<T> {
    private final String key;
    private final String topic;
    private final T message;


    public KafkaMessageWithKey(String key, String topic, T message) {
        this.key = key;
        this.topic = topic;
        this.message = message;
    }

    public String getKey() {
        return key;
    }

    public T getMessage() {
        return message;
    }

    public String getTopic() {
        return topic;
    }
}
