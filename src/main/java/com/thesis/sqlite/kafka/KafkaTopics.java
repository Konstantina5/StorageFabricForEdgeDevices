package com.thesis.sqlite.kafka;

public interface KafkaTopics {
    String INIT_TOPIC = "init-topic";
    String DOWNNODES_TOPIC = "downnodes-topic";
    String NODE_INFO = "new_node_info_topic";
    String NODE_PING = "ping_node_topic";
}
