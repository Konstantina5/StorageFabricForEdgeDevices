package com.thesis.sqlite.kafka;

import com.fasterxml.jackson.core.type.TypeReference;
import com.thesis.sqlite.dht.BaseRequest;
import com.thesis.sqlite.dht.DhtService;
import com.thesis.sqlite.messages.kafka.NodeAdded;
import com.thesis.sqlite.messages.kafka.NodePing;
import com.thesis.sqlite.utils.JsonUtil;
import com.thesis.sqlite.utils.Utils;
import lombok.AllArgsConstructor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.util.Set;

import static com.thesis.sqlite.kafka.KafkaTopics.NODE_INFO;
import static com.thesis.sqlite.kafka.KafkaTopics.NODE_PING;

@Service
@AllArgsConstructor
public class KafkaConsumer {
    private final DhtService dhtService;
    private final JsonUtil jsonUtil;
    private final ApplicationEventPublisher eventPublisher;

    @KafkaListener(topics = "init-topic", groupId = "group_id")
    public void consumeInitNodeMessage(ConsumerRecord<String, String> message) {
        final var key = message.key();

        // Check if the producer ID matches the ID of the producer service
        if (key != null && !Utils.HOSTNAME.equals(key)) {
            // Process the message
            final var newNodeRequest = message.value();
            Utils.LOGGER.info("Message received: {}", newNodeRequest);
            BaseRequest object = jsonUtil.parse(newNodeRequest, BaseRequest.class);
            dhtService.addNode(object);
        } else {
            Utils.LOGGER.warn("Skipping message produced by the producer service");
        }
    }

    @KafkaListener(topics = NODE_INFO, groupId = "node_info_group_#{T(com.thesis.sqlite.utils.Utils).TABLE_NAME}")
    public void consumeNodeInfoMessage(ConsumerRecord<String, String> message) {
        NodeAdded nodeAdded = jsonUtil.parse(message.value(), NodeAdded.class);
        Utils.LOGGER.info("Message received: {}", nodeAdded);
        eventPublisher.publishEvent(nodeAdded);
    }

    @KafkaListener(topics = NODE_PING, groupId = "node_ping_group_#{T(com.thesis.sqlite.utils.Utils).TABLE_NAME}")
    public void consumeNodePingMessage(ConsumerRecord<String, String> message) {
        NodePing nodeAdded = jsonUtil.parse(message.value(), NodePing.class);
        Utils.LOGGER.info("Message received: {}", nodeAdded);
        eventPublisher.publishEvent(nodeAdded);
    }

    @KafkaListener(id = "downNodes", topics = "downnodes-topic", groupId = "${spring.kafka.consumer.group-id}")
    public void consumeDownNodesMessage(String message) {
        Utils.LOGGER.info("Message received: {}", message);
        Set<String> object = jsonUtil.fromJson(message, new TypeReference<Set<String>>() {});
        dhtService.downNodesAction(object);
    }
}