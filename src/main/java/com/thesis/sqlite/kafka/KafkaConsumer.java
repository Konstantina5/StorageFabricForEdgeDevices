package com.thesis.sqlite.kafka;

import java.util.Set;

import com.thesis.sqlite.messages.kafka.NodeAdded;
import com.thesis.sqlite.utils.JsonUtil;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.thesis.sqlite.dht.BaseRequest;
import com.thesis.sqlite.dht.DhtService;
import com.thesis.sqlite.utils.Utils;

import lombok.AllArgsConstructor;

import static com.thesis.sqlite.kafka.KafkaTopics.NODE_INFO;

@Service
@AllArgsConstructor
public class KafkaConsumer {

    private final DhtService dhtService;
    private final ObjectMapper mapper;
    private final JsonUtil jsonUtil;
    private final ApplicationEventPublisher eventPublisher;

    @KafkaListener(topics = "init-topic", groupId = "group_id")
    public void consumeInitNodeMessage(ConsumerRecord<String, String> message) {
        final var key = message.key();

        // Check if the producer ID matches the ID of the producer service
        if (key != null && !Utils.HOSTNAME.equals(key)) {
            try {
                // Process the message
                final var newNodeRequest = message.value();
                Utils.LOGGER.info("Message received: {}", newNodeRequest);
                final var object = mapper.readValue(newNodeRequest, BaseRequest.class);
                dhtService.addNode(object);
            } catch (JsonProcessingException e) {
                Utils.LOGGER.error(e.getMessage(), e);
            }
        } else {
            Utils.LOGGER.warn("Skipping message produced by the producer service");
        }
    }

    @KafkaListener(topics = NODE_INFO, groupId = "group_id")
    public void consumeNodeInfoMessage(ConsumerRecord<String, String> message) {
        NodeAdded nodeAdded = jsonUtil.parse(message.value(), NodeAdded.class);
        Utils.LOGGER.info("Message received: {}", nodeAdded);
        eventPublisher.publishEvent(nodeAdded);
    }

    @KafkaListener(id = "downNodes", topics = "downnodes-topic", groupId = "${spring.kafka.consumer.group-id}")
    public void consumeDownNodesMessage(String message) {
        try {
            Utils.LOGGER.info("Message received: {}", message);
            final var object = mapper.readValue(message, new TypeReference<Set<String>>() {
            });
            dhtService.downNodesAction(object);
        } catch (JsonProcessingException e) {
            Utils.LOGGER.error(e.getMessage(), e);
        }
    }
}