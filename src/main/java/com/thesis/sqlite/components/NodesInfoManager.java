package com.thesis.sqlite.components;

import com.thesis.sqlite.dto.nodes.NodeInfos;
import com.thesis.sqlite.messages.kafka.KafkaMessage;
import com.thesis.sqlite.messages.kafka.NodeAdded;
import com.thesis.sqlite.utils.Utils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static com.thesis.sqlite.kafka.KafkaTopics.NODE_INFO;

@Component
public class NodesInfoManager {
    private final Map<String, NodeInfos> nodeInfos = new ConcurrentHashMap<>();
    private final ApplicationEventPublisher eventPublisher;

    @Autowired
    public NodesInfoManager(ApplicationEventPublisher eventPublisher) {
        this.eventPublisher = eventPublisher;
    }

    @EventListener(ApplicationReadyEvent.class)
    public void initializeNode() {
        KafkaMessage<NodeAdded> kafkaMessage = new KafkaMessage<>(NODE_INFO, new NodeAdded(Utils.HOSTNAME,
                new NodeInfos("rer")));
        eventPublisher.publishEvent(kafkaMessage);
    }

    @EventListener
    public void onNodeAdded(NodeAdded nodeAdded) {
        nodeInfos.put(nodeAdded.getNodeName(), nodeAdded.getNodeInfo());
    }

}
