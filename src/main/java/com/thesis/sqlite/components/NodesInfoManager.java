package com.thesis.sqlite.components;

import com.thesis.sqlite.dto.nodes.NodeInfos;
import com.thesis.sqlite.messages.kafka.NodeAdded;
import com.thesis.sqlite.messages.kafka.NodePing;
import com.thesis.sqlite.messages.kafka.base.KafkaMessage;
import com.thesis.sqlite.utils.Utils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import static com.thesis.sqlite.kafka.KafkaTopics.NODE_INFO;
import static com.thesis.sqlite.kafka.KafkaTopics.NODE_PING;

@Component
public class NodesInfoManager {
    private final Map<String, Instant> nodeInfos = new ConcurrentHashMap<>();
    public final Map<String, NodeInfos> tableInfos = new ConcurrentHashMap<>();
    private final NodeInfos currentNodeInfo = new NodeInfos(Utils.TABLE_NAME, Utils.BASE_URL, Utils.TABLE_NAME) ;

    private final ApplicationEventPublisher eventPublisher;

    @Autowired
    public NodesInfoManager(ApplicationEventPublisher eventPublisher) {
        this.eventPublisher = eventPublisher;
    }

    @EventListener(ApplicationReadyEvent.class)
    public void initializeNode() {
        KafkaMessage<NodeAdded> kafkaMessage = new KafkaMessage<>(NODE_INFO, new NodeAdded(Utils.HOSTNAME, currentNodeInfo));//Add id as conf maybe
        eventPublisher.publishEvent(kafkaMessage);
    }

    @EventListener
    public void onNodeAdded(NodeAdded nodeAdded) {
        Optional.ofNullable(Utils.TABLE_NAME)
//                .filter(name -> !name.equals(nodeAdded.getNodeInfo().getTableName()))  TODO k: uncomment when QueryHandler is ready
                .ifPresent(__ -> {
                    nodeInfos.put(nodeAdded.getNodeInfo().getTableName(), Instant.now());
                    tableInfos.put(nodeAdded.getNodeInfo().getTableName(), nodeAdded.getNodeInfo());
                });

        System.out.println();
    }

    @EventListener
    public void onNodePing(NodePing nodePing) {
        Optional.ofNullable(Utils.TABLE_NAME)
//                .filter(name -> !name.equals(nodePing.getNodeInfo().getTableName())) TODO k: uncomment when QueryHandler is ready
                .ifPresent(__ -> {
                    nodeInfos.put(nodePing.getNodeInfo().getTableName(), Instant.now());
                    tableInfos.put(nodePing.getNodeInfo().getTableName(), nodePing.getNodeInfo());
                });

        System.out.println();
    }

    @Scheduled(fixedDelay = 60000, initialDelay = 60000) //60000ms = 1min
    public void sendNodePing() {
        //Send a ping message to indicate that the node is still active
        KafkaMessage<NodePing> kafkaMessage =
                new KafkaMessage<>(NODE_PING, new NodePing(Utils.HOSTNAME, currentNodeInfo));//Add id as conf maybe
        eventPublisher.publishEvent(kafkaMessage);
    }

    //Every one min remove inactive nodes
    @Scheduled(fixedDelay = 60100 , initialDelay = 60100) //60000ms = 1min
    public void removeInactiveNodes() {
        Instant before = Instant.now().minus(2, ChronoUnit.MINUTES);
        List<String> list = nodeInfos.entrySet().stream()
                .filter(entry -> entry.getValue().isBefore(before))
                .map(Map.Entry::getKey)
                .toList();

        tableInfos.entrySet()
                .removeIf(entry -> list.contains(entry.getKey()));
        nodeInfos.entrySet()
                .removeIf(entry -> list.contains(entry.getKey()));
    }

    public Map<String, NodeInfos> getTableInfos() {
        return tableInfos;
    }
}
