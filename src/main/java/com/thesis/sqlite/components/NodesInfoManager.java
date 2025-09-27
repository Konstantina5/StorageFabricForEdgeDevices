package com.thesis.sqlite.components;

import com.thesis.sqlite.dto.nodes.InfoType;
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
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import static com.thesis.sqlite.kafka.KafkaTopics.NODE_INFO;

@Component
public class NodesInfoManager {
    private static final String MY_TABLE = System.getenv("DB_NAME");
    private final Map<String, NodeInfos> nodeInfos = new ConcurrentHashMap<>();
    public final Map<String, NodeInfos> tableInfos = new ConcurrentHashMap<>();

    private final ApplicationEventPublisher eventPublisher;

    @Autowired
    public NodesInfoManager(ApplicationEventPublisher eventPublisher) {

        this.eventPublisher = eventPublisher;

//        tableInfos.putIfAbsent("author", new NodeInfos("client", "http://localhost:8080/api", InfoType.AUTHOR,
//                new String[]{"author"}, "author"));
//        tableInfos.putIfAbsent("address", new NodeInfos("client", "http://localhost:8080/api", InfoType.ADDRESS,
//                new String[]{"address"}, "address"));
//        tableInfos.putIfAbsent("book", new NodeInfos("client", "http://localhost:8080/api", InfoType.BOOK,
//                new String[]{"book"}, "book"));
    }

    @EventListener(ApplicationReadyEvent.class)
    public void initializeNode() {
        KafkaMessage<NodeAdded> kafkaMessage = new KafkaMessage<>(NODE_INFO, new NodeAdded(Utils.HOSTNAME,
                new NodeInfos("client", System.getenv("BASE_URL"), InfoType.AUTHOR, new String[]{"author"}, MY_TABLE)));
        //Add id as conf maybe
        eventPublisher.publishEvent(kafkaMessage);
    }



    @EventListener
    public void onNodeAdded(NodeAdded nodeAdded) {
        Optional.ofNullable(MY_TABLE)
//                .filter(name -> !name.equals(nodeAdded.getNodeInfo().getTableName()))
                .ifPresent(__ -> {
                    nodeInfos.put(nodeAdded.getNodeName(), nodeAdded.getNodeInfo());
                    tableInfos.put(nodeAdded.getNodeInfo().getTableName(), nodeAdded.getNodeInfo());
                });

        System.out.println();
    }

    public Map<String, NodeInfos> getTableInfos() {
        return tableInfos;
    }
}
