package com.thesis.sqlite.components.streaming;

import com.fasterxml.jackson.databind.JsonNode;
import com.thesis.sqlite.components.query.ExternalInteractor;
import com.thesis.sqlite.messages.kafka.base.KafkaMessage;
import com.thesis.sqlite.services.LocalDataService;
import com.thesis.sqlite.utils.Utils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
@ConditionalOnProperty(
        value="streaming",
        havingValue = "true")
public class ProduceData {
    private static final int BATCH_SIZE = 10;

    private final ApplicationEventPublisher eventPublisher;
    private final LocalDataService localDataService;
    private int offset = 0;

    @Autowired
    public ProduceData(ApplicationEventPublisher eventPublisher,
                       LocalDataService localDataService) {
        this.eventPublisher = eventPublisher;
        this.localDataService = localDataService;
    }

    private void sendMessage(JsonNode message) {
        KafkaMessage<JsonNode> kafkaMessage =
                new KafkaMessage<>(Utils.TABLE_NAME, message);
        eventPublisher.publishEvent(kafkaMessage);
    }

    @Scheduled(fixedRate = 2000)
    public void sendBatch() {
        List<JsonNode> batch = localDataService.fetchBatch(Utils.TABLE_NAME, BATCH_SIZE, offset);

        // restart when table is exhausted
        if (batch.isEmpty()) {
            offset = 0;
            return;
        }

        batch.forEach(this::sendMessage);

        offset += BATCH_SIZE;

        // safety reset
        if (batch.size() < BATCH_SIZE) {
            offset = 0;
        }
    }
}
