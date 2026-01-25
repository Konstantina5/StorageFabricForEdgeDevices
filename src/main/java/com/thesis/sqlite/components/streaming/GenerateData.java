package com.thesis.sqlite.components.streaming;

import com.fasterxml.jackson.databind.JsonNode;
import com.thesis.sqlite.messages.kafka.base.KafkaMessage;
import com.thesis.sqlite.services.LocalDataService;
import com.thesis.sqlite.utils.Utils;
import jakarta.inject.Singleton;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Optional;

@Component
@Singleton
public class GenerateData {
    private static final int BATCH_SIZE = 1000;

    private final ApplicationEventPublisher eventPublisher;
    private final LocalDataService localDataService;
    private int offset = 0;

    @Autowired
    public GenerateData(ApplicationEventPublisher eventPublisher,
                        LocalDataService localDataService) {
        this.eventPublisher = eventPublisher;
        this.localDataService = localDataService;
    }

    private void sendMessage(JsonNode message) {
        KafkaMessage<JsonNode> kafkaMessage =
                new KafkaMessage<>(Utils.TABLE_NAME, message);
        eventPublisher.publishEvent(kafkaMessage);
    }

    public void sendBatch(Optional<Integer> amount) {
        int batchSize = amount.orElse(BATCH_SIZE);
        List<JsonNode> batch = localDataService.fetchBatch(Utils.TABLE_NAME, batchSize, offset);

        // restart when table is exhausted
        if (batch.isEmpty()) {
            offset = 0;
            return;
        }

        batch.forEach(this::sendMessage);

        offset += batchSize;

        // safety reset
        if (batch.size() < batchSize) {
            offset = 0;
        }
    }
}
