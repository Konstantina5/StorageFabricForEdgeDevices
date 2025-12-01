package com.thesis.sqlite.kafka.streaming;

import com.fasterxml.jackson.databind.JsonNode;
import com.thesis.sqlite.components.streaming.StreamingTemplateService;
import com.thesis.sqlite.dto.streaming.Field;
import com.thesis.sqlite.messages.kafka.base.KafkaMessage;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

@Component
public class DataProducer {
    @Value("${streaming.topic}")
    private String TOPIC_NAME;
    private final Random random = new Random();

    private final AtomicInteger currentId = new AtomicInteger(0);
    private final ApplicationEventPublisher eventPublisher;
    private final StreamingTemplateService streamingTemplateService;
    private Map<String, Field> fieldMap;

    @Autowired
    public DataProducer(ApplicationEventPublisher eventPublisher, StreamingTemplateService streamingTemplateService) {
        this.eventPublisher = eventPublisher;
        this.streamingTemplateService = streamingTemplateService;
        fieldMap = streamingTemplateService.generateFieldMap();
    }

    public void sendMessage(JsonNode message) {
        KafkaMessage<JsonNode> kafkaMessage = new KafkaMessage<>(TOPIC_NAME, message);
        eventPublisher.publishEvent(kafkaMessage);
    }

    @Scheduled(fixedRate = 2000)
    public void sendRandomDataInterval() {
        fieldMap.forEach((key, value) -> streamingTemplateService.generateCurrentValue(value));
        JsonNode jsonNode = streamingTemplateService.generateFinalJson(fieldMap);
        sendMessage(jsonNode);
    }
}
