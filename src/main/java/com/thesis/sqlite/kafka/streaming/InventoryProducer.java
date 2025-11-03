package com.thesis.sqlite.kafka.streaming;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.thesis.sqlite.messages.kafka.base.KafkaMessage;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

@Component
public class InventoryProducer {
    private static final String INVENTORY_TOPIC = "inventory";
    private static final int MAX_PRODUCT_ID = 1000;
    private final Random random = new Random();
    private final AtomicInteger currentId = new AtomicInteger(0);

    private final ApplicationEventPublisher eventPublisher;

    public InventoryProducer(ApplicationEventPublisher eventPublisher) {
        this.eventPublisher = eventPublisher;
    }

    public void sendInventory(Inventory inventory) {
        KafkaMessage<Inventory> kafkaMessage = new KafkaMessage<>(INVENTORY_TOPIC, inventory);
        eventPublisher.publishEvent(kafkaMessage);
    }

    @Scheduled(fixedRate = 1000)
    public void sendRandomInventory() {
        int productId = currentId.updateAndGet(id -> (id % MAX_PRODUCT_ID) + 1);
        int stockLevel = 10 + random.nextInt(90); // random stock between 10–99

        Inventory inventory = new Inventory(productId, stockLevel);

        sendInventory(inventory);
    }

    public record Inventory(@JsonProperty("product_id") Integer productId,
                            @JsonProperty("stock_level") int stockLevel) {
    }
}
