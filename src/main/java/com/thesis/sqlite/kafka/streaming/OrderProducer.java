package com.thesis.sqlite.kafka.streaming;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.thesis.sqlite.messages.kafka.base.KafkaMessage;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

@Component
public class OrderProducer {
    private static final String ORDER_TOPIC = "orders";
    private static final int MAX_PRODUCT_ID = 1000;
    private final Random random = new Random();
    private final AtomicInteger currentId = new AtomicInteger(0);
    private final ApplicationEventPublisher eventPublisher;

    public OrderProducer(ApplicationEventPublisher eventPublisher) {
        this.eventPublisher = eventPublisher;
    }

    public void sendOrder(Order order) {
        KafkaMessage<Order> kafkaMessage = new KafkaMessage<>(ORDER_TOPIC, order);
        eventPublisher.publishEvent(kafkaMessage);
    }

    @Scheduled(fixedRate = 2000)
    public void sendRandomInventory() {
        int productId = currentId.updateAndGet(id -> (id % MAX_PRODUCT_ID) + 1);
        int quantity = 10 + random.nextInt(50); // random stock between 10–99

        Order order = new Order(productId, quantity);

        sendOrder(order);
    }

    public record Order(@JsonProperty("product_id") Integer productId, int quantity) {
    }
}
