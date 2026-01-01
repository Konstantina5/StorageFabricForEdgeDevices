package com.thesis.sqlite.components.streaming;

import com.thesis.sqlite.algorithm.streaming.KafkaStreaming;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

@Component
@ConditionalOnProperty(
        value="streaming",
        havingValue = "true")
public class KafkaStreamingInitializer implements CommandLineRunner {

    private final KafkaStreaming kafkaStreaming;

    public KafkaStreamingInitializer(KafkaStreaming kafkaStreaming) {
        this.kafkaStreaming = kafkaStreaming;
    }

    @Override
    public void run(String... args) throws Exception {
        kafkaStreaming.initialize();
    }
}
