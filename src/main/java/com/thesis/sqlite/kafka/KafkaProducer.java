package com.thesis.sqlite.kafka;

import com.thesis.sqlite.messages.kafka.base.KafkaMessage;
import com.thesis.sqlite.utils.JsonUtil;
import com.thesis.sqlite.utils.Utils;
import lombok.AllArgsConstructor;
import org.springframework.context.event.EventListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
@AllArgsConstructor
public class KafkaProducer {

    private KafkaTemplate<String, String> kafkaTemplate;
    private final JsonUtil jsonUtil;

    public <T> void sendMessageWithKey(String topic, T message) {
        String request = jsonUtil.toString(message);
        kafkaTemplate.send(topic, Utils.HOSTNAME, request);
        Utils.LOGGER.info("Message sent: {}", request);
    }

    private  <T> void sendMessage(String topic, T message) {
        String request = jsonUtil.toString(message);
        kafkaTemplate.send(topic, request);
        Utils.LOGGER.info("Message sent: {}", request);
    }

    @EventListener
    public void onKafkaMessageSend(KafkaMessage<?> message) {
        sendMessage(message.getTopic(), message.getMessage());
    }

}
