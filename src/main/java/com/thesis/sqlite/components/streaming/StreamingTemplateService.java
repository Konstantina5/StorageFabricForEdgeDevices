package com.thesis.sqlite.components.streaming;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.thesis.sqlite.dto.streaming.Field;
import com.thesis.sqlite.utils.JsonUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.Resource;
import org.springframework.core.io.ResourceLoader;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.UUID;
import java.util.stream.Collectors;

@Component
public class StreamingTemplateService {
    @Value( "${streaming.files-prefix}" )
    private String filesPrefix;
    @Autowired
    private JsonUtil jsonUtil;
    @Autowired
    private ResourceLoader resourceLoader;
    private static final Random RANDOM = new Random();

    private String loadJsonDataTemplate() {
        String format = String.format("classpath:streaming/data/%s_template.json", filesPrefix);
        Resource resource = resourceLoader.getResource(format);
        try {
            return new String(resource.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public String loadKafkaStreamTemplate() {
        String format = String.format("classpath:streaming/initialization/%s.txt", filesPrefix);
        Resource resource = resourceLoader.getResource(format);
        try {
            return new String(resource.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public Map<String, Field> generateFieldMap() {
        String jsonNode = loadJsonDataTemplate();
        Map<String, Field> templateMap = jsonUtil.fromJson(jsonNode, new TypeReference<>() {});
        return templateMap;
    }


    public JsonNode generateFinalJson(Map<String, Field> fieldMap) {
        Map<String, Object> finalResult = fieldMap.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().getCurrentValue()));

        return jsonUtil.toJson(finalResult);
    }

    public Object generateCurrentValue(Field field) {
        return switch (field.getType()) {
            case INTEGER -> Optional.of(field)
                    .filter(f -> f.getGeneration().equals(Field.Generation.INCREASING))
                    .map(f -> {
                        f.setCurrentValue(((Integer) f.getCurrentValue() % field.getMax()) + 1);
                        return f.getCurrentValue();
                    })
                    .orElseGet(() -> RANDOM.nextInt((field.getMax() - field.getMin()) + 1) + field.getMin());
            case FLOAT -> field.getMin() + RANDOM.nextFloat() * (field.getMax() - field.getMin());
            case STRING -> UUID.randomUUID().toString();
        };
    }
}
