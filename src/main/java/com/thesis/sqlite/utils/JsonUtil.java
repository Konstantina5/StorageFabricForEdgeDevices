package com.thesis.sqlite.utils;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.IOException;

@Component
public class JsonUtil {
    private final ObjectMapper objectMapper;

    @Autowired
    public JsonUtil(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    public <A> A fromJson(String str, TypeReference<A> typeReference) {
        try {
            return objectMapper.readValue(str, typeReference);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public JsonNode toJson(final Object data) {
        try {
            return objectMapper.valueToTree(data);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public String toString(final Object obj) {
        try {
            return objectMapper.writeValueAsString(obj);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public JsonNode parse(final String json) {
        try {
            return objectMapper.readTree(json);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public <A> A parse(final String str, Class<A> clazz) {
        try {
            return objectMapper.readValue(str, clazz);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public <A> A fromJson(byte[] bytes, Class<A> clazz) {
        try {
            return objectMapper.treeToValue(objectMapper.readTree(bytes), clazz);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public <A> A fromJson(JsonNode jsonNode, Class<A> clazz) {
        try {
            return objectMapper.treeToValue(jsonNode, clazz);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
