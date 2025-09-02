package com.thesis.sqlite.dto.request;

import com.fasterxml.jackson.databind.JsonNode;

public class GetAllResult {
    private final JsonNode result;

    public GetAllResult(JsonNode result) {
        this.result = result;
    }

    public JsonNode getResult() {
        return result;
    }
}
