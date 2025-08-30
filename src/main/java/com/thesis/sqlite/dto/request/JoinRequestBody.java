package com.thesis.sqlite.dto.request;

import com.thesis.sqlite.dto.join.JoinDto;

import java.util.List;

public class JoinRequestBody {
    private final String originalQuery;
    private final List<JoinDto> joins;

    public JoinRequestBody(String originalQuery, List<JoinDto> joins) {
        this.originalQuery = originalQuery;
        this.joins = joins;
    }

    public String getOriginalQuery() {
        return originalQuery;
    }

    public List<JoinDto> getJoins() {
        return joins;
    }
}
