package com.thesis.sqlite.dto.request.streaming;

public class QueryWithDataAmount extends QueryTable {
    private final Integer dataAmount;

    public QueryWithDataAmount(String query, String tableName, Integer dataAmount) {
        super(query, tableName);
        this.dataAmount = dataAmount;
    }

    public Integer getDataAmount() {
        return dataAmount;
    }
}
