package com.thesis.sqlite.dto.request.streaming;

public class QueryTable {
    private final String query;
    private final String tableName;

    public QueryTable(String query, String tableName) {
        this.query = query;
        this.tableName = tableName;
    }

    public String getQuery() {
        return query;
    }

    public String getTableName() {
        return tableName;
    }
}
