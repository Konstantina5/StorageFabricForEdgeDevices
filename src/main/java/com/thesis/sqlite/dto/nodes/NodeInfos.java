package com.thesis.sqlite.dto.nodes;

public class NodeInfos {
    private final String id;
    private final String url;
    private final String tableName;

    public NodeInfos(String id, String url, String tableName) {
        this.id = id;
        this.url = url;
        this.tableName = tableName;
    }

    public String getId() {
        return id;
    }

    public String getUrl() {
        return url;
    }

    public String getTableName() {
        return tableName;
    }
}
