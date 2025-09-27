package com.thesis.sqlite.dto.nodes;

public class NodeInfos {
    private final String id;
    private final String url;
    private final InfoType infoType;
    private final String[] types;
    private final String tableName;

    public NodeInfos(String id, String url, InfoType infoType, String[] types, String tableName) {
        this.id = id;
        this.url = url;
        this.infoType = infoType;
        this.types = types;
        this.tableName = tableName;
    }

    public String getId() {
        return id;
    }

    public String getUrl() {
        return url;
    }

    public InfoType getInfoType() {
        return infoType;
    }

    public String[] getTypes() {
        return types;
    }

    public String getTableName() {
        return tableName;
    }
}
